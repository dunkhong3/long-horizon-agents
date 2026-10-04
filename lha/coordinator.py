"""The coordinator, which is plain code and not an LLM, and which owns the plan.

It is easiest to think of it as a project manager with a to-do board
(`tasks`) and a notebook of findings (`facts`), and every loop it does four
things.

  1. reclaims tasks whose worker went silent (expired leases)
  2. processes submitted results, one transaction each:
       - error or invalid output  -> retry (same row, attempt + 1) or fail
       - valid                    -> check every fact against its source,
                                     write facts, create follow-up tasks
  3. checks the goal (or the budget): if met or out -> create the report task
  4. stops when the report is accepted (a partial one if the budget ran out)

Workers propose and the coordinator decides. It is the only writer of facts
and of the plan, so there is a single source of truth.
"""

import asyncio
import json
import re
import time
from collections import Counter
from datetime import timedelta
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ValidationError
from sqlalchemy import func, literal, select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from lha.config import (
    BACKOFF_BASE_SECONDS,
    MAX_ROUNDS,
    MAX_VERIFY_ROUNDS,
    NEW_ROUND_DELAY_SECONDS,
    POLL_SECONDS,
)
from lha.db import crud
from lha.db.models import events, facts, sessions, tasks
from lha.schemas import facts as F
from lha.schemas.tasks import (
    INPUT_MODELS,
    OUTPUT_MODELS,
    CompareInput,
    CompareOutput,
    DiscoverInput,
    DiscoverOutput,
    ReportInput,
    ReportOutput,
    VerifyInput,
    VerifyOutput,
)
from lha.world.model import REGISTRY_DOC

ACTOR = "coordinator"
ACTIVE = ("ready", "leased", "submitted")
PERMANENT_ERRORS = ("not_found", "context_overflow")
PROGRESS_EVERY_SECONDS = 2.0


class Rejected(Exception):
    """A result failed the coordinator's checks (for example a fact doesn't match its source)."""


class Coordinator:
    def __init__(
        self,
        engine: AsyncEngine,
        session_id: UUID,
        kill_at: int | None = None,
        max_seconds: float | None = None,
        on_tick=None,
        quiet: bool = False,
    ):
        self.engine = engine
        self.sid = session_id
        self.kill_at = kill_at
        self.max_seconds = max_seconds
        self.on_tick = on_tick  # called every loop (the supervisor uses it)
        self.quiet = quiet
        self.started = time.monotonic()
        self._last_progress = 0.0

    # --- main loop ----------------------------------------------------------

    async def run(self) -> str:
        """Returns 'finished', or 'killed' when --kill-at is reached."""
        while True:
            async with self.engine.begin() as conn:
                await self._sweep_expired_leases(conn)

            for task_id in await self._submitted_task_ids():
                async with self.engine.begin() as conn:
                    await self._process(conn, task_id)

            async with self.engine.begin() as conn:
                steps = await crud.step_count(conn, self.sid)
                finished = await self._check_goal_and_budget(conn, steps)

            await self._maybe_print_progress(steps)
            if self.kill_at is not None and steps >= self.kill_at:
                return "killed"
            if finished:
                return "finished"
            if self.on_tick:
                await self.on_tick()
            await asyncio.sleep(POLL_SECONDS)

    async def _submitted_task_ids(self) -> list[UUID]:
        async with self.engine.connect() as conn:
            q = (
                select(tasks.c.id)
                .where(tasks.c.session_id == self.sid, tasks.c.status == "submitted")
                .order_by(tasks.c.created_at)
            )
            return list((await conn.execute(q)).scalars())

    # --- results ------------------------------------------------------------

    async def _process(self, conn: AsyncConnection, task_id: UUID) -> None:
        """Accept or reject one submitted result, in one transaction.

        If we crash halfway, the transaction rolls back and the task is still
        `submitted`, so on resume it is simply processed again. There is never
        a state where facts exist but their follow-up tasks don't.
        """
        q = select(tasks).where(tasks.c.id == task_id).with_for_update()
        task = (await conn.execute(q)).one()
        if task.status != "submitted":
            return

        result = task.result or {}
        if "error" in result:
            err = result["error"]
            await self._retry_or_fail(conn, task, err["kind"], err.get("message", ""))
            return

        try:
            output = OUTPUT_MODELS[task.type].model_validate(result)
        except ValidationError as e:
            await self._retry_or_fail(conn, task, "invalid_output", str(e).splitlines()[0])
            return

        handlers = {
            "discover_host": self._accept_discover,
            "compare_service": self._accept_compare,
            "verify_drift": self._accept_verify,
            "write_report": self._accept_report,
        }
        try:
            # A savepoint, so if a check fails halfway its writes are undone.
            async with conn.begin_nested():
                await handlers[task.type](conn, task, output)
        except (Rejected, crud.FactConflict) as e:
            # FactConflict means the result would overwrite a verified fact,
            # which we never do silently, so it is treated like any other
            # rejected result.
            await self._retry_or_fail(conn, task, "rejected", str(e))
            return

        await conn.execute(update(tasks).where(tasks.c.id == task.id).values(status="succeeded"))
        await self._decide(conn, task, "accept", "")

    async def _accept_discover(self, conn: AsyncConnection, task: Row, out: DiscoverOutput) -> None:
        inp = DiscoverInput(**task.input)
        cited = await self._cited_responses(conn, task)

        # 1. Check every claim against the tool response it cites.
        host_resp = _cited(cited, out.host_event_id, "get_host")
        if out.host != inp.host or host_resp["host"] != inp.host:
            raise Rejected(f"host mismatch: {out.host}")
        for s in out.services:
            r = _cited(cited, s.event_id, "get_service")
            if (r["host"], r["service"], r["replicas"]) != (inp.host, s.service, s.replicas):
                raise Rejected(f"{s.service}: claimed {s.replicas}, response says {r['replicas']}")
        for d in out.documents:
            r = _cited(cited, d.event_id, "fetch_document")
            named = set(re.findall(r"\bhost-\d+\b", r["content"]))
            if r["name"] != d.name or not set(d.mentions) <= named:
                raise Rejected(f"{d.name}: mentions {d.mentions} not all in the document")
            if d.name == REGISTRY_DOC and d.registry != json.loads(r["content"]):
                raise Rejected("registry doesn't match registry.json")

        # 2. Write facts.
        sid, tid = self.sid, task.id
        await crud.upsert_fact(
            conn, sid, F.host_subject(inp.host), F.EXISTS, True,
            source_task_id=tid, source_event_id=UUID(out.host_event_id),
        )  # fmt: skip
        for s in out.services:
            await crud.upsert_fact(
                conn, sid, F.service_subject(s.service, inp.host), F.REPLICAS, s.replicas,
                source_task_id=tid, source_event_id=UUID(s.event_id),
            )  # fmt: skip
        registry_found = False
        for d in out.documents:
            await crud.upsert_fact(
                conn, sid, F.doc_subject(d.name, inp.host), F.MENTIONS, d.mentions,
                source_task_id=tid, source_event_id=UUID(d.event_id),
            )  # fmt: skip
            for service, expected in (d.registry or {}).items():
                registry_found = True
                await crud.upsert_fact(
                    conn, sid, F.registry_subject(service), F.EXPECTED, expected,
                    source_task_id=tid, source_event_id=UUID(d.event_id),
                )  # fmt: skip

        # 3. Follow-ups. Creating a task that already exists does nothing.
        for d in out.documents:
            for host in d.mentions:
                await crud.create_task(conn, sid, "discover_host", DiscoverInput(host=host), tid)
        # A compare needs both the read and the registry entry.
        for s in out.services:
            if await crud.current_fact(conn, sid, F.registry_subject(s.service), F.EXPECTED):
                inp_c = CompareInput(service=s.service, host=inp.host)
                await crud.create_task(conn, sid, "compare_service", inp_c, tid)
        if registry_found:
            # Services read before the registry was found get compared now.
            q = select(facts.c.subject).where(
                facts.c.session_id == sid, facts.c.key == F.REPLICAS, facts.c.status != F.SUPERSEDED
            )
            for subject in (await conn.execute(q)).scalars():
                service, host = subject.removeprefix("service:").split("@")
                inp_c = CompareInput(service=service, host=host)
                await crud.create_task(conn, sid, "compare_service", inp_c, tid)

    async def _accept_compare(self, conn: AsyncConnection, task: Row, out: CompareOutput) -> None:
        inp = CompareInput(**task.input)
        subject = F.service_subject(inp.service, inp.host)
        read = await self._cited_fact(conn, out.read_fact_id, subject, F.REPLICAS)
        expected = await self._cited_fact(
            conn, out.registry_fact_id, F.registry_subject(inp.service), F.EXPECTED
        )
        # Never take the model's word for something code can check exactly.
        if (read.value, expected.value) != (out.actual, out.expected):
            raise Rejected(
                f"claimed {out.actual} vs {out.expected}; facts say {read.value} vs {expected.value}"
            )
        drift = read.value != expected.value
        if out.drift != drift:
            raise Rejected(f"claimed drift={out.drift}, comparison says {drift}")

        if not drift:
            await crud.upsert_fact(conn, self.sid, subject, F.VERDICT, "match", source_task_id=task.id)
            return
        # A mismatch is only a hypothesis until independent reads agree.
        evidence = [{"event_id": str(read.source_event_id), "replicas": read.value}]
        await crud.upsert_fact(
            conn, self.sid, subject, F.DRIFT,
            {"expected": expected.value, "actual": read.value},
            status=F.INFERRED, evidence=evidence, source_task_id=task.id,
        )  # fmt: skip
        verify = VerifyInput(service=inp.service, host=inp.host, round=1)
        await crud.create_task(conn, self.sid, "verify_drift", verify, task.id)

    async def _accept_verify(self, conn: AsyncConnection, task: Row, out: VerifyOutput) -> None:
        inp = VerifyInput(**task.input)
        cited = await self._cited_responses(conn, task)
        r = _cited(cited, out.event_id, "get_service")
        if (r["host"], r["service"], r["replicas"]) != (inp.host, inp.service, out.replicas):
            raise Rejected(f"claimed {out.replicas}, response says {r['replicas']}")

        subject = F.service_subject(inp.service, inp.host)
        await crud.upsert_fact(
            conn, self.sid, subject, F.REPLICAS, out.replicas,
            source_task_id=task.id, source_event_id=UUID(out.event_id),
        )  # fmt: skip
        drift = await crud.current_fact(conn, self.sid, subject, F.DRIFT)
        if drift is None or drift.status != F.INFERRED:
            return  # already decided, so this read is just extra evidence

        # Two independent reads must agree. The newest read is not
        # automatically the truth, because it could be stale too.
        evidence = [*drift.evidence, {"event_id": out.event_id, "replicas": out.replicas}]
        value, votes = Counter(e["replicas"] for e in evidence).most_common(1)[0]
        expected = drift.value["expected"]
        if votes >= 2 and value == expected:
            await crud.update_fact(conn, self.sid, drift.id, status=F.REFUTED, evidence=evidence)
        elif votes >= 2:
            new_value = {"expected": expected, "actual": value}
            await crud.update_fact(
                conn, self.sid, drift.id, status=F.VERIFIED, evidence=evidence, value=new_value
            )
        else:
            # The reads disagree, so neither is trusted, and we break the tie
            # with another round, up to the cap.
            await crud.update_fact(conn, self.sid, drift.id, status=F.INFERRED, evidence=evidence)
            if inp.round < MAX_VERIFY_ROUNDS:
                nxt = VerifyInput(service=inp.service, host=inp.host, round=inp.round + 1)
                await crud.create_task(conn, self.sid, "verify_drift", nxt, task.id)

    async def _accept_report(self, conn: AsyncConnection, task: Row, out: ReportOutput) -> None:
        inp = ReportInput(**task.input)
        if out.drift_fact_id is None:
            if not inp.partial:
                raise Rejected("the report must cite the verified drift")
        else:
            subject = F.service_subject(out.service or "", out.host or "")
            fact = await self._cited_fact(conn, out.drift_fact_id, subject, F.DRIFT)
            if fact.status != F.VERIFIED:
                raise Rejected("the cited drift is not verified")
            if fact.value != {"expected": out.expected, "actual": out.actual}:
                raise Rejected(f"report says {out.expected}/{out.actual}, fact says {fact.value}")
        succeeded = out.drift_fact_id is not None and not inp.partial
        await conn.execute(
            update(sessions)
            .where(sessions.c.id == self.sid)
            .values(
                report=out.model_dump(mode="json"),
                status="succeeded" if succeeded else "failed",
                finished_at=func.now(),
            )
        )

    # --- source checks ------------------------------------------------------

    async def _cited_responses(self, conn: AsyncConnection, task: Row) -> dict[str, dict]:
        """Successful tool calls of THIS attempt, by event id."""
        q = select(events.c.id, events.c.payload).where(
            events.c.session_id == self.sid,
            events.c.task_id == task.id,
            events.c.attempt == task.attempt,
            events.c.kind == "tool_call",
        )
        return {str(i): p for i, p in (await conn.execute(q)).all() if p.get("ok")}

    async def _cited_fact(self, conn: AsyncConnection, fact_id: str, subject: str, key: str) -> Row:
        try:
            fact = await crud.get_fact(conn, UUID(fact_id))
        except ValueError:
            raise Rejected(f"not a fact id: {fact_id!r}") from None
        if fact is None or fact.session_id != self.sid:
            raise Rejected(f"unknown fact {fact_id}")
        if (fact.subject, fact.key) != (subject, key) or fact.status == F.SUPERSEDED:
            raise Rejected(f"fact {fact_id} is not the current {subject} {key}")
        return fact

    # --- retries and replanning ---------------------------------------------

    async def _retry_or_fail(self, conn: AsyncConnection, task: Row, kind: str, reason: str) -> None:
        """A retry is the same row with attempt + 1, after a backoff."""
        if kind not in PERMANENT_ERRORS and task.attempt < task.max_attempts:
            delay = BACKOFF_BASE_SECONDS * 2**task.attempt
            await conn.execute(
                update(tasks)
                .where(tasks.c.id == task.id)
                .values(
                    status="ready",
                    attempt=task.attempt + 1,
                    leased_by=None,
                    lease_expires_at=None,
                    result=None,
                    not_before=func.now() + timedelta(seconds=delay),
                )
            )
            await self._decide(conn, task, "retry", f"{kind}: {reason}")
            return
        await conn.execute(update(tasks).where(tasks.c.id == task.id).values(status="failed"))
        await self._decide(conn, task, "fail", f"{kind}: {reason}")
        await self._replan(conn, task, kind)

    async def _replan(self, conn: AsyncConnection, task: Row, kind: str) -> None:
        """Fixed rules for a task that failed for good. Nothing downstream was
        ever created from it, because follow-ups only come from accepted results."""
        inp: BaseModel = INPUT_MODELS[task.type](**task.input)
        sid = self.sid
        if isinstance(inp, DiscoverInput):
            subject = F.host_subject(inp.host)
            if kind == "not_found":
                await crud.upsert_fact(conn, sid, subject, F.EXISTS, False, source_task_id=task.id)
                return await self._decide(conn, task, "replan", "host does not exist; dropped")
            await crud.upsert_fact(conn, sid, subject, F.UNREACHABLE, True, source_task_id=task.id)
            if inp.round < MAX_ROUNDS:
                nxt = DiscoverInput(host=inp.host, round=inp.round + 1)
                await crud.create_task(conn, sid, task.type, nxt, task.id, NEW_ROUND_DELAY_SECONDS)
                return await self._decide(conn, task, "replan", f"new round {nxt.round} later")
        elif isinstance(inp, CompareInput):
            if inp.round < MAX_ROUNDS:
                nxt = CompareInput(service=inp.service, host=inp.host, round=inp.round + 1)
                await crud.create_task(conn, sid, task.type, nxt, task.id, NEW_ROUND_DELAY_SECONDS)
                return await self._decide(conn, task, "replan", f"new round {nxt.round} later")
        elif isinstance(inp, VerifyInput):
            if inp.round < MAX_VERIFY_ROUNDS:
                nxt = VerifyInput(service=inp.service, host=inp.host, round=inp.round + 1)
                await crud.create_task(conn, sid, task.type, nxt, task.id, NEW_ROUND_DELAY_SECONDS)
                return await self._decide(conn, task, "replan", f"verify round {nxt.round} later")
        elif isinstance(inp, ReportInput):
            await conn.execute(
                update(sessions).where(sessions.c.id == sid).values(status="failed", finished_at=func.now())
            )
            return await self._decide(conn, task, "replan", "report failed; session failed")
        await self._decide(conn, task, "replan", "out of rounds; given up")

    async def _sweep_expired_leases(self, conn: AsyncConnection) -> None:
        """A worker that stopped heartbeating has lost its task, so hand it out again.

        The attempt bump is the fencing token, which means that if that worker
        was only slow, its late submit now matches 0 rows and is dropped.
        """
        q = (
            select(tasks)
            .where(
                tasks.c.session_id == self.sid,
                tasks.c.status == "leased",
                tasks.c.lease_expires_at < func.now(),
            )
            .with_for_update()
        )
        for task in (await conn.execute(q)).all():
            await self._retry_or_fail(conn, task, "lease_expired", f"worker {task.leased_by} silent")

    # --- goal, budget, end --------------------------------------------------

    async def _check_goal_and_budget(self, conn: AsyncConnection, steps: int) -> bool:
        """Returns True when the session is over."""
        session = await crud.get_session(conn, self.sid)
        if session.status != "running":
            return True
        report = (
            await conn.execute(
                select(tasks.c.id).where(tasks.c.session_id == self.sid, tasks.c.type == "write_report")
            )
        ).first()
        if report is not None:
            return False  # waiting for the reporter

        reason = None
        if await self._goal_met(conn):
            reason, partial = "goal met: a verified drift backed by the registry", False
        elif steps >= session.step_budget:
            reason, partial = f"step budget ({session.step_budget}) used up", True
        elif self.max_seconds and time.monotonic() - self.started > self.max_seconds:
            reason, partial = f"time limit ({self.max_seconds:.0f}s) reached", True
        elif not await self._has_active_tasks(conn):
            reason, partial = "no work left and the goal is not met", True
        if reason is None:
            return False

        # Stop all other work, then hand over to the reporter. The reporter
        # is exempt from the step budget so a partial report still gets written.
        cancelled = await conn.execute(
            update(tasks)
            .where(tasks.c.session_id == self.sid, tasks.c.status.in_(ACTIVE))
            .values(status="cancelled")
            .returning(tasks.c.id)
        )
        n_cancelled = len(cancelled.all())
        await crud.create_task(conn, self.sid, "write_report", ReportInput(partial=partial))
        await crud.log_event(
            conn, self.sid, ACTOR, "goal_met" if not partial else "run_ending",
            {"reason": reason, "cancelled_tasks": n_cancelled, "steps": steps},
        )  # fmt: skip
        if not self.quiet:
            print(f"[coordinator] {reason}; cancelled {n_cancelled} leftover task(s); writing report")
        return False

    async def _goal_met(self, conn: AsyncConnection) -> bool:
        q = select(facts.c.subject).where(
            facts.c.session_id == self.sid, facts.c.key == F.DRIFT, facts.c.status == F.VERIFIED
        )
        for subject in (await conn.execute(q)).scalars():
            service = subject.removeprefix("service:").split("@")[0]
            if await crud.current_fact(conn, self.sid, F.registry_subject(service), F.EXPECTED):
                return True
        return False

    async def _has_active_tasks(self, conn: AsyncConnection) -> bool:
        q = select(tasks.c.id).where(tasks.c.session_id == self.sid, tasks.c.status.in_(ACTIVE))
        return (await conn.execute(q.limit(1))).first() is not None

    # --- logging ------------------------------------------------------------

    async def _decide(self, conn: AsyncConnection, task: Row, action: str, reason: str) -> None:
        """Every coordinator decision goes to `events`, with its reason."""
        payload: dict[str, Any] = {"action": action, "reason": reason, "task_key": task.task_key}
        await crud.log_event(conn, self.sid, ACTOR, "decision", payload, task.id, task.attempt)

    async def _maybe_print_progress(self, steps: int) -> None:
        now = time.monotonic()
        if self.quiet or now - self._last_progress < PROGRESS_EVERY_SECONDS:
            return
        self._last_progress = now
        print(f"[coordinator] {await progress_line(self.engine, self.sid, steps)}")


async def progress_line(engine: AsyncEngine, sid: UUID, steps: int) -> str:
    """A one-line summary, worked out again from the database every time (and
    never from a previous summary)."""
    async with engine.connect() as conn:
        by_status = dict(
            (
                await conn.execute(
                    select(tasks.c.status, func.count())
                    .where(tasks.c.session_id == sid)
                    .group_by(tasks.c.status)
                )
            ).all()
        )
        claims = dict(
            (
                await conn.execute(
                    select(facts.c.status, func.count())
                    .where(facts.c.session_id == sid, facts.c.key == F.DRIFT)
                    .group_by(facts.c.status)
                )
            ).all()
        )
        hosts = (
            await conn.execute(
                select(func.count()).where(
                    facts.c.session_id == sid,
                    facts.c.key == F.EXISTS,
                    facts.c.value == literal(True, JSONB),
                )
            )
        ).scalar_one()
    active = sum(by_status.get(s, 0) for s in ACTIVE)
    return (
        f"step {steps:4d} | tasks: {by_status.get('succeeded', 0)} done, {active} active, "
        f"{by_status.get('failed', 0)} failed | hosts found: {hosts} | drift claims: "
        f"{claims.get(F.INFERRED, 0)} inferred, {claims.get(F.VERIFIED, 0)} verified, "
        f"{claims.get(F.REFUTED, 0)} refuted"
    )


def _cited(cited: dict[str, dict], event_id: str, tool: str) -> dict:
    """The response of a successful tool call from this attempt, or Rejected."""
    payload = cited.get(event_id)
    if payload is None or payload.get("tool") != tool:
        raise Rejected(f"{tool} event {event_id} is not a successful call from this attempt")
    return payload["response"]
