"""The coordinator, which is plain code and not an LLM, and which owns the plan.

    python -m lha.coordinator --session <id>

It is easiest to think of it as a project manager with a to-do board
(`tasks`) and a notebook of findings (`facts`), and every loop it does four
things.

  1. reclaims tasks whose worker went silent (expired leases)
  2. processes submitted results, one transaction each:
       - error or invalid output  -> retry (same row, attempt + 1) or fail
       - too big for one attempt  -> split it into batches
       - valid                    -> check every fact against its source,
                                     write facts, create follow-up tasks
  3. checks the goal (or the budget, the time limit or a stall): if met or
     out -> create the report task
  4. stops when the report is accepted (a partial one if the run ran out)

Workers propose and the coordinator decides. It is the only writer of facts
and of the plan, so there is a single source of truth. It runs as its own
process, the supervisor starts it again if it dies, and a Postgres advisory
lock makes sure only one coordinator works on a session at a time.
"""

import argparse
import asyncio
import json
import re
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ValidationError
from sqlalchemy import func, literal, or_, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from lha.config import (
    BACKOFF_BASE_SECONDS,
    BREAKER_COOLDOWN_SECONDS,
    BREAKER_THRESHOLD,
    LOCK_WAIT_SECONDS,
    MAX_ROUNDS,
    MAX_VERIFY_ROUNDS,
    NEW_ROUND_DELAY_SECONDS,
    POLL_SECONDS,
    SPLIT_BATCH,
    STALL_STEPS,
)
from lha.db import crud, make_engine
from lha.db.models import events, facts, sessions, tasks
from lha.faults import roll
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
# Failures that say something about the host, which feed its circuit breaker.
NETWORK_TASKS = ("discover_host", "verify_drift")
NETWORK_ERRORS = ("timeout", "server_error", "rate_limited", "empty_response", "malformed_response",
                  "connection_error")  # fmt: skip


class Rejected(Exception):
    """A result failed the coordinator's checks (for example a fact doesn't match its source)."""


class InjectedCrash(Exception):
    """A seeded coordinator crash, in the middle of committing a result (see --crashes)."""


class Coordinator:
    def __init__(
        self,
        engine: AsyncEngine,
        session_id: UUID,
        deadline: float | None = None,
        quiet: bool = False,
    ):
        self.engine = engine
        self.sid = session_id
        self.deadline = deadline  # wall-clock time limit, as a Unix time
        self.quiet = quiet

    # --- main loop ----------------------------------------------------------

    async def run(self) -> str:
        """Returns 'finished', or 'locked' if another coordinator never let go."""
        async with self.engine.connect() as lock_conn:
            # The lock lives as long as this connection, so it is released
            # when this process dies, however it dies.
            await lock_conn.execution_options(isolation_level="AUTOCOMMIT")
            if not await self._take_lock(lock_conn):
                return "locked"
            try:
                await self._loop()
            finally:
                await lock_conn.execute(text("SELECT pg_advisory_unlock_all()"))
            return "finished"

    async def _loop(self) -> None:
        async with self.engine.connect() as conn:
            self.session = await crud.get_session(conn, self.sid)
        while True:
            async with self.engine.begin() as conn:
                await self._sweep_expired_leases(conn)

            for task_id in await self._submitted_task_ids():
                async with self.engine.begin() as conn:
                    await self._process(conn, task_id)

            async with self.engine.begin() as conn:
                if await self._check_goal_and_budget(conn):
                    return
            await asyncio.sleep(POLL_SECONDS)

    async def _take_lock(self, conn: AsyncConnection) -> bool:
        """One coordinator per session, which matters when a restarted one starts
        while an old one has not fully died yet."""
        key = f"coordinator:{self.sid}"
        waited = 0.0
        while True:
            q = text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))")
            if (await conn.execute(q, {"key": key})).scalar_one():
                return True
            if waited == 0.0:
                self._say("another coordinator holds this session; waiting for it to let go")
            if waited >= LOCK_WAIT_SECONDS:
                return False
            await asyncio.sleep(0.5)
            waited += 0.5

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
        `submitted`, so after a restart it is simply processed again. There is
        never a state where facts exist but their follow-up tasks don't.
        """
        q = select(tasks).where(tasks.c.id == task_id).with_for_update()
        task = (await conn.execute(q)).one()
        if task.status != "submitted":
            return

        result = task.result or {}
        if "error" in result:
            err = result["error"]
            await self._retry_or_fail(conn, task, err["kind"], err.get("message", ""))
        else:
            await self._accept_or_reject(conn, task, result)

        # A seeded crash after the writes and before the commit, which the
        # transaction undoes, and the next coordinator does this result again.
        r = roll(self.session.seed, "coordinator", task.task_key, task.attempt)
        if r < self.session.crash_rate and not await self._crashed_on(task):
            raise InjectedCrash(f"while processing {task.task_key} (attempt {task.attempt})")

    async def _crashed_on(self, task: Row) -> bool:
        """Whether an injected crash already hit this result, and if not, record that one is about to.

        The record is written in its own transaction, so it survives the
        crash, and a result crashes a coordinator at most once, because an
        injected crash stands for bad luck and not for a bug.
        """
        async with self.engine.begin() as conn:
            q = select(events.c.id).where(
                events.c.session_id == self.sid,
                events.c.task_id == task.id,
                events.c.attempt == task.attempt,
                events.c.kind == "crash_injected",
                events.c.actor == ACTOR,
            )
            if (await conn.execute(q)).first() is not None:
                return True
            await crud.log_event(
                conn, self.sid, ACTOR, "crash_injected", {"kind": "coordinator"}, task.id, task.attempt
            )
        return False

    async def _accept_or_reject(self, conn: AsyncConnection, task: Row, result: dict) -> None:
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
        await self._breaker_success(conn, task)

    async def _accept_discover(self, conn: AsyncConnection, task: Row, out: DiscoverOutput) -> None:
        inp = DiscoverInput(**task.input)
        cited = await self._cited_responses(conn, task)

        # 1. Check every claim against the tool response it cites, and that
        # nothing this task was meant to read is missing.
        host_resp = _cited(cited, out.host_event_id, "get_host")
        if out.host != inp.host or host_resp["host"] != inp.host:
            raise Rejected(f"host mismatch: {out.host}")
        wanted = inp.services if inp.services is not None else host_resp["services"]
        if sorted(s.service for s in out.services) != sorted(wanted):
            raise Rejected(f"services {[s.service for s in out.services]}, expected {wanted}")
        wanted_docs = host_resp["documents"] if inp.part <= 1 else []
        if sorted(d.name for d in out.documents) != sorted(wanted_docs):
            raise Rejected(f"documents {[d.name for d in out.documents]}, expected {wanted_docs}")
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
        listing = {"services": host_resp["services"], "documents": host_resp["documents"]}
        for key, value in ((F.EXISTS, True), (F.LISTING, listing)):
            await crud.upsert_fact(
                conn, sid, F.host_subject(inp.host), key, value,
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
                await self._create(conn, "discover_host", DiscoverInput(host=host), tid)
        # A compare needs both the read and the registry entry.
        for s in out.services:
            if await crud.current_fact(conn, sid, F.registry_subject(s.service), F.EXPECTED):
                await self._create(
                    conn, "compare_service", CompareInput(service=s.service, host=inp.host), tid
                )
        if registry_found:
            # Services read before the registry was found get compared now.
            q = select(facts.c.subject).where(
                facts.c.session_id == sid, facts.c.key == F.REPLICAS, facts.c.status != F.SUPERSEDED
            )
            for subject in (await conn.execute(q)).scalars():
                service, host = F.split_service_subject(subject)
                await self._create(conn, "compare_service", CompareInput(service=service, host=host), tid)

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
        await self._create(conn, "verify_drift", verify, task.id)

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
                await self._create(conn, "verify_drift", nxt, task.id)

    async def _accept_report(self, conn: AsyncConnection, task: Row, out: ReportOutput) -> None:
        """The report must list exactly the verified drifts, each matching its fact."""
        inp = ReportInput(**task.input)
        q = select(facts.c.id).where(
            facts.c.session_id == self.sid, facts.c.key == F.DRIFT, facts.c.status == F.VERIFIED
        )
        verified = {str(i) for i in (await conn.execute(q)).scalars()}
        cited = [f.drift_fact_id for f in out.findings]
        if sorted(cited) != sorted(verified):
            raise Rejected(f"the report cites {len(cited)} drift(s), {len(verified)} are verified")
        for finding in out.findings:
            subject = F.service_subject(finding.service, finding.host)
            fact = await self._cited_fact(conn, finding.drift_fact_id, subject, F.DRIFT)
            if fact.value != {"expected": finding.expected, "actual": finding.actual}:
                raise Rejected(f"report says {finding.expected}/{finding.actual}, fact says {fact.value}")
        succeeded = bool(out.findings) and not inp.partial
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
        """Successful tool calls of THIS attempt, by event id.

        A fetched pointer counts as the tool it copies, but only once its copy
        has been checked against the original event, so a worker can't make
        up a response and call it a copy.
        """
        q = select(events.c.id, events.c.payload).where(
            events.c.session_id == self.sid,
            events.c.task_id == task.id,
            events.c.attempt == task.attempt,
            events.c.kind == "tool_call",
        )
        cited = {}
        for event_id, p in (await conn.execute(q)).all():
            if not p.get("ok"):
                continue
            if p["tool"] == "fetch_pointer":
                if not await self._copy_is_true(conn, task, p):
                    continue
                p = {**p, "tool": p["of"]}
            cited[str(event_id)] = p
        return cited

    async def _copy_is_true(self, conn: AsyncConnection, task: Row, copy: dict) -> bool:
        try:
            original_id = UUID(str(copy.get("pointer")))
        except ValueError:
            return False
        q = select(events).where(
            events.c.id == original_id,
            events.c.session_id == self.sid,
            await crud.earlier_reads(conn, task),
        )
        original = (await conn.execute(q)).one_or_none()
        return (
            original is not None
            and original.kind == "tool_call"
            and original.payload.get("ok") is True
            and original.payload.get("tool") == copy.get("of")
            and original.payload.get("response") == copy.get("response")
        )

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

    # --- retries, splits and replanning -------------------------------------

    async def _retry_or_fail(self, conn: AsyncConnection, task: Row, kind: str, reason: str) -> None:
        """A retry is the same row with attempt + 1, after a backoff."""
        probe_at = await self._breaker_failure(conn, task, kind)
        if kind == "context_overflow" and await self._split(conn, task, reason):
            return
        if kind not in PERMANENT_ERRORS and task.attempt < task.max_attempts:
            not_before = probe_at or _now() + timedelta(seconds=BACKOFF_BASE_SECONDS * 2**task.attempt)
            await conn.execute(
                update(tasks)
                .where(tasks.c.id == task.id)
                .values(
                    status="ready",
                    attempt=task.attempt + 1,
                    leased_by=None,
                    lease_expires_at=None,
                    result=None,
                    not_before=not_before,
                )
            )
            await self._decide(conn, task, "retry", f"{kind}: {reason}")
            return
        await conn.execute(update(tasks).where(tasks.c.id == task.id).values(status="failed"))
        await self._decide(conn, task, "fail", f"{kind}: {reason}")
        await self._replan(conn, task, kind, probe_at)

    async def _split(self, conn: AsyncConnection, task: Row, reason: str) -> bool:
        """Split a discovery that didn't fit in one attempt into batches of services.

        The list of services comes from the failed attempt's own get_host
        call, which is a raw response in `events`, and not from anything the
        worker claimed. A batch that still doesn't fit is not split again,
        so it fails like any other permanent error.
        """
        if task.type != "discover_host":
            return False
        inp = DiscoverInput(**task.input)
        if inp.parts:
            return False
        host_resp = next(
            (
                p["response"]
                for p in (await self._cited_responses(conn, task)).values()
                if p["tool"] == "get_host"
            ),
            None,
        )
        if host_resp is None or len(host_resp["services"]) <= 1:
            return False
        services = host_resp["services"]
        batches = [services[i : i + SPLIT_BATCH] for i in range(0, len(services), SPLIT_BATCH)]
        for n, batch in enumerate(batches, 1):
            part = DiscoverInput(host=inp.host, round=inp.round, services=batch, part=n, parts=len(batches))
            await self._create(conn, "discover_host", part, task.id)
        await conn.execute(update(tasks).where(tasks.c.id == task.id).values(status="split"))
        await self._decide(conn, task, "split", f"{reason}; split into {len(batches)} batches")
        return True

    async def _replan(self, conn: AsyncConnection, task: Row, kind: str, probe_at: datetime | None) -> None:
        """Fixed rules for a task that failed for good. Nothing downstream was
        ever created from it, because follow-ups only come from accepted results."""
        inp: BaseModel = INPUT_MODELS[task.type](**task.input)
        sid = self.sid
        delay = NEW_ROUND_DELAY_SECONDS
        if probe_at is not None:
            delay = max(delay, (probe_at - _now()).total_seconds())
        if kind == "context_overflow":
            # A new round would be just as big, so there is no point.
            return await self._decide(conn, task, "replan", "too big for one attempt; given up")
        if isinstance(inp, DiscoverInput):
            subject = F.host_subject(inp.host)
            if kind == "not_found":
                await crud.upsert_fact(conn, sid, subject, F.EXISTS, False, source_task_id=task.id)
                return await self._decide(conn, task, "replan", "host does not exist; dropped")
            await crud.upsert_fact(conn, sid, subject, F.UNREACHABLE, True, source_task_id=task.id)
            if inp.round < MAX_ROUNDS:
                nxt = inp.model_copy(update={"round": inp.round + 1})
                await self._create(conn, task.type, nxt, task.id, delay, probe=probe_at is not None)
                return await self._decide(conn, task, "replan", f"new round {nxt.round} later")
        elif isinstance(inp, CompareInput):
            if inp.round < MAX_ROUNDS:
                nxt = CompareInput(service=inp.service, host=inp.host, round=inp.round + 1)
                await self._create(conn, task.type, nxt, task.id, delay)
                return await self._decide(conn, task, "replan", f"new round {nxt.round} later")
        elif isinstance(inp, VerifyInput):
            if inp.round < MAX_VERIFY_ROUNDS:
                nxt = VerifyInput(service=inp.service, host=inp.host, round=inp.round + 1)
                await self._create(conn, task.type, nxt, task.id, delay, probe=probe_at is not None)
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

    # --- the circuit breaker --------------------------------------------------

    async def _create(
        self,
        conn: AsyncConnection,
        task_type: str,
        inp: BaseModel,
        parent: UUID | None,
        delay: float = 0.0,
        probe: bool = False,
    ) -> UUID | None:
        """Create a task, holding it back while its host's breaker is open.

        The one task that tests whether the host is back (the probe) may run
        when the cooldown ends, and every other task for the host waits
        another cooldown on top, or until the probe succeeds.
        """
        host = getattr(inp, "host", None)
        if task_type in NETWORK_TASKS and host:
            breaker = await crud.current_fact(conn, self.sid, F.host_subject(host), F.BREAKER)
            if breaker is not None and breaker.value["state"] == "open":
                until = datetime.fromisoformat(breaker.value["open_until"])
                if not probe:
                    until += timedelta(seconds=BREAKER_COOLDOWN_SECONDS)
                delay = max(delay, (until - _now()).total_seconds())
        return await crud.create_task(conn, self.sid, task_type, inp, parent, delay)

    async def _breaker_failure(self, conn: AsyncConnection, task: Row, kind: str) -> datetime | None:
        """Count a failed network attempt against its host, and open the breaker
        after BREAKER_THRESHOLD in a row. Returns when the probe may run."""
        host = task.input.get("host")
        if task.type not in NETWORK_TASKS or kind not in NETWORK_ERRORS or not host:
            return None
        subject = F.host_subject(host)
        cur = await crud.current_fact(conn, self.sid, subject, F.BREAKER)
        failures = (cur.value["failures"] if cur else 0) + 1
        if failures < BREAKER_THRESHOLD:
            value = {"state": "closed", "failures": failures, "open_until": None}
            await crud.upsert_fact(conn, self.sid, subject, F.BREAKER, value, source_task_id=task.id)
            return None
        until = _now() + timedelta(seconds=BREAKER_COOLDOWN_SECONDS)
        value = {"state": "open", "failures": failures, "open_until": until.isoformat()}
        await crud.upsert_fact(conn, self.sid, subject, F.BREAKER, value, source_task_id=task.id)
        # Hold back everything else waiting for this host.
        held = until + timedelta(seconds=BREAKER_COOLDOWN_SECONDS)
        await conn.execute(
            update(tasks)
            .where(*self._ready_for_host(host), tasks.c.id != task.id)
            .values(not_before=func.greatest(func.coalesce(tasks.c.not_before, held), held))
        )
        await self._decide(conn, task, "breaker_open", f"{host}: {failures} failures in a row")
        return until

    async def _breaker_success(self, conn: AsyncConnection, task: Row) -> None:
        """A successful network task closes its host's breaker and releases what it held."""
        host = task.input.get("host")
        if task.type not in NETWORK_TASKS or not host:
            return
        subject = F.host_subject(host)
        cur = await crud.current_fact(conn, self.sid, subject, F.BREAKER)
        if cur is None or cur.value["failures"] == 0:
            return
        value = {"state": "closed", "failures": 0, "open_until": None}
        await crud.upsert_fact(conn, self.sid, subject, F.BREAKER, value, source_task_id=task.id)
        if cur.value["state"] == "open":
            await conn.execute(
                update(tasks)
                .where(*self._ready_for_host(host), tasks.c.not_before > func.now())
                .values(not_before=func.now())
            )
            await self._decide(conn, task, "breaker_closed", f"{host} answered again")

    def _ready_for_host(self, host: str) -> list:
        return [
            tasks.c.session_id == self.sid,
            tasks.c.status == "ready",
            tasks.c.type.in_(NETWORK_TASKS),
            tasks.c.input["host"].as_string() == host,
        ]

    # --- goal, budget, stalls, end --------------------------------------------

    async def _check_goal_and_budget(self, conn: AsyncConnection) -> bool:
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

        steps = await crud.step_count(conn, self.sid)
        reason = None
        if await self._goal_met(conn, session):
            reason, partial = _GOAL_TEXT[session.goal_kind], False
        elif steps >= session.step_budget:
            reason, partial = f"step budget ({session.step_budget}) used up", True
        elif self.deadline and time.time() > self.deadline:
            reason, partial = "time limit reached", True
        elif not await self._has_active_tasks(conn):
            if await self._reopen(conn, "no work left"):
                return False
            reason, partial = "no work left and the goal is not met", True
        elif await self._steps_since_progress(conn) >= STALL_STEPS:
            if await self._reopen(conn, f"no result accepted in {STALL_STEPS} steps"):
                return False
            reason, partial = f"stalled: no result accepted in {STALL_STEPS} steps", True
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
        self._say(f"{reason}; cancelled {n_cancelled} leftover task(s); writing report")
        return False

    async def _goal_met(self, conn: AsyncConnection, session: Row) -> bool:
        if session.goal_kind == "all":
            return await self._everything_checked(conn, session)
        q = select(facts.c.subject).where(
            facts.c.session_id == self.sid, facts.c.key == F.DRIFT, facts.c.status == F.VERIFIED
        )
        for subject in (await conn.execute(q)).scalars():
            service, _ = F.split_service_subject(subject)
            if await crud.current_fact(conn, self.sid, F.registry_subject(service), F.EXPECTED):
                return True
        return False

    async def _everything_checked(self, conn: AsyncConnection, session: Row) -> bool:
        """The 'find all drifts' goal, which is met when no host is left unexplored
        and every service has a verdict."""
        facts_now = await self._current_facts(conn)
        if not facts_now.get(F.EXPECTED):
            return False  # the registry hasn't been found yet
        if self._unread(session, facts_now):
            return False
        for subject in facts_now.get(F.REPLICAS, {}):
            drift = facts_now.get(F.DRIFT, {}).get(subject)
            decided = drift is not None and drift.status in (F.VERIFIED, F.REFUTED)
            if subject not in facts_now.get(F.VERDICT, {}) and not decided:
                return False
        return True

    async def _current_facts(self, conn: AsyncConnection) -> dict[str, dict[str, Row]]:
        """Every current fact of the session, by key and then by subject."""
        q = select(facts.c.subject, facts.c.key, facts.c.value, facts.c.status).where(
            facts.c.session_id == self.sid, facts.c.status != F.SUPERSEDED
        )
        by_key: dict[str, dict[str, Row]] = {}
        for row in (await conn.execute(q)).all():
            by_key.setdefault(row.key, {})[row.subject] = row
        return by_key

    def _unread(self, session: Row, facts_now: dict[str, dict[str, Row]]) -> dict[str, tuple[list, list]]:
        """What is known to exist but hasn't been read, as host -> (services, documents).

        A host that some document mentions but nobody has read yet appears with
        None for both, because its listing is not known yet.
        """
        exists, listings = facts_now.get(F.EXISTS, {}), facts_now.get(F.LISTING, {})
        mentioned = set(session.start_hosts)
        for row in facts_now.get(F.MENTIONS, {}).values():
            mentioned.update(row.value)
        unread: dict[str, tuple] = {}
        for host in sorted(mentioned):
            if F.host_subject(host) not in exists:
                unread[host] = (None, None)
        for subject, row in listings.items():
            host = subject.removeprefix("host:")
            services = [
                s
                for s in row.value["services"]
                if F.service_subject(s, host) not in facts_now.get(F.REPLICAS, {})
            ]
            documents = [
                d
                for d in row.value["documents"]
                if F.doc_subject(d, host) not in facts_now.get(F.MENTIONS, {})
            ]
            if services or documents:
                unread[host] = (services, documents)
        return unread

    async def _has_active_tasks(self, conn: AsyncConnection) -> bool:
        q = select(tasks.c.id).where(tasks.c.session_id == self.sid, tasks.c.status.in_(ACTIVE))
        return (await conn.execute(q.limit(1))).first() is not None

    async def _steps_since_progress(self, conn: AsyncConnection) -> int:
        """Steps since a result was last accepted or work was last re-opened."""
        last = (
            select(func.max(events.c.created_at))
            .where(
                events.c.session_id == self.sid,
                events.c.kind == "decision",
                events.c.payload["action"].as_string().in_(("accept", "reopen")),
            )
            .scalar_subquery()
        )
        q = select(func.count()).where(
            events.c.session_id == self.sid,
            events.c.kind.in_(crud.STEP_KINDS),
            or_(last.is_(None), events.c.created_at > last),
        )
        return (await conn.execute(q)).scalar_one()

    async def _reopen(self, conn: AsyncConnection, why: str) -> int:
        """Stall detection's second chance, which re-opens work that was put off.

        That is one more round for hosts given up as unreachable, for drifts
        still undecided and for services that never got a verdict, where
        nothing is already working on them. Each re-opened task has a round
        past the usual cap, so its task_key is new, and a second re-open of the
        same thing does nothing because the key already exists.
        """
        busy = await conn.execute(
            select(tasks.c.type, tasks.c.input).where(
                tasks.c.session_id == self.sid, tasks.c.status.in_(ACTIVE)
            )
        )
        working = {(t, i.get("host"), i.get("service")) for t, i in busy.all()}
        current = await conn.execute(
            select(facts.c.subject, facts.c.key, facts.c.value, facts.c.status).where(
                facts.c.session_id == self.sid, facts.c.status != F.SUPERSEDED
            )
        )
        rows = current.all()
        has = {(r.subject, r.key) for r in rows}
        created = 0
        for r in rows:
            nxt: tuple[str, BaseModel] | None = None
            if r.key == F.UNREACHABLE and (r.subject, F.EXISTS) not in has:
                host = r.subject.removeprefix("host:")
                if ("discover_host", host, None) not in working:
                    nxt = ("discover_host", DiscoverInput(host=host, round=MAX_ROUNDS + 1))
            elif r.key == F.DRIFT and r.status == F.INFERRED:
                service, host = F.split_service_subject(r.subject)
                if ("verify_drift", host, service) not in working:
                    nxt = (
                        "verify_drift",
                        VerifyInput(service=service, host=host, round=MAX_VERIFY_ROUNDS + 1),
                    )
            elif (
                r.key == F.REPLICAS and (r.subject, F.VERDICT) not in has and (r.subject, F.DRIFT) not in has
            ):
                service, host = F.split_service_subject(r.subject)
                if ("compare_service", host, service) not in working and (
                    F.registry_subject(service),
                    F.EXPECTED,
                ) in has:
                    nxt = ("compare_service", CompareInput(service=service, host=host, round=MAX_ROUNDS + 1))
            if nxt is not None and await self._create(conn, nxt[0], nxt[1], None):
                created += 1
        # Hosts read only in part, because a batch of a split discovery failed
        # for good, get the rest read in batches of their own.
        session = await crud.get_session(conn, self.sid)
        for host, (services, _) in self._unread(session, await self._current_facts(conn)).items():
            if services is None or ("discover_host", host, None) in working:
                continue
            batches = [services[i : i + SPLIT_BATCH] for i in range(0, len(services), SPLIT_BATCH)] or [[]]
            for n, batch in enumerate(batches, 1):
                inp = DiscoverInput(
                    host=host, round=MAX_ROUNDS + 1, services=batch, part=n, parts=len(batches)
                )
                if await self._create(conn, "discover_host", inp, None):
                    created += 1
        if created:
            await self._decide(conn, None, "reopen", f"{why}; re-opened {created} task(s)")
            self._say(f"{why}; re-opened {created} task(s)")
        return created

    # --- logging ------------------------------------------------------------

    async def _decide(self, conn: AsyncConnection, task: Row | None, action: str, reason: str) -> None:
        """Every coordinator decision goes to `events`, with its reason."""
        payload: dict[str, Any] = {
            "action": action,
            "reason": reason,
            "task_key": task.task_key if task else None,
        }
        task_id, attempt = (task.id, task.attempt) if task else (None, None)
        await crud.log_event(conn, self.sid, ACTOR, "decision", payload, task_id, attempt)

    def _say(self, message: str) -> None:
        if not self.quiet:
            print(f"[coordinator] {message}", flush=True)


_GOAL_TEXT = {
    "one": "goal met: a verified drift backed by the registry",
    "all": "goal met: every host explored and every service has a verdict",
}


def _now() -> datetime:
    return datetime.now(UTC)


async def progress_line(engine: AsyncEngine, sid: UUID) -> str:
    """A one-line summary, worked out again from the database every time (and
    never from a previous summary)."""
    async with engine.connect() as conn:
        steps = await crud.step_count(conn, sid)
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
                    facts.c.status != F.SUPERSEDED,
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


async def main(args: argparse.Namespace) -> int:
    engine = make_engine()
    coordinator = Coordinator(engine, UUID(args.session), deadline=args.deadline, quiet=args.quiet)
    try:
        outcome = await coordinator.run()
    except InjectedCrash as e:
        print(f"[coordinator] injected crash {e}", flush=True)
        return 1
    finally:
        await engine.dispose()
    return 0 if outcome == "finished" else 3


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--session", required=True)
    parser.add_argument("--deadline", type=float, help="time limit, as a Unix time")
    parser.add_argument("--quiet", action="store_true")
    try:
        sys.exit(asyncio.run(main(parser.parse_args())))
    except KeyboardInterrupt:
        sys.exit(0)
