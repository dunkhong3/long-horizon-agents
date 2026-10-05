"""The coordinator, which is plain code and not an LLM, and which owns the plan.

    python -m lha.core.coordinator --session <id> [--partition N]

It is easiest to think of it as a project manager with a to-do board
(`tasks`) and a notebook of findings (`facts`), and every loop it does four
things.

  1. reclaims tasks whose worker went silent (expired leases)
  2. processes submitted results, one transaction each:
       - error or invalid output  -> retry (same row, attempt + 1) or fail
       - too big for one attempt  -> split it, if the domain can
       - valid                    -> the domain checks every fact against its
                                     source, writes facts, creates follow-ups
  3. checks the goal (or the budget, the time limit or a stall): if met or
     out -> create the report task
  4. stops when the report is accepted (a partial one if the run ran out)

Workers propose and the coordinator decides. It is the only writer of facts
and of the plan, so there is a single source of truth. It runs as its own
process, the supervisor starts it again if it dies, and a Postgres advisory
lock makes sure only one coordinator works on a partition at a time.

Everything here is the same for every domain. What a result means, how a
failed task is replanned, and when the goal is met are the domain's rules
(lha/core/domain.py), which this loop calls.
"""

import argparse
import asyncio
import contextlib
import sys
import time
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ValidationError
from sqlalchemy import func, or_, select, text, update
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from lha.config import (
    BACKOFF_BASE_SECONDS,
    BREAKER_COOLDOWN_SECONDS,
    BREAKER_THRESHOLD,
    COORDINATOR_IDLE_SECONDS,
    GOAL_CHECK_SECONDS,
    LOCK_WAIT_SECONDS,
    NEW_ROUND_DELAY_SECONDS,
    POLL_SECONDS,
    STALL_STEPS,
)
from lha.core.domain import Domain, Rejected, create_task, get_domain
from lha.core.schemas import SUPERSEDED
from lha.db import crud, make_engine
from lha.db.models import events, facts, sessions, tasks
from lha.db.notify import Listener
from lha.faults import roll

ACTOR = "coordinator"
ACTIVE = ("ready", "leased", "submitted")
PERMANENT_ERRORS = ("not_found", "context_overflow")
# Failures that say something about a resource, which feed its circuit breaker.
NETWORK_ERRORS = ("timeout", "server_error", "rate_limited", "empty_response", "malformed_response",
                  "connection_error")  # fmt: skip
BREAKER = "breaker"  # resource:<r> -> {"state", "failures", "open_until"}


class InjectedCrash(Exception):
    """A seeded coordinator crash, in the middle of committing a result (see --crashes)."""


class Coordinator:
    def __init__(
        self,
        engine: AsyncEngine,
        session_id: UUID,
        partition: int = 0,
        deadline: float | None = None,
        quiet: bool = False,
    ):
        self.engine = engine
        self.sid = session_id
        self.partition = partition  # which part of the plan this coordinator decides
        self.leader = partition == 0  # the leader also checks the goal and ends the run
        self.deadline = deadline  # wall-clock time limit, as a Unix time
        self.quiet = quiet
        self.session: Row
        self.domain: Domain

    async def load(self) -> None:
        async with self.engine.connect() as conn:
            self.session = await crud.get_session(conn, self.sid)
        self.domain = get_domain(self.session.domain)

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
                await self.load()
                await self._loop()
            finally:
                await lock_conn.execute(text("SELECT pg_advisory_unlock_all()"))
            return "finished"

    async def _loop(self) -> None:
        listener = None
        if self.session.wakeups == "notify":
            listener = Listener("lha_submitted", f"{self.sid}:{self.partition}")
        last_check = 0.0
        async with listener or contextlib.nullcontext():
            while True:
                async with self.engine.begin() as conn:
                    await self._sweep_expired_leases(conn)

                submitted = await self._submitted_task_ids()
                for task_id in submitted:
                    async with self.engine.begin() as conn:
                        await self._process(conn, task_id)

                # The goal check reads every current fact, so it runs at most
                # every GOAL_CHECK_SECONDS and not after every result.
                if time.monotonic() - last_check >= GOAL_CHECK_SECONDS:
                    last_check = time.monotonic()
                    async with self.engine.begin() as conn:
                        if await self._check_goal_and_budget(conn):
                            return
                if submitted:
                    continue  # more may have arrived meanwhile
                if listener is None:
                    await asyncio.sleep(POLL_SECONDS)
                else:
                    # A submitted result wakes us at once, and the timeout keeps
                    # the sweep of expired leases and the goal check going.
                    await listener.wait(COORDINATOR_IDLE_SECONDS)

    async def _take_lock(self, conn: AsyncConnection) -> bool:
        """One coordinator per partition, which matters when a restarted one
        starts while an old one has not fully died yet."""
        key = f"coordinator:{self.sid}:{self.partition}"
        waited = 0.0
        while True:
            q = text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))")
            if (await conn.execute(q, {"key": key})).scalar_one():
                return True
            if waited == 0.0:
                self.say("another coordinator holds this partition; waiting for it to let go")
            if waited >= LOCK_WAIT_SECONDS:
                return False
            await asyncio.sleep(0.5)
            waited += 0.5

    async def _submitted_task_ids(self) -> list[UUID]:
        async with self.engine.connect() as conn:
            q = (
                select(tasks.c.id)
                .where(
                    tasks.c.session_id == self.sid,
                    tasks.c.partition == self.partition,
                    tasks.c.status == "submitted",
                )
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
            payload = {"kind": "coordinator"}
            await crud.log_event(conn, self.sid, ACTOR, "crash_injected", payload, task.id, task.attempt)
        return False

    async def _accept_or_reject(self, conn: AsyncConnection, task: Row, result: dict) -> None:
        try:
            output = self.domain.task_types[task.type].output.model_validate(result)
        except ValidationError as e:
            await self._retry_or_fail(conn, task, "invalid_output", str(e).splitlines()[0])
            return
        try:
            # A savepoint, so if a check fails halfway its writes are undone.
            async with conn.begin_nested():
                await self.domain.accept(self, conn, task, output)
        except (Rejected, crud.FactConflict) as e:
            # FactConflict means the result would overwrite a verified fact,
            # which we never do silently, so it is treated like any other
            # rejected result.
            await self._retry_or_fail(conn, task, "rejected", str(e))
            return

        await conn.execute(update(tasks).where(tasks.c.id == task.id).values(status="succeeded"))
        await self.decide(conn, task, "accept", "")
        await self._breaker_success(conn, task)

    # --- what the domain's rules use ------------------------------------------

    async def cited_responses(self, conn: AsyncConnection, task: Row) -> dict[str, dict]:
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

    async def cited_fact(self, conn: AsyncConnection, fact_id: str, subject: str, key: str) -> Row:
        """The current fact a result cites, which must be the one it claims to be, or Rejected."""
        try:
            fact = await crud.get_fact(conn, UUID(fact_id))
        except ValueError:
            raise Rejected(f"not a fact id: {fact_id!r}") from None
        if fact is None or fact.session_id != self.sid:
            raise Rejected(f"unknown fact {fact_id}")
        if (fact.subject, fact.key) != (subject, key) or fact.status == SUPERSEDED:
            raise Rejected(f"fact {fact_id} is not the current {subject} {key}")
        return fact

    async def current_facts(self, conn: AsyncConnection) -> dict[str, dict[str, Row]]:
        """Every current fact of the session, by key and then by subject."""
        q = select(facts.c.subject, facts.c.key, facts.c.value, facts.c.status).where(
            facts.c.session_id == self.sid, facts.c.status != SUPERSEDED
        )
        by_key: dict[str, dict[str, Row]] = {}
        for row in (await conn.execute(q)).all():
            by_key.setdefault(row.key, {})[row.subject] = row
        return by_key

    async def active_tasks(self, conn: AsyncConnection) -> list[tuple[str, dict]]:
        """(type, input) of every task that is still waiting, running or submitted."""
        q = select(tasks.c.type, tasks.c.input).where(
            tasks.c.session_id == self.sid, tasks.c.status.in_(ACTIVE)
        )
        return [(t, i) for t, i in (await conn.execute(q)).all()]

    async def mark_split(self, conn: AsyncConnection, task: Row, reason: str) -> None:
        await conn.execute(update(tasks).where(tasks.c.id == task.id).values(status="split"))
        await self.decide(conn, task, "split", reason)

    async def finish(self, conn: AsyncConnection, report: dict[str, Any], succeeded: bool) -> None:
        """Store the accepted report and end the session."""
        await conn.execute(
            update(sessions)
            .where(sessions.c.id == self.sid)
            .values(report=report, status="succeeded" if succeeded else "failed", finished_at=func.now())
        )

    # --- retries, splits and replanning -------------------------------------

    async def _retry_or_fail(self, conn: AsyncConnection, task: Row, kind: str, reason: str) -> None:
        """A retry is the same row with attempt + 1, after a backoff."""
        probe_at = await self._breaker_failure(conn, task, kind)
        if kind == "context_overflow" and await self.domain.split(self, conn, task, reason):
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
            await self.decide(conn, task, "retry", f"{kind}: {reason}")
            return
        await conn.execute(update(tasks).where(tasks.c.id == task.id).values(status="failed"))
        await self.decide(conn, task, "fail", f"{kind}: {reason}")
        await self._replan(conn, task, kind, probe_at)

    async def _replan(self, conn: AsyncConnection, task: Row, kind: str, probe_at: datetime | None) -> None:
        """What happens to a task that failed for good. Nothing downstream was
        ever created from it, because follow-ups only come from accepted results."""
        if kind == "context_overflow":
            # A new round would be just as big, so there is no point.
            return await self.decide(conn, task, "replan", "too big for one attempt; given up")
        if task.type == self.domain.report_type:
            await conn.execute(
                update(sessions)
                .where(sessions.c.id == self.sid)
                .values(status="failed", finished_at=func.now())
            )
            return await self.decide(conn, task, "replan", "report failed; session failed")
        delay = NEW_ROUND_DELAY_SECONDS
        if probe_at is not None:
            delay = max(delay, (probe_at - _now()).total_seconds())
        await self.domain.replan(self, conn, task, kind, delay, probe=probe_at is not None)

    async def _sweep_expired_leases(self, conn: AsyncConnection) -> None:
        """A worker that stopped heartbeating has lost its task, so hand it out again.

        The attempt bump is the fencing token, which means that if that worker
        was only slow, its late submit now matches 0 rows and is dropped.
        """
        q = (
            select(tasks)
            .where(
                tasks.c.session_id == self.sid,
                tasks.c.partition == self.partition,
                tasks.c.status == "leased",
                tasks.c.lease_expires_at < func.now(),
            )
            .with_for_update()
        )
        for task in (await conn.execute(q)).all():
            await self._retry_or_fail(conn, task, "lease_expired", f"worker {task.leased_by} silent")

    # --- creating tasks, and the circuit breaker --------------------------------

    async def create(
        self,
        conn: AsyncConnection,
        task_type: str,
        inp: BaseModel,
        parent: UUID | None,
        delay: float = 0.0,
        probe: bool = False,
    ) -> UUID | None:
        """Create a task, holding it back while its resource's breaker is open.

        The one task that tests whether the resource is back (the probe) may
        run when the cooldown ends, and every other task for the resource waits
        another cooldown on top, or until the probe succeeds.
        """
        resource = self.domain.resource(task_type, inp)
        if self.domain.task_types[task_type].network and resource:
            breaker = await crud.current_fact(conn, self.sid, _breaker_subject(resource), BREAKER)
            if breaker is not None and breaker.value["state"] == "open":
                until = datetime.fromisoformat(breaker.value["open_until"])
                if not probe:
                    until += timedelta(seconds=BREAKER_COOLDOWN_SECONDS)
                delay = max(delay, (until - _now()).total_seconds())
        return await create_task(
            conn, self.sid, self.domain, task_type, inp, parent, delay, self.session.partitions
        )

    async def _breaker_failure(self, conn: AsyncConnection, task: Row, kind: str) -> datetime | None:
        """Count a failed network attempt against its resource, and open the
        breaker after BREAKER_THRESHOLD in a row. Returns when the probe may run."""
        if not self.domain.task_types[task.type].network or kind not in NETWORK_ERRORS or not task.resource:
            return None
        subject = _breaker_subject(task.resource)
        cur = await crud.current_fact(conn, self.sid, subject, BREAKER)
        failures = (cur.value["failures"] if cur else 0) + 1
        if failures < BREAKER_THRESHOLD:
            value = {"state": "closed", "failures": failures, "open_until": None}
            await crud.upsert_fact(conn, self.sid, subject, BREAKER, value, source_task_id=task.id)
            return None
        until = _now() + timedelta(seconds=BREAKER_COOLDOWN_SECONDS)
        value = {"state": "open", "failures": failures, "open_until": until.isoformat()}
        await crud.upsert_fact(conn, self.sid, subject, BREAKER, value, source_task_id=task.id)
        # Hold back everything else waiting for this resource.
        held = until + timedelta(seconds=BREAKER_COOLDOWN_SECONDS)
        await conn.execute(
            update(tasks)
            .where(*self._ready_for(task.resource), tasks.c.id != task.id)
            .values(not_before=func.greatest(func.coalesce(tasks.c.not_before, held), held))
        )
        await self.decide(conn, task, "breaker_open", f"{task.resource}: {failures} failures in a row")
        return until

    async def _breaker_success(self, conn: AsyncConnection, task: Row) -> None:
        """A successful network task closes its resource's breaker and releases what it held."""
        if not self.domain.task_types[task.type].network or not task.resource:
            return
        subject = _breaker_subject(task.resource)
        cur = await crud.current_fact(conn, self.sid, subject, BREAKER)
        if cur is None or cur.value["failures"] == 0:
            return
        value = {"state": "closed", "failures": 0, "open_until": None}
        await crud.upsert_fact(conn, self.sid, subject, BREAKER, value, source_task_id=task.id)
        if cur.value["state"] == "open":
            await conn.execute(
                update(tasks)
                .where(*self._ready_for(task.resource), tasks.c.not_before > func.now())
                .values(not_before=func.now())
            )
            await self.decide(conn, task, "breaker_closed", f"{task.resource} answered again")

    def _ready_for(self, resource: str) -> list:
        network = [name for name, t in self.domain.task_types.items() if t.network]
        return [
            tasks.c.session_id == self.sid,
            tasks.c.status == "ready",
            tasks.c.type.in_(network),
            tasks.c.resource == resource,
        ]

    # --- goal, budget, stalls, end --------------------------------------------

    async def _check_goal_and_budget(self, conn: AsyncConnection) -> bool:
        """Returns True when the session is over. Only the leader checks the
        goal, the budget and stalls, and the others just watch for the end."""
        session = await crud.get_session(conn, self.sid)
        if session.status != "running":
            return True
        if not self.leader:
            return False
        q = select(tasks.c.id).where(tasks.c.session_id == self.sid, tasks.c.type == self.domain.report_type)
        if (await conn.execute(q)).first() is not None:
            return False  # waiting for the reporter

        steps = await crud.step_count(conn, self.sid)
        reason = None
        if await self.domain.goal_met(self, conn, session):
            reason, partial = self.domain.done[session.goal_kind], False
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
        await self.create(conn, self.domain.report_type, self.domain.report_input(partial), None)
        await crud.log_event(
            conn, self.sid, ACTOR, "goal_met" if not partial else "run_ending",
            {"reason": reason, "cancelled_tasks": n_cancelled, "steps": steps},
        )  # fmt: skip
        self.say(f"{reason}; cancelled {n_cancelled} leftover task(s); writing report")
        return False

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
        """Stall detection's second chance, where the domain re-opens what was put off."""
        created = await self.domain.reopen(self, conn)
        if created:
            await self.decide(conn, None, "reopen", f"{why}; re-opened {created} task(s)")
            self.say(f"{why}; re-opened {created} task(s)")
        return created

    # --- logging ------------------------------------------------------------

    async def decide(self, conn: AsyncConnection, task: Row | None, action: str, reason: str) -> None:
        """Every coordinator decision goes to `events`, with its reason."""
        payload: dict[str, Any] = {
            "action": action,
            "reason": reason,
            "task_key": task.task_key if task else None,
        }
        if task is not None and task.status == "submitted" and task.submitted_at is not None:
            # How long the result waited for the coordinator, which is what
            # grows first when one coordinator can't keep up.
            payload["latency_ms"] = round((_now() - task.submitted_at).total_seconds() * 1000)
        task_id, attempt = (task.id, task.attempt) if task else (None, None)
        await crud.log_event(conn, self.sid, ACTOR, "decision", payload, task_id, attempt)

    def say(self, message: str) -> None:
        if not self.quiet:
            print(f"[coordinator] {message}", flush=True)


def _breaker_subject(resource: str) -> str:
    return f"resource:{resource}"


def _now() -> datetime:
    return datetime.now(UTC)


async def progress_line(engine: AsyncEngine, sid: UUID) -> str:
    """A one-line summary, worked out again from the database every time (and
    never from a previous summary)."""
    async with engine.connect() as conn:
        session = await crud.get_session(conn, sid)
        steps = await crud.step_count(conn, sid)
        q = select(tasks.c.status, func.count()).where(tasks.c.session_id == sid).group_by(tasks.c.status)
        by_status = dict((await conn.execute(q)).all())
        extra = await get_domain(session.domain).progress(conn, sid)
    active = sum(by_status.get(s, 0) for s in ACTIVE)
    line = (
        f"step {steps:4d} | tasks: {by_status.get('succeeded', 0)} done, {active} active, "
        f"{by_status.get('failed', 0)} failed"
    )
    return f"{line} | {extra}" if extra else line


async def main(args: argparse.Namespace) -> int:
    engine = make_engine()
    coordinator = Coordinator(
        engine, UUID(args.session), partition=args.partition, deadline=args.deadline, quiet=args.quiet
    )
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
    parser.add_argument("--partition", type=int, default=0, help="which part of the plan (0 is the leader)")
    parser.add_argument("--deadline", type=float, help="time limit, as a Unix time")
    parser.add_argument("--quiet", action="store_true")
    try:
        sys.exit(asyncio.run(main(parser.parse_args())))
    except KeyboardInterrupt:
        sys.exit(0)
