"""Queries shared by workers and the coordinator.

The interesting ones are claim_task, heartbeat and submit_result, which
together make leases safe across processes (see 'Task queue and
leases' in docs/design.md).
"""

from datetime import timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import func, or_, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.config import LEASE_SECONDS, MAX_ATTEMPTS
from lha.core.schemas import OBSERVED, SUPERSEDED, VERIFIED
from lha.db.models import events, facts, sessions, task_deps, tasks
from lha.ids import uuid7

# A 'step' is one model call or one tool call.
STEP_KINDS = ("model_call", "tool_call")


# --- events -------------------------------------------------------------------


async def log_event(
    conn: AsyncConnection,
    session_id: UUID,
    actor: str,
    kind: str,
    payload: dict[str, Any],
    task_id: UUID | None = None,
    attempt: int | None = None,
) -> UUID:
    event_id = uuid7()
    await conn.execute(
        events.insert().values(
            id=event_id,
            session_id=session_id,
            task_id=task_id,
            attempt=attempt,
            actor=actor,
            kind=kind,
            payload=payload,
        )
    )
    return event_id


async def step_count(conn: AsyncConnection, session_id: UUID) -> int:
    q = select(func.count()).where(events.c.session_id == session_id, events.c.kind.in_(STEP_KINDS))
    return (await conn.execute(q)).scalar_one()


# --- sessions -----------------------------------------------------------------


async def cost_usd(conn: AsyncConnection, session_id: UUID) -> float:
    """What a session's real model calls have cost so far, with the tool calls
    that pay for a model of their own, such as a page fetched through the
    model provider (the fake model costs nothing)."""
    cost = events.c.payload["cost_usd"].as_float()
    q = select(func.coalesce(func.sum(cost), 0.0)).where(
        events.c.session_id == session_id, events.c.kind.in_(("model_call", "tool_call"))
    )
    return float((await conn.execute(q)).scalar_one())


async def get_session(conn: AsyncConnection, session_id: UUID) -> Row:
    return (await conn.execute(select(sessions).where(sessions.c.id == session_id))).one()


async def session_status(conn: AsyncConnection, session_id: UUID) -> str:
    q = select(sessions.c.status).where(sessions.c.id == session_id)
    return (await conn.execute(q)).scalar_one()


async def session_machines(conn: AsyncConnection, session_id: UUID) -> list[str]:
    """The machines that work on a session, which are its home first and then
    each machine that joined it (python -m lha.run --join), in that order."""
    home = (
        await conn.execute(select(sessions.c.home_machine).where(sessions.c.id == session_id))
    ).scalar_one()
    q = (
        select(events.c.payload)
        .where(events.c.session_id == session_id, events.c.kind == "machine_joined")
        .order_by(events.c.created_at)
    )
    joined = [payload["machine"] for (payload,) in (await conn.execute(q)).all()]
    return list(dict.fromkeys(m for m in (home, *joined) if m))


# --- tasks --------------------------------------------------------------------


async def create_task(
    conn: AsyncConnection,
    session_id: UUID,
    task_type: str,
    inp: dict[str, Any],
    *,
    key: str,
    role: str,
    scope: list[str],
    resource: str | None,
    partition: int,
    parent_task_id: UUID | None = None,
    delay_seconds: float = 0.0,
    waiting: bool = False,
    local: bool = False,
) -> UUID | None:
    """Create a task unless one with the same task_key already exists.

    Returns the new task's id, or None if it already existed (in which case
    nothing happens). The domain works out the key, role, scope, resource and
    partition (see lha.core.domain.create_task). A task planned ahead of its
    inputs starts out waiting, and its dependencies go in task_deps.
    """
    stmt = (
        pg_insert(tasks)
        .values(
            id=uuid7(),
            session_id=session_id,
            task_key=key,
            parent_task_id=parent_task_id,
            type=task_type,
            role=role,
            resource=resource,
            partition=partition,
            input=inp,
            scope=scope,
            status="waiting" if waiting else "ready",
            local=local,
            attempt=1,
            max_attempts=MAX_ATTEMPTS,
            not_before=func.now() + timedelta(seconds=delay_seconds) if delay_seconds else None,
        )
        .on_conflict_do_nothing(constraint="tasks_one_per_key")
        .returning(tasks.c.id)
    )
    return (await conn.execute(stmt)).scalar_one_or_none()


async def task_id_by_key(conn: AsyncConnection, session_id: UUID, key: str) -> UUID | None:
    q = select(tasks.c.id).where(tasks.c.session_id == session_id, tasks.c.task_key == key)
    return (await conn.execute(q)).scalar_one_or_none()


# --- tasks planned ahead of their inputs ------------------------------------------------


async def add_deps(
    conn: AsyncConnection, session_id: UUID, task_id: UUID, deps: list[tuple[UUID, bool]]
) -> None:
    """Record what a waiting task waits on, as (task it waits for, whether it must succeed)."""
    rows = [{"session_id": session_id, "task_id": task_id, "depends_on": d, "required": r} for d, r in deps]
    if rows:
        await conn.execute(pg_insert(task_deps).values(rows).on_conflict_do_nothing())


async def copy_deps(conn: AsyncConnection, session_id: UUID, old: UUID, new: UUID) -> None:
    """Make every task that waits on `old` also wait on `new`, its replacement."""
    await conn.execute(
        text(
            """
            INSERT INTO task_deps (session_id, task_id, depends_on, required)
            SELECT session_id, task_id, :new, required FROM task_deps WHERE depends_on = :old
            ON CONFLICT DO NOTHING
            """
        ),
        {"old": old, "new": new},
    )


async def drop_deps_on(conn: AsyncConnection, task_id: UUID) -> None:
    """Forget the edges to a task that has been replaced, so nothing waits on it any more."""
    await conn.execute(task_deps.delete().where(task_deps.c.depends_on == task_id))


# A waiting task is let down when a task it needs failed for good, was split
# or was cancelled, and nothing replaced it, because then its input will never exist.
BLOCKED_SQL = text(
    """
    SELECT DISTINCT ON (t.id) t.*, dep.task_key AS blocked_by FROM tasks t
      JOIN task_deps d ON d.task_id = t.id
      JOIN tasks dep ON dep.id = d.depends_on
     WHERE t.session_id = :sid AND t.partition = :p AND t.status = 'waiting'
       AND d.required AND dep.status IN ('failed', 'split', 'cancelled')
     ORDER BY t.id, dep.task_key
    """
)

# A waiting task may run once nothing it waits on is still active and every
# task it needs has succeeded, which also covers a task left waiting on nothing.
RELEASE_SQL = text(
    """
    UPDATE tasks t SET status = 'ready'
     WHERE t.session_id = :sid AND t.partition = :p AND t.status = 'waiting'
       AND NOT EXISTS (
         SELECT 1 FROM task_deps d JOIN tasks dep ON dep.id = d.depends_on
          WHERE d.task_id = t.id
            AND (dep.status IN ('waiting', 'ready', 'leased', 'submitted')
                 OR (d.required AND dep.status <> 'succeeded')))
    RETURNING t.id, t.task_key
    """
)


async def blocked_waiting(conn: AsyncConnection, session_id: UUID, partition: int) -> list[Row]:
    return list((await conn.execute(BLOCKED_SQL, {"sid": session_id, "p": partition})).all())


async def release_waiting(conn: AsyncConnection, session_id: UUID, partition: int) -> list[Row]:
    return list((await conn.execute(RELEASE_SQL, {"sid": session_id, "p": partition})).all())


# A local task needs files only the session's home machine has, so only a
# worker there takes it, and a session with no home lets any worker take it.
HERE = """(NOT local OR EXISTS (SELECT 1 FROM sessions s WHERE s.id = :sid
              AND (s.home_machine IS NULL OR s.home_machine = :machine)))"""

# One statement, so one transaction. It picks the oldest ready task for this
# role that this machine may take, locks it (skipping rows other workers
# have locked), and leases it.
CLAIM_SQL = text(
    f"""
    UPDATE tasks SET status = 'leased', leased_by = :worker,
           lease_expires_at = now() + make_interval(secs => :lease)
     WHERE id = (
       SELECT id FROM tasks
        WHERE session_id = :sid AND status = 'ready' AND role = :role
          AND (not_before IS NULL OR not_before <= now())   -- respect backoff
          AND {HERE}
        ORDER BY created_at, id
        LIMIT 1
        FOR UPDATE SKIP LOCKED)
    RETURNING id, task_key, parent_task_id, type, attempt, input, scope
    """
)


async def claim_task(
    conn: AsyncConnection, session_id: UUID, role: str, worker: str, machine: str | None = None
) -> Row | None:
    params = {"worker": worker, "lease": float(LEASE_SECONDS), "sid": session_id, "role": role,
              "machine": machine}  # fmt: skip
    return (await conn.execute(CLAIM_SQL, params)).one_or_none()


async def seconds_until_due(
    conn: AsyncConnection, session_id: UUID, role: str, machine: str | None = None
) -> float | None:
    """How long until the next ready task for this role that this machine may
    take can be claimed, or None if there is none. A worker with nothing to
    do sleeps at most this long, because no notification is sent when a
    backoff simply runs out."""
    q = text(
        f"""
        SELECT extract(epoch FROM min(not_before) - now())
          FROM tasks WHERE session_id = :sid AND status = 'ready' AND role = :role AND {HERE}
        """
    )
    due = (await conn.execute(q, {"sid": session_id, "role": role, "machine": machine})).scalar_one()
    return None if due is None else max(0.0, float(due))


async def heartbeat(conn: AsyncConnection, task_id: UUID, worker: str, attempt: int) -> bool:
    """Extend the lease, where False means the task was taken from us.

    The `attempt` check is the fencing token, so if the coordinator reclaimed
    the task (attempt + 1) or cancelled it, this matches 0 rows.
    """
    stmt = (
        update(tasks)
        .where(
            tasks.c.id == task_id,
            tasks.c.leased_by == worker,
            tasks.c.attempt == attempt,
            tasks.c.status == "leased",
        )
        .values(lease_expires_at=func.now() + timedelta(seconds=LEASE_SECONDS))
        .returning(tasks.c.id)
    )
    return (await conn.execute(stmt)).first() is not None


async def submit_result(
    conn: AsyncConnection, task_id: UUID, worker: str, attempt: int, result: dict[str, Any]
) -> bool:
    """Hand a result (or an error) to the coordinator, with the same fencing as heartbeat."""
    stmt = (
        update(tasks)
        .where(
            tasks.c.id == task_id,
            tasks.c.leased_by == worker,
            tasks.c.attempt == attempt,
            tasks.c.status == "leased",
        )
        .values(status="submitted", result=result, submitted_at=func.now())
        .returning(tasks.c.id)
    )
    return (await conn.execute(stmt)).first() is not None


async def earlier_reads(conn: AsyncConnection, task: Row):
    """A condition on `events` for the raw outputs a task may reuse by pointer.

    Those are the outputs of its own earlier attempts and, for a batch of a
    split discovery, the outputs of the attempt that was too big, which
    already read part of what the batch is about to read.
    """
    own = (events.c.task_id == task.id) & (events.c.attempt < task.attempt)
    if task.parent_task_id is not None:
        q = select(tasks.c.status).where(tasks.c.id == task.parent_task_id)
        if (await conn.execute(q)).scalar_one_or_none() == "split":
            return or_(own, events.c.task_id == task.parent_task_id)
    return own


# --- facts --------------------------------------------------------------------


class FactConflict(Exception):
    """A write would silently replace a verified fact, which is never allowed."""


async def current_fact(conn: AsyncConnection, session_id: UUID, subject: str, key: str) -> Row | None:
    # At most one row matches, because the partial unique index guarantees it.
    q = select(facts).where(
        facts.c.session_id == session_id,
        facts.c.subject == subject,
        facts.c.key == key,
        facts.c.status != SUPERSEDED,
    )
    return (await conn.execute(q)).one_or_none()


async def get_fact(conn: AsyncConnection, fact_id: UUID) -> Row | None:
    return (await conn.execute(select(facts).where(facts.c.id == fact_id))).one_or_none()


async def upsert_fact(
    conn: AsyncConnection,
    session_id: UUID,
    subject: str,
    key: str,
    value: Any,
    *,
    source_task_id: UUID,
    source_event_id: UUID | None = None,
    status: str = OBSERVED,
    evidence: Any = None,
) -> UUID:
    """Write a fact. The same value does nothing, and a different value supersedes the old row.

    Facts are never deleted, and a replaced row is kept with status
    'superseded'. Only the coordinator calls this.
    """
    cur = await current_fact(conn, session_id, subject, key)
    if cur is not None and cur.value == value and cur.status == status:
        return cur.id  # a retry writing the same fact changes nothing
    if cur is not None and cur.status == VERIFIED:
        raise FactConflict(f"{subject} {key}: verified value {cur.value!r}, got {value!r}")
    if cur is not None:
        # Mark the old row first, otherwise the unique index rejects the insert.
        await conn.execute(update(facts).where(facts.c.id == cur.id).values(status=SUPERSEDED))
    fact_id = uuid7()
    await conn.execute(
        facts.insert().values(
            id=fact_id,
            session_id=session_id,
            subject=subject,
            key=key,
            value=value,
            status=status,
            source_task_id=source_task_id,
            source_event_id=source_event_id,
            evidence=evidence,
        )
    )
    await log_event(
        conn,
        session_id,
        "coordinator",
        "fact_changed",
        {"fact_id": str(fact_id), "subject": subject, "key": key, "value": value,
         "status": status, "replaced": str(cur.id) if cur else None},
        task_id=source_task_id,
    )  # fmt: skip
    return fact_id


async def upsert_facts(
    conn: AsyncConnection,
    session_id: UUID,
    writes: list[tuple[str, str, Any, UUID | None]],
    *,
    source_task_id: UUID,
) -> None:
    """Write several observed facts, each as (subject, key, value, source event).

    The rules are the same as upsert_fact's, but the whole batch takes four
    statements however many facts it has, which matters for a result such as
    a page of the registry with forty entries.
    """
    latest = {(subject, key): (value, event) for subject, key, value, event in writes}
    if not latest:
        return
    q = select(facts).where(
        facts.c.session_id == session_id,
        facts.c.status != SUPERSEDED,
        tuple_(facts.c.subject, facts.c.key).in_(list(latest)),
    )
    current = {(r.subject, r.key): r for r in (await conn.execute(q)).all()}
    replaced, rows, changes = [], [], []
    for (subject, key), (value, event) in latest.items():
        cur = current.get((subject, key))
        if cur is not None and cur.value == value and cur.status == OBSERVED:
            continue
        if cur is not None and cur.status == VERIFIED:
            raise FactConflict(f"{subject} {key}: verified value {cur.value!r}, got {value!r}")
        if cur is not None:
            replaced.append(cur.id)
        fact_id = uuid7()
        rows.append(
            dict(
                id=fact_id, session_id=session_id, subject=subject, key=key, value=value,
                status=OBSERVED, source_task_id=source_task_id, source_event_id=event,
            )
        )  # fmt: skip
        change = {"fact_id": str(fact_id), "subject": subject, "key": key, "value": value,
                  "status": OBSERVED, "replaced": str(cur.id) if cur else None}  # fmt: skip
        changes.append(
            dict(
                id=uuid7(), session_id=session_id, task_id=source_task_id, actor="coordinator",
                kind="fact_changed", payload=change,
            )
        )  # fmt: skip
    if replaced:
        await conn.execute(update(facts).where(facts.c.id.in_(replaced)).values(status=SUPERSEDED))
    if rows:
        await conn.execute(facts.insert(), rows)
        await conn.execute(events.insert(), changes)


async def update_fact(
    conn: AsyncConnection, session_id: UUID, fact_id: UUID, *, status: str, **values: Any
) -> None:
    """Change a fact's status in place (for example inferred -> verified), and log it."""
    await conn.execute(update(facts).where(facts.c.id == fact_id).values(status=status, **values))
    payload = {"fact_id": str(fact_id), "status": status, **values}
    await log_event(conn, session_id, "coordinator", "fact_changed", payload)
