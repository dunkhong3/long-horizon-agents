"""Queries shared by workers and the coordinator.

The interesting ones are claim_task, heartbeat and submit_result: together
they make leases safe across processes (see "Heartbeat" in docs/design.md).
"""

from datetime import timedelta
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.config import LEASE_SECONDS, MAX_ATTEMPTS
from lha.db.models import events, facts, sessions, tasks
from lha.ids import uuid7
from lha.schemas.facts import OBSERVED, SUPERSEDED, VERIFIED
from lha.schemas.tasks import ROLE_OF, task_key, task_scope

# A "step" is one model call or one tool call.
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


async def get_session(conn: AsyncConnection, session_id: UUID) -> Row:
    return (await conn.execute(select(sessions).where(sessions.c.id == session_id))).one()


async def session_status(conn: AsyncConnection, session_id: UUID) -> str:
    q = select(sessions.c.status).where(sessions.c.id == session_id)
    return (await conn.execute(q)).scalar_one()


# --- tasks --------------------------------------------------------------------


async def create_task(
    conn: AsyncConnection,
    session_id: UUID,
    task_type: str,
    inp: BaseModel,
    parent_task_id: UUID | None = None,
    delay_seconds: float = 0.0,
) -> UUID | None:
    """Create a task unless one with the same task_key already exists.

    Returns the new task's id, or None if it already existed (a no-op).
    """
    stmt = (
        pg_insert(tasks)
        .values(
            id=uuid7(),
            session_id=session_id,
            task_key=task_key(task_type, inp),
            parent_task_id=parent_task_id,
            type=task_type,
            role=ROLE_OF[task_type],
            input=inp.model_dump(),
            scope=task_scope(inp),
            status="ready",  # tasks are only created once their inputs exist
            attempt=1,
            max_attempts=MAX_ATTEMPTS,
            not_before=func.now() + timedelta(seconds=delay_seconds) if delay_seconds else None,
        )
        .on_conflict_do_nothing(constraint="tasks_one_per_key")
        .returning(tasks.c.id)
    )
    return (await conn.execute(stmt)).scalar_one_or_none()


# One statement, so one transaction: pick the oldest ready task for this role,
# lock it (skipping rows other workers have locked), and lease it.
CLAIM_SQL = text(
    """
    UPDATE tasks SET status = 'leased', leased_by = :worker,
           lease_expires_at = now() + make_interval(secs => :lease)
     WHERE id = (
       SELECT id FROM tasks
        WHERE session_id = :sid AND status = 'ready' AND role = :role
          AND (not_before IS NULL OR not_before <= now())   -- respect backoff
        ORDER BY created_at, id
        LIMIT 1
        FOR UPDATE SKIP LOCKED)
    RETURNING id, task_key, type, attempt, input, scope
    """
)


async def claim_task(conn: AsyncConnection, session_id: UUID, role: str, worker: str) -> Row | None:
    params = {"worker": worker, "lease": float(LEASE_SECONDS), "sid": session_id, "role": role}
    return (await conn.execute(CLAIM_SQL, params)).one_or_none()


async def heartbeat(conn: AsyncConnection, task_id: UUID, worker: str, attempt: int) -> bool:
    """Extend the lease. False means the task was taken from us.

    The `attempt` check is the fencing token: if the coordinator reclaimed
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
    """Hand a result (or an error) to the coordinator. Same fencing as heartbeat."""
    stmt = (
        update(tasks)
        .where(
            tasks.c.id == task_id,
            tasks.c.leased_by == worker,
            tasks.c.attempt == attempt,
            tasks.c.status == "leased",
        )
        .values(status="submitted", result=result)
        .returning(tasks.c.id)
    )
    return (await conn.execute(stmt)).first() is not None


# --- facts --------------------------------------------------------------------


class FactConflict(Exception):
    """A write would silently replace a verified fact. Never allowed."""


async def current_fact(conn: AsyncConnection, session_id: UUID, subject: str, key: str) -> Row | None:
    # At most one row matches: the partial unique index guarantees it.
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
    """Write a fact. Same value: no-op. Different value: supersede the old row.

    Facts are never deleted; a replaced row is kept with status 'superseded'.
    Only the coordinator calls this.
    """
    cur = await current_fact(conn, session_id, subject, key)
    if cur is not None and cur.value == value and cur.status == status:
        return cur.id  # idempotent: a retry writing the same fact changes nothing
    if cur is not None and cur.status == VERIFIED:
        raise FactConflict(f"{subject} {key}: verified value {cur.value!r}, got {value!r}")
    if cur is not None:
        # Mark the old row first, or the unique index rejects the insert.
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


async def update_fact(
    conn: AsyncConnection, session_id: UUID, fact_id: UUID, *, status: str, **values: Any
) -> None:
    """Change a fact's status in place (e.g. inferred -> verified), and log it."""
    await conn.execute(update(facts).where(facts.c.id == fact_id).values(status=status, **values))
    payload = {"fact_id": str(fact_id), "status": status, **values}
    await log_event(conn, session_id, "coordinator", "fact_changed", payload)
