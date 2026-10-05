"""Builds a ContextPacket for one task attempt, fresh from Postgres.

This is our answer to the context going bad as the window fills, because the
model never sees the run's history. It only sees a small packet put together
for this one task, within a token budget, made of four layers.

    1. pinned    goal + this task's spec            never cut
    2. facts     current facts in the task's scope  newest first
    3. recent    the last few events of this task   e.g. why the last try failed
    4. pointers  where earlier raw tool outputs are payloads left out

The packet is built from the top down, which means we add the layers in
priority order for as long as the next item still fits. Nothing is ever cut
in half, and whatever was left out is recorded in `omitted`.
"""

import json
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.config import (
    CONTEXT_WINDOW_TOKENS,
    MAX_POINTERS,
    OUTPUT_RESERVE_TOKENS,
    RECENT_EVENTS,
    WORK_RESERVE_TOKENS,
)
from lha.db import crud
from lha.db.models import events, facts

if TYPE_CHECKING:
    from lha.core.domain import Domain
from lha.core.schemas import SUPERSEDED, ContextPacket, EventView, FactView, Pinned, PointerView


class ContextOverflow(Exception):
    """The pinned layer alone doesn't fit, so the task is too big (a planning bug)."""


def estimate_tokens(obj: Any) -> int:
    """Roughly 4 characters per token, which is good enough for fake models."""
    if hasattr(obj, "model_dump"):
        obj = obj.model_dump()
    return len(json.dumps(obj, default=str)) // 4 + 1


def pack(
    pinned: Pinned,
    fact_items: list[FactView],
    recent_items: list[EventView],
    pointer_items: list[PointerView],
    window: int = CONTEXT_WINDOW_TOKENS,
    reserve: int = OUTPUT_RESERVE_TOKENS + WORK_RESERVE_TOKENS,
) -> ContextPacket:
    budget = window - reserve  # leave room for the attempt's work and the model's answer
    used = estimate_tokens(pinned)
    if used > budget:
        # Truncating pinned would silently change what the task is.
        raise ContextOverflow(f"pinned needs {used} tokens, budget is {budget}")

    kept: dict[str, list] = {"facts": [], "recent": [], "pointers": []}
    omitted: dict[str, int] = {}
    full = False
    for layer, items in (("facts", fact_items), ("recent", recent_items), ("pointers", pointer_items)):
        for item in items:
            cost = estimate_tokens(item)
            if not full and used + cost <= budget:
                kept[layer].append(item)
                used += cost
            else:
                # Once something doesn't fit, everything after it has a lower
                # priority, so it is left out too.
                full = True
                omitted[layer] = omitted.get(layer, 0) + 1

    return ContextPacket(
        pinned=pinned,
        facts=kept["facts"],
        recent=kept["recent"],
        pointers=kept["pointers"],
        omitted=omitted,
        tokens=used,
    )


async def build_packet(conn: AsyncConnection, domain: "Domain", session: Row, task: Row) -> ContextPacket:
    pinned = Pinned(
        goal=session.goal,
        task_type=task.type,
        task_key=task.task_key,
        attempt=task.attempt,
        input=task.input,
    )
    return pack(
        pinned,
        await _select_facts(conn, domain, session.id, task.scope),
        await _recent_events(conn, session.id, task.id),
        await _pointers(conn, domain, session.id, task),
    )


async def _select_facts(
    conn: AsyncConnection, domain: "Domain", session_id: UUID, scope: list[str]
) -> list[FactView]:
    """Current facts in the task's scope, picked with a plain SQL filter and no embeddings.

    A scope entry is an exact subject, a pattern where '*' stands for any
    text (such as '*@host-4', everything on a host), or 'key:<key>=<status>'
    for every fact with that key and status (such as every verified drift),
    which also brings in the facts the domain says belong with each of them.
    """
    conditions, by_key = [], []
    for entry in scope:
        if entry.startswith("key:"):
            key, status = entry.removeprefix("key:").split("=")
            by_key.append((key, status))
            conditions.append((facts.c.key == key) & (facts.c.status == status))
        elif "*" in entry:
            pattern = entry.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_").replace("*", "%")
            conditions.append(facts.c.subject.like(pattern))
        else:
            conditions.append(facts.c.subject == entry)
    q = (
        select(facts)
        .where(facts.c.session_id == session_id, facts.c.status != SUPERSEDED, or_(*conditions))
        .order_by(facts.c.created_at.desc())
    )
    rows = list((await conn.execute(q)).all())

    if by_key:
        related = [pair for row in rows if (row.key, row.status) in by_key for pair in domain.related(row)]
        for subject, key in related:
            q = select(facts).where(
                facts.c.session_id == session_id,
                facts.c.subject == subject,
                facts.c.key == key,
                facts.c.status != SUPERSEDED,
            )
            rows += list((await conn.execute(q)).all())

    return [
        FactView(id=str(r.id), subject=r.subject, key=r.key, value=r.value, status=r.status) for r in rows
    ]


async def _recent_events(conn: AsyncConnection, session_id: UUID, task_id: UUID) -> list[EventView]:
    """What happened to this task before, meaning the coordinator's decisions about it."""
    q = (
        select(events)
        .where(
            events.c.session_id == session_id,
            events.c.task_id == task_id,
            events.c.kind.in_(("decision", "lease_lost")),
        )
        .order_by(events.c.created_at.desc())
        .limit(RECENT_EVENTS)
    )
    out = []
    for e in (await conn.execute(q)).all():
        p = e.payload
        summary = f"{p.get('action', e.kind)}: {p.get('reason', '')}".strip(": ")
        out.append(EventView(kind=e.kind, attempt=e.attempt, summary=summary))
    return out


async def _pointers(
    conn: AsyncConnection, domain: "Domain", session_id: UUID, task: Row
) -> list[PointerView]:
    """Successful raw tool outputs this task may reuse, newest first.

    Those come from its own earlier attempts, or for a batch of a split task
    from the attempt that was too big (see crud.earlier_reads). Each
    pointer says which tool and path the output came from but leaves the
    output out, and an agent can fetch one with the fetch_pointer tool instead
    of calling the network again. A fetched copy is not pointed at again,
    because its original already is.
    """
    q = (
        select(events.c.id, events.c.payload)
        .where(
            events.c.session_id == session_id,
            events.c.kind == "tool_call",
            await crud.earlier_reads(conn, task),
        )
        .order_by(events.c.created_at.desc())
    )
    pointers: dict[str, PointerView] = {}
    for event_id, p in (await conn.execute(q)).all():
        if (
            p.get("ok")
            and p["tool"] != "fetch_pointer"
            and p["path"] not in pointers
            and domain.pointer_wanted(task, p["path"])
        ):
            pointers[p["path"]] = PointerView(id=str(event_id), tool=p["tool"], path=p["path"])
    return list(pointers.values())[:MAX_POINTERS]
