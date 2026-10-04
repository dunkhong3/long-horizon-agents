"""Builds a ContextPacket for one task attempt, fresh from Postgres.

This is our answer to the context going bad as the window fills, because the
model never sees the run's history. It only sees a small packet put together
for this one task, within a token budget, made of four layers.

    1. pinned    goal + this task's spec            never cut
    2. facts     current facts in the task's scope  newest first
    3. recent    the last few events of this task   e.g. why the last try failed
    4. pointers  ids of earlier raw tool outputs    payloads left out

The packet is built from the top down, which means we add the layers in
priority order for as long as the next item still fits. Nothing is ever cut
in half, and whatever was left out is recorded in `omitted`.
"""

import json
from typing import Any
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.config import CONTEXT_WINDOW_TOKENS, OUTPUT_RESERVE_TOKENS, RECENT_EVENTS
from lha.db.models import events, facts
from lha.schemas.context import ContextPacket, EventView, FactView, Pinned
from lha.schemas.facts import DRIFT, REPLICAS, SUPERSEDED, VERIFIED, registry_subject


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
    pointer_items: list[str],
    window: int = CONTEXT_WINDOW_TOKENS,
    reserve: int = OUTPUT_RESERVE_TOKENS,
) -> ContextPacket:
    budget = window - reserve  # leave room for the model's answer
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


async def build_packet(conn: AsyncConnection, session: Row, task: Row) -> ContextPacket:
    pinned = Pinned(
        goal=session.goal,
        task_type=task.type,
        task_key=task.task_key,
        attempt=task.attempt,
        input=task.input,
    )
    return pack(
        pinned,
        await _select_facts(conn, session.id, task.scope),
        await _recent_events(conn, session.id, task.id),
        await _pointers(conn, session.id, task.id, task.attempt),
    )


async def _select_facts(conn: AsyncConnection, session_id: UUID, scope: list[str]) -> list[FactView]:
    """Current facts in the task's scope, picked with a plain SQL filter and no embeddings."""
    conditions = []
    for entry in scope:
        if entry == "verified_drifts":
            conditions.append((facts.c.key == DRIFT) & (facts.c.status == VERIFIED))
        elif entry.startswith("*@"):
            conditions.append(facts.c.subject.like(f"%@{entry[2:]}"))
        else:
            conditions.append(facts.c.subject == entry)
    q = (
        select(facts)
        .where(facts.c.session_id == session_id, facts.c.status != SUPERSEDED, or_(*conditions))
        .order_by(facts.c.created_at.desc())
    )
    rows = list((await conn.execute(q)).all())

    if "verified_drifts" in scope:
        # The reporter also needs the registry entry and latest read behind
        # each verified drift.
        extra = []
        for row in rows:
            service = row.subject.removeprefix("service:").split("@")[0]
            extra += [(registry_subject(service), "replicas"), (row.subject, REPLICAS)]
        for subject, key in extra:
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


async def _pointers(conn: AsyncConnection, session_id: UUID, task_id: UUID, attempt: int) -> list[str]:
    """Ids of raw tool outputs from earlier attempts of this task."""
    q = (
        select(events.c.id)
        .where(
            events.c.session_id == session_id,
            events.c.task_id == task_id,
            events.c.kind == "tool_call",
            events.c.attempt < attempt,
        )
        .order_by(events.c.created_at.desc())
        .limit(20)
    )
    return [str(i) for i in (await conn.execute(q)).scalars()]
