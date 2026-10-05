"""The scorer, which works out whether a run reached its goal, and counts what happened on the way.

It builds the world again from the session's seed (nothing about the answer
is stored in the database), and the domain compares it with the accepted
report. The counts are the same for every domain, and the domain adds a few
of its own.
"""

from collections import Counter
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine

from lha.core.domain import Domain, get_domain
from lha.core.schemas import VERIFIED
from lha.db import crud
from lha.db.models import events, tasks


@dataclass
class Score:
    passed: bool
    checks: list[tuple[str, bool]]
    stats: dict[str, Any] = field(default_factory=dict)
    report: dict[str, Any] | None = None


async def score_session(engine: AsyncEngine, session_id: UUID) -> Score:
    async with engine.connect() as conn:
        session = await crud.get_session(conn, session_id)
        domain = get_domain(session.domain)
        world = domain.make_world(session.seed, session.n_hosts, session.goal_kind)
        findings = (session.report or {}).get("findings", [])
        cited = [await crud.get_fact(conn, UUID(f["fact_id"])) for f in findings]
        verified = bool(cited) and all(f is not None and f.status == VERIFIED for f in cited)
        checks = [
            ("session succeeded", session.status == "succeeded"),
            *await domain.score(conn, session, world),
            ("every cited fact is verified", verified),
        ]
        stats = {**await _stats(conn, domain, session_id), **await domain.stats(conn, session_id, world)}
    return Score(all(ok for _, ok in checks), checks, stats, session.report)


async def _stats(conn, domain: Domain, sid: UUID) -> dict[str, Any]:
    async def count(table, *where) -> int:
        q = select(func.count()).select_from(table).where(table.c.session_id == sid, *where)
        return (await conn.execute(q)).scalar_one()

    e = events.c
    tool_calls = await count(events, e.kind == "tool_call")
    model_calls = await count(events, e.kind == "model_call")
    faults = await count(
        events, e.kind == "tool_call", e.payload["ok"].as_boolean().is_(False),
        e.payload["error"].as_string() != "not_found",
    )  # fmt: skip
    model_errors = await count(events, e.kind == "model_call", e.payload["corrupted"].as_string().isnot(None))
    actions = await conn.execute(
        select(e.payload["action"].as_string()).where(e.session_id == sid, e.kind == "decision")
    )
    decisions = Counter(actions.scalars())
    rejected = await count(events, e.kind == "decision", e.payload["reason"].as_string().like("rejected%"))
    lease_lost = await count(events, e.kind == "lease_lost")
    crashes = await count(events, e.kind == "crash_injected")
    pointer_fetches = await count(
        events, e.kind == "tool_call", e.payload["tool"].as_string() == "fetch_pointer"
    )
    task_status = dict(
        (
            await conn.execute(
                select(tasks.c.status, func.count()).where(tasks.c.session_id == sid).group_by(tasks.c.status)
            )
        ).all()
    )
    prompts = await conn.execute(
        select(e.payload["prompt_tokens"].as_integer()).where(e.session_id == sid, e.kind == "model_call")
    )
    reads = await conn.execute(
        select(e.payload["path"].as_string(), tasks.c.type)
        .join(tasks, tasks.c.id == e.task_id)
        .where(
            e.session_id == sid,
            e.kind == "tool_call",
            e.payload["ok"].as_boolean().is_(True),
            e.payload["tool"].as_string() != "fetch_pointer",  # a copy from Postgres, not a read
        )
        .order_by(e.created_at)
    )
    return {
        "steps": tool_calls + model_calls,
        "model_calls": model_calls,
        "tool_calls": tool_calls,
        "faults_injected": faults,
        "model_errors_injected": model_errors,
        "outputs_rejected": rejected,
        "retries": decisions.get("retry", 0),
        "replans": decisions.get("replan", 0),
        "splits": decisions.get("split", 0),
        "breaker_opened": decisions.get("breaker_open", 0),
        "reopened": decisions.get("reopen", 0),
        "crashes_injected": crashes,
        "pointer_fetches": pointer_fetches,
        "leases_lost": lease_lost,
        "tasks": task_status,
        **prompt_stats([t for t in prompts.scalars() if t is not None]),
        "repeated_reads": repeated_reads((path, domain.task_types[t].rereads) for path, t in reads),
    }


def prompt_stats(tokens: list[int]) -> dict[str, int]:
    """How big the prompts were, over every model call of a run."""
    if not tokens:
        return {"prompt_tokens_mean": 0, "prompt_tokens_max": 0, "prompt_tokens_total": 0}
    return {
        "prompt_tokens_mean": round(sum(tokens) / len(tokens)),
        "prompt_tokens_max": max(tokens),
        "prompt_tokens_total": sum(tokens),
    }


def repeated_reads(reads) -> int:
    """Successful reads of something that had already been read successfully.

    `reads` is (path, deliberate) pairs in the order they happened, where a
    deliberate read is a re-check of a suspected drift, which is meant to
    repeat a read and so doesn't count. Everything else that repeats is
    wasted work, such as a retry starting over or a host visited twice.
    """
    seen: set[str] = set()
    repeated = 0
    for path, deliberate in reads:
        if path in seen and not deliberate:
            repeated += 1
        seen.add(path)
    return repeated
