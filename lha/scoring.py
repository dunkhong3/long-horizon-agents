"""The scorer: did the run find the planted drift, with verified evidence?

It regenerates the world from the session's seed (nothing about the answer
is stored in the database) and compares it with the accepted report.
"""

from collections import Counter
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from sqlalchemy import func, literal, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncEngine

from lha.db import crud
from lha.db.models import events, facts, tasks
from lha.schemas import facts as F
from lha.world.model import generate_world


@dataclass
class Score:
    passed: bool
    checks: list[tuple[str, bool]]
    stats: dict[str, Any] = field(default_factory=dict)
    report: dict[str, Any] | None = None


async def score_session(engine: AsyncEngine, session_id: UUID) -> Score:
    async with engine.connect() as conn:
        session = await crud.get_session(conn, session_id)
        world = generate_world(session.seed, session.n_hosts)
        drift = world.drift
        report = session.report or {}

        drift_fact = None
        if report.get("drift_fact_id"):
            drift_fact = await crud.get_fact(conn, UUID(report["drift_fact_id"]))

        checks = [
            ("session succeeded", session.status == "succeeded"),
            (f"found the drifted service ({drift.name})", report.get("service") == drift.name),
            (f"on the right host ({drift.host})", report.get("host") == drift.host),
            (
                f"right counts (expected {drift.expected}, actual {drift.actual})",
                (report.get("expected"), report.get("actual")) == (drift.expected, drift.actual),
            ),
            ("cited drift fact is verified", drift_fact is not None and drift_fact.status == F.VERIFIED),
        ]
        stats = await _stats(conn, session_id, world)
    return Score(all(ok for _, ok in checks), checks, stats, session.report)


async def _stats(conn, sid: UUID, world) -> dict[str, Any]:
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
    task_status = dict(
        (
            await conn.execute(
                select(tasks.c.status, func.count()).where(tasks.c.session_id == sid).group_by(tasks.c.status)
            )
        ).all()
    )
    hosts_found = await count(
        facts, facts.c.key == F.EXISTS, facts.c.value == literal(True, JSONB),
        facts.c.status != F.SUPERSEDED,
    )  # fmt: skip
    refuted = await count(facts, facts.c.key == F.DRIFT, facts.c.status == F.REFUTED)
    return {
        "steps": tool_calls + model_calls,
        "model_calls": model_calls,
        "tool_calls": tool_calls,
        "faults_injected": faults,
        "model_errors_injected": model_errors,
        "outputs_rejected": rejected,
        "retries": decisions.get("retry", 0),
        "replans": decisions.get("replan", 0),
        "leases_lost": lease_lost,
        "tasks": task_status,
        "hosts_checked": f"{hosts_found}/{world.n_hosts}",
        "decoys_refuted": f"{refuted}/{len(world.decoys)}",
    }
