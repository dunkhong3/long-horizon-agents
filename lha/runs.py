"""What the runs are doing and what they found, read from Postgres.

The lha command and the MCP server both answer from here, and like the
supervisor's progress line and the dashboard, every answer is worked out
again from the tables when it is asked for. A run started with --detach
also leaves its process id in runs/<session>/pid, so a run whose session is
still open can be told apart from one whose supervisor is gone, for example
after the machine restarted, which --resume continues.
"""

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import String, cast, select
from sqlalchemy.ext.asyncio import AsyncEngine

from lha.core.agent import FAKE
from lha.core.coordinator import progress_line
from lha.db import crud
from lha.db.models import sessions
from lha.run import reap_detached

RUNS_DIR = Path("runs")


async def find_session(engine: AsyncEngine, ref: str | None = None) -> UUID | None:
    """The session a reference names, which is a whole id or the start of one,
    or the latest session for no reference at all. An unknown or ambiguous
    reference names none."""
    q = select(sessions.c.id).order_by(sessions.c.created_at.desc())
    if ref:
        q = q.where(cast(sessions.c.id, String).startswith(ref.strip().lower())).limit(2)
    else:
        q = q.limit(1)
    async with engine.connect() as conn:
        found = (await conn.execute(q)).scalars().all()
    return found[0] if len(found) == 1 else None


async def list_runs(engine: AsyncEngine, limit: int = 10) -> list[dict[str, Any]]:
    """The latest sessions, newest first."""
    s = sessions.c
    q = (
        select(s.id, s.domain, s.world, s.model, s.status, s.goal, s.created_at, s.finished_at)
        .order_by(s.created_at.desc())
        .limit(limit)
    )
    async with engine.connect() as conn:
        rows = (await conn.execute(q)).all()
    return [
        {
            "session_id": str(r.id),
            "domain": r.domain,
            "world": r.world,
            "model": r.model,
            "status": r.status,
            "goal": r.goal,
            "started_at": r.created_at.isoformat(),
            "finished_at": r.finished_at.isoformat() if r.finished_at else None,
        }
        for r in rows
    ]


async def run_status(engine: AsyncEngine, sid: UUID) -> dict[str, Any]:
    """How a run is doing, with its progress line and, for a real model, what it has cost."""
    async with engine.connect() as conn:
        session = await crud.get_session(conn, sid)
        steps = await crud.step_count(conn, sid)
        cost = await crud.cost_usd(conn, sid) if session.model != FAKE else None
    end = session.finished_at or datetime.now(UTC)
    return {
        "session_id": str(sid),
        "domain": session.domain,
        "world": session.world,
        "model": session.model,
        "goal": session.goal,
        "status": session.status,
        "supervisor": supervisor_state(sid) if session.status == "running" else None,
        "steps": steps,
        "cost_usd": round(cost, 4) if cost is not None else None,
        "budget_usd": session.budget_usd,
        "elapsed_seconds": round((end - session.created_at).total_seconds()),
        "progress": await progress_line(engine, sid),
        "report_ready": session.report is not None,
    }


async def run_report(engine: AsyncEngine, sid: UUID) -> str | None:
    """A finished run's report in markdown, or None while there is none yet."""
    async with engine.connect() as conn:
        session = await crud.get_session(conn, sid)
    if session.report is None:
        return None
    return session.report.get("summary") or "The run ended without a written summary.\n"


def supervisor_state(sid: UUID) -> str:
    """'alive' or 'gone' for a run started with --detach, from the process id
    it left, and 'unknown' for a run started in a terminal, which left none."""
    reap_detached()
    try:
        pid = int((RUNS_DIR / str(sid) / "pid").read_text())
    except (OSError, ValueError):
        return "unknown"
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return "gone"
    except PermissionError:
        return "alive"  # it exists, under another user
    return "alive"
