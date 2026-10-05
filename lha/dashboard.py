"""A live dashboard, which is a small read-only web page that watches runs as they happen.

    python -m lha.dashboard --port 8000      # then open http://localhost:8000

It is its own process and only reads Postgres, so it can be started before,
during or after a run, and it shows any session of any domain, because
everything it shows is worked out again from the tables on every request,
in the same way as the supervisor's progress line. The page asks for a
fresh view once a second.
"""

import argparse
from pathlib import Path
from typing import Any
from uuid import UUID

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy import Float, cast, func, select
from sqlalchemy.ext.asyncio import AsyncEngine

from lha.core.coordinator import progress_line
from lha.core.schemas import SUPERSEDED
from lha.db import crud, make_engine
from lha.db.models import events, facts, sessions, tasks

RECENT_DECISIONS = 25
LISTED_SESSIONS = 20


def create_app(engine: AsyncEngine) -> FastAPI:
    app = FastAPI(title="lha dashboard")

    @app.get("/", response_class=HTMLResponse)
    async def page() -> str:
        return PAGE

    @app.get("/api/sessions")
    async def list_sessions() -> list[dict[str, Any]]:
        s = sessions.c
        q = (
            select(s.id, s.domain, s.seed, s.goal_kind, s.status, s.created_at)
            .order_by(s.created_at.desc())
            .limit(LISTED_SESSIONS)
        )
        async with engine.connect() as conn:
            rows = (await conn.execute(q)).all()
        return [{**r._asdict(), "id": str(r.id)} for r in rows]

    @app.get("/api/sessions/{sid}")
    async def session_view(sid: UUID) -> dict[str, Any]:
        async with engine.connect() as conn:
            session = (await conn.execute(select(sessions).where(sessions.c.id == sid))).one_or_none()
            if session is None:
                raise HTTPException(404, "no such session")
            view = {
                "session": {
                    "id": str(session.id),
                    "domain": session.domain,
                    "seed": session.seed,
                    "size": session.n_hosts,
                    "goal_kind": session.goal_kind,
                    "goal": session.goal,
                    "fault_rate": session.fault_rate,
                    "partitions": session.partitions,
                    "step_budget": session.step_budget,
                    "status": session.status,
                    "report": session.report,
                },
                "steps": await crud.step_count(conn, sid),
                "tasks": await _tasks_by_type(conn, sid),
                "facts": await _facts_by_status(conn, sid),
                "breakers": await _open_breakers(conn, sid),
                "timeline": await _steps_per_second(conn, sid, session.created_at),
                "decisions": await _recent_decisions(conn, sid),
            }
        view["progress"] = await progress_line(engine, sid)
        return view

    return app


async def _tasks_by_type(conn, sid: UUID) -> dict[str, dict[str, int]]:
    q = (
        select(tasks.c.type, tasks.c.status, func.count())
        .where(tasks.c.session_id == sid)
        .group_by(tasks.c.type, tasks.c.status)
    )
    out: dict[str, dict[str, int]] = {}
    for task_type, status, n in (await conn.execute(q)).all():
        out.setdefault(task_type, {})[status] = n
    return out


async def _facts_by_status(conn, sid: UUID) -> dict[str, int]:
    q = select(facts.c.status, func.count()).where(facts.c.session_id == sid).group_by(facts.c.status)
    return dict((await conn.execute(q)).all())


async def _open_breakers(conn, sid: UUID) -> list[dict[str, Any]]:
    """Resources whose circuit breaker is not closed right now."""
    f = facts.c
    q = select(f.subject, f.value).where(
        f.session_id == sid, f.key == "breaker", f.status != SUPERSEDED, f.value["state"].astext != "closed"
    )
    return [{"resource": s.removeprefix("resource:"), **v} for s, v in (await conn.execute(q)).all()]


async def _steps_per_second(conn, sid: UUID, started) -> list[list[float]]:
    """Cumulative steps against seconds since the session started, for the chart."""
    second = func.floor(cast(func.extract("epoch", events.c.created_at - started), Float))
    q = (
        select(second, func.count())
        .where(events.c.session_id == sid, events.c.kind.in_(crud.STEP_KINDS))
        .group_by(second)
        .order_by(second)
    )
    total, out = 0, []
    for sec, n in (await conn.execute(q)).all():
        total += n
        out.append([float(sec), total])
    return out


async def _recent_decisions(conn, sid: UUID) -> list[dict[str, Any]]:
    q = (
        select(events.c.created_at, events.c.payload)
        .where(events.c.session_id == sid, events.c.kind == "decision")
        .order_by(events.c.created_at.desc())
        .limit(RECENT_DECISIONS)
    )
    return [{"at": at.isoformat(), **p} for at, p in (await conn.execute(q)).all()]


PAGE = (Path(__file__).parent / "dashboard.html").read_text()  # the page, which asks for /api/...


def main() -> None:
    p = argparse.ArgumentParser(description="a live, read-only view of the runs in Postgres")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    args = p.parse_args()
    uvicorn.run(create_app(make_engine()), host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
