"""The research brief, where agents read a library of sources and find when projects launched.

It is the second domain on the same core, to show that the core knows
nothing about networks. Its world is a library of sources that cite each
other, and the goal is the launch year of each project the question asks
about, where a year only counts once two or more independent sources agree
on it and outnumber every other year, because some sources are outdated.
"""

from functools import lru_cache
from typing import Any
from uuid import UUID

from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import func, literal, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.core.domain import Domain, TaskType
from lha.core.schemas import SUPERSEDED, VERIFIED
from lha.db import models
from lha.domains.research import agents, rules
from lha.domains.research import facts as F
from lha.domains.research.app import create_app
from lha.domains.research.tasks import (
    BriefInput,
    BriefOutput,
    ReadInput,
    ReadOutput,
    ReconcileInput,
    ReconcileOutput,
    task_key,
    task_scope,
)
from lha.domains.research.tools import ResearchTools
from lha.domains.research.world import ASKED, START_SOURCES, Library, generate_library


@lru_cache(maxsize=16)
def _library(seed: int, size: int, goal_kind: str) -> Library:
    return generate_library(seed, size, ASKED[goal_kind])


class ResearchDomain(Domain):
    name = "research"
    roles = ("reader", "analyst", "writer")
    task_types = {
        "read_source": TaskType(ReadInput, ReadOutput, "reader", agents.run_read, network=True),
        "reconcile_project": TaskType(ReconcileInput, ReconcileOutput, "analyst", agents.run_reconcile),
        "write_brief": TaskType(BriefInput, BriefOutput, "writer", agents.run_brief),
    }
    report_type = "write_brief"
    default_size = 60
    goals = {
        kind: (
            "Write a brief on when these projects launched: {asked}. A year only counts "
            "when two or more sources agree on it and outnumber every other year, because "
            "some sources are outdated. You start knowing only these sources: {start}."
        )
        for kind in ("one", "all")
    }
    done = {kind: "goal met: every project asked about has a verified answer" for kind in ("one", "all")}

    def make_world(self, seed: int, size: int, goal_kind: str) -> Library:
        return _library(seed, size, goal_kind)

    def create_app(self, world: Library, fault_rate: float) -> FastAPI:
        return create_app(world, fault_rate)

    def tools(self, http, task_key, attempt, log, load) -> ResearchTools:
        return ResearchTools(http, task_key, attempt, log, load)

    def start(self, world: Library) -> list[str]:
        return list(START_SOURCES)

    def goal_args(self, world: Library) -> dict[str, str]:
        return {"asked": ", ".join(world.asked)}

    def start_tasks(self, world: Library) -> list[tuple[str, BaseModel]]:
        return [("read_source", ReadInput(source=s)) for s in START_SOURCES]

    def task_key(self, task_type: str, inp: BaseModel) -> str:
        return task_key(task_type, inp)

    def scope(self, task_type: str, inp: BaseModel) -> list[str]:
        return task_scope(inp)

    def resource(self, task_type: str, inp: BaseModel) -> str | None:
        return getattr(inp, "source", None) or getattr(inp, "project", None)

    def report_input(self, partial: bool) -> BriefInput:
        return BriefInput(partial=partial)

    def _session_library(self, coord) -> Library:
        s = coord.session
        return self.make_world(s.seed, s.n_hosts, s.goal_kind)

    async def accept(self, coord, conn, task, out) -> None:
        if task.type == "read_source":
            await rules.accept_read(coord, conn, task, out, self._session_library(coord))
        elif task.type == "reconcile_project":
            await rules.accept_reconcile(coord, conn, task, out)
        else:
            await rules.accept_brief(coord, conn, task, out)

    async def replan(self, coord, conn, task, kind, delay, probe) -> None:
        await rules.replan(coord, conn, task, kind, delay, probe)

    async def goal_met(self, coord, conn, session) -> bool:
        return await rules.goal_met(coord, conn, self._session_library(coord))

    async def reopen(self, coord, conn) -> int:
        return await rules.reopen(coord, conn, self._session_library(coord))

    async def score(self, conn: AsyncConnection, session: Row, world: Library) -> list[tuple[str, bool]]:
        answers = {a["project"]: a["year"] for a in (session.report or {}).get("findings", [])}
        return [
            (f"answered all {len(world.asked)} project(s) asked about", set(world.asked) <= set(answers)),
            ("every year is right", all(world.truth.get(p) == y for p, y in answers.items())),
        ]

    async def stats(self, conn: AsyncConnection, sid: UUID, world: Library) -> dict[str, Any]:
        f = models.facts.c
        read = await _sources_read(conn, sid)
        outdated = {(p, w) for p, (_, w) in world.outdated.items()}
        q = select(f.subject, f.value).where(
            f.session_id == sid, f.subject.like("claim:%"), f.status != SUPERSEDED
        )
        seen = {(s.removeprefix("claim:").split("@")[0], v) for s, v in (await conn.execute(q)).all()}
        return {"sources_read": f"{read}/{world.n_sources}", "outdated_claims_seen": len(outdated & seen)}

    async def progress(self, conn: AsyncConnection, sid: UUID) -> str:
        f = models.facts.c
        read = await _sources_read(conn, sid)
        q = select(func.count()).where(f.session_id == sid, f.subject.like("answer:%"), f.status == VERIFIED)
        answers = (await conn.execute(q)).scalar_one()
        return f"sources read: {read} | answers verified: {answers}"


async def _sources_read(conn: AsyncConnection, sid: UUID) -> int:
    f = models.facts.c
    q = select(func.count()).where(
        f.session_id == sid, f.key == F.READ, f.value == literal(True, JSONB), f.status != SUPERSEDED
    )
    return (await conn.execute(q)).scalar_one()


RESEARCH = ResearchDomain()
