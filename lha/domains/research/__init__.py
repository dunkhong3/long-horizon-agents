"""Verified research, where agents plan, search and read pages to answer a question.

A question is answered as a table of cells, where each cell is one attribute
of one subject, such as the license of Atlas. A cell only gets an answer
once two or more independent sources agree on its value and outnumber every
other value, and the coordinator checks every quote against the page it was
read from, so a report states nothing that no page says.

It runs on a mock web generated from a seed, with the fake model or a real
one, and scored against the answers the mock web was built from. With
--question it runs on the real web instead, with a real model and its
provider's web search, where there is nothing to score against, so what
counts is that every answer it gives is verified.
"""

from functools import lru_cache
from typing import Any
from urllib.parse import parse_qs, urlsplit
from uuid import UUID

from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.core.domain import Domain, TaskType
from lha.core.schemas import SUPERSEDED, VERIFIED
from lha.db import models
from lha.domains.research import agents, rules
from lha.domains.research import facts as F
from lha.domains.research.app import create_app
from lha.domains.research.tasks import (
    PlanInput,
    PlanOutput,
    ReadInput,
    ReadOutput,
    ReconcileInput,
    ReconcileOutput,
    ReportInput,
    ReportOutput,
    SearchInput,
    SearchOutput,
    task_key,
    task_scope,
)
from lha.domains.research.tools import ResearchTools
from lha.domains.research.web import site_of
from lha.domains.research.world import ASKED, MockWeb, cells_of, generate_web


@lru_cache(maxsize=16)
def _web(seed: int, size: int, goal_kind: str) -> MockWeb:
    return generate_web(seed, size, ASKED[goal_kind])


class ResearchDomain(Domain):
    name = "research"
    roles = ("reader", "searcher", "analyst", "writer")
    task_types = {
        "plan_research": TaskType(
            PlanInput, PlanOutput, "searcher", agents.run_plan, agents.PLAN_INSTRUCTIONS
        ),
        "search_web": TaskType(
            SearchInput, SearchOutput, "searcher", agents.run_search, agents.SEARCH_INSTRUCTIONS, network=True
        ),
        "read_page": TaskType(
            ReadInput, ReadOutput, "reader", agents.run_read, agents.READ_INSTRUCTIONS, network=True
        ),
        "reconcile_cell": TaskType(
            ReconcileInput, ReconcileOutput, "analyst", agents.run_reconcile, agents.RECONCILE_INSTRUCTIONS
        ),
        "write_report": TaskType(
            ReportInput, ReportOutput, "writer", agents.run_report, agents.REPORT_INSTRUCTIONS
        ),
    }
    report_type = "write_report"
    default_size = 60  # pages in the mock web, or the most pages to read on the real web
    real_web = True
    goals = {"one": "{question}", "all": "{question}"}
    done = {kind: "goal met: every value asked about has a verified answer" for kind in ("one", "all")}

    def make_world(self, seed: int, size: int, goal_kind: str) -> MockWeb:
        return _web(seed, size, goal_kind)

    def create_app(self, world: MockWeb, fault_rate: float) -> FastAPI:
        return create_app(world, fault_rate)

    def tools(self, http, task_key, attempt, log, load, session) -> ResearchTools:
        return ResearchTools(http, task_key, attempt, log, load, world=session.world)

    def start(self, world: MockWeb | None) -> list[str]:
        return []  # nothing is known at the start, and the planner's searches find the pages

    def goal_args(self, world: MockWeb | None) -> dict[str, str]:
        return {"question": world.question if world is not None else ""}

    def start_tasks(self, world: MockWeb | None, goal: str) -> list[tuple[str, BaseModel]]:
        return [("plan_research", PlanInput(question=goal))]

    def task_key(self, task_type: str, inp: BaseModel) -> str:
        return task_key(task_type, inp)

    def scope(self, task_type: str, inp: BaseModel) -> list[str]:
        return task_scope(inp)

    def resource(self, task_type: str, inp: BaseModel) -> str | None:
        if isinstance(inp, ReadInput):
            return site_of(inp.url)  # one breaker and one coordinator per site
        if isinstance(inp, SearchInput):
            return "search"
        return None

    def report_input(self, partial: bool) -> ReportInput:
        return ReportInput(partial=partial)

    def gone(self, task: Row, path: str) -> bool:
        """A 404 counts only for the page the task reads, as a whole."""
        url = task.input.get("url")
        if url is None:
            return True
        if path.startswith("/page?"):
            query = parse_qs(urlsplit(path).query)
            return query.get("url") == [url] and query.get("page") == ["0"]
        return path == f"{url}#page=0"

    async def accept(self, coord, conn, task, out) -> None:
        accept = {
            "plan_research": rules.accept_plan,
            "search_web": rules.accept_search,
            "read_page": rules.accept_read,
            "reconcile_cell": rules.accept_reconcile,
            "write_report": rules.accept_report,
        }
        await accept[task.type](coord, conn, task, out)

    async def replan(self, coord, conn, task, kind, delay, probe) -> None:
        await rules.replan(coord, conn, task, kind, delay, probe)

    async def split(self, coord, conn, task, reason) -> bool:
        return await rules.split(coord, conn, task, reason)

    async def goal_met(self, coord, conn, session) -> bool:
        return await rules.goal_met(coord, conn)

    async def reopen(self, coord, conn) -> int:
        return await rules.reopen(coord, conn)

    def pointer_wanted(self, task: Row, path: str) -> bool:
        """A page read may reuse earlier reads of its own page, or of the one page it reads."""
        url, page = task.input.get("url"), task.input.get("page")
        if url is None or task.type != "read_page":
            return False
        if path.startswith("/page?"):
            query = parse_qs(urlsplit(path).query)
            return query.get("url") == [url] and (page is None or query.get("page") == [str(page)])
        return path.startswith(f"{url}#page=") and (page is None or path == f"{url}#page={page}")

    async def score(
        self, conn: AsyncConnection, session: Row, world: MockWeb | None
    ) -> list[tuple[str, bool]]:
        if world is None:
            return []  # the real web has no answer key, so only the checks every run gets apply
        answers = {
            (a["subject"], a["attribute"]): a["value"] for a in (session.report or {}).get("findings", [])
        }
        asked = cells_of(world.asked)
        return [
            (f"answered all {len(asked)} value(s) asked about", set(asked) <= set(answers)),
            (
                "every value is right",
                all(F.same_value(world.truth[c], v) for c, v in answers.items() if c in world.truth),
            ),
        ]

    async def stats(self, conn: AsyncConnection, sid: UUID, world: MockWeb | None) -> dict[str, Any]:
        f = models.facts.c
        q = select(f.value).where(f.session_id == sid, f.key == F.READ, f.status != SUPERSEDED)
        pages = [v for v in (await conn.execute(q)).scalars() if isinstance(v, dict)]
        copies = sum(1 for v in pages if v["source"] != v["site"])
        out: dict[str, Any] = {"pages_read": len(pages), "copies_caught": copies}
        if world is not None:
            q = select(f.subject, f.value).where(
                f.session_id == sid, f.key == F.VALUE, f.subject.like("claim:%")
            )
            outdated = {(cell, value) for cell, (_, value) in world.outdated.items()}
            seen = set()
            for subject, value in (await conn.execute(q)).all():
                cell = F.split_cell(subject.removeprefix("claim:").split("@", 1)[0])
                if (cell, value["value"]) in outdated:
                    seen.add(cell)
            out["outdated_values_seen"] = len(seen)
        return out

    async def progress(self, conn: AsyncConnection, sid: UUID) -> str:
        f = models.facts.c
        read = select(func.count()).where(f.session_id == sid, f.key == F.READ, f.status != SUPERSEDED)
        answers = select(func.count()).where(
            f.session_id == sid, f.subject.like("answer:%"), f.status == VERIFIED
        )
        return (
            f"pages read: {(await conn.execute(read)).scalar_one()} | "
            f"answers verified: {(await conn.execute(answers)).scalar_one()}"
        )


RESEARCH = ResearchDomain()
