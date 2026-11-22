"""Code work on a git repository, where agents make a check pass one failing item at a time.

The check is a command, such as a test run, with {item} where one failing
item goes, such as one test file. A checker runs the whole check on the
session's own branch and names what fails, a coder fixes each item with
edits it has seen pass in a scratch copy, and the coordinator commits a fix
only once trusted code has run its item's check with exactly those edits,
so every commit on the branch is one that was checked.

It runs on a mock repository generated from the seed, with the fake model
or a real one, and scored by running the check on the finished branch.
With --repo it runs on a real repository instead, with a real model, where
the check is the one --check gives and the result is a branch to review.
"""

import shlex
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.core.domain import Domain, TaskType
from lha.core.schemas import VERIFIED
from lha.db import models
from lha.domains.code import agents, rules
from lha.domains.code import facts as F
from lha.domains.code import workspace as ws
from lha.domains.code.tasks import (
    CheckInput,
    CheckOutput,
    FixInput,
    FixOutput,
    ReportInput,
    ReportOutput,
    task_key,
    task_scope,
)
from lha.domains.code.tools import CodeTools
from lha.domains.code.world import CHECK, DEFAULT_MODULES, PROTECTED, MockRepo, generate_repo

# What a fix may not change in a real repository unless --allow-test-edits
# says so, which is the usual places tests live.
TEST_FILES = (
    "tests/*", "test/*", "*/tests/*", "*/test/*", "test_*.py", "*/test_*.py", "*_test.py", "*_test.go",
    "*.test.*", "*.spec.*",
)  # fmt: skip
GOAL = "Make the check `{check}` pass in {repo}, on the branch {branch}, one failing item at a time{guard}."


@lru_cache(maxsize=16)
def _repo(seed: int, size: int, goal_kind: str) -> MockRepo:
    return generate_repo(seed, size, goal_kind)


class CodeDomain(Domain):
    name = "code"
    roles = ("coder", "checker", "writer")
    task_types = {
        "check_all": TaskType(
            CheckInput,
            CheckOutput,
            "checker",
            agents.run_check_all,
            agents.CHECK_INSTRUCTIONS,
            network=True,
            rereads=True,
        ),
        "fix_item": TaskType(
            FixInput, FixOutput, "coder", agents.run_fix, agents.FIX_INSTRUCTIONS, network=True, rereads=True
        ),
        "write_report": TaskType(
            ReportInput, ReportOutput, "writer", agents.run_report, agents.REPORT_INSTRUCTIONS
        ),
    }
    report_type = "write_report"
    default_size = DEFAULT_MODULES  # modules in the mock repository
    real_repo = True
    goals = {"one": GOAL, "all": GOAL}
    done = {kind: "goal met: the whole check passes on the branch" for kind in ("one", "all")}

    # --- the world ----------------------------------------------------------

    def make_world(self, seed: int, size: int, goal_kind: str) -> MockRepo:
        return _repo(seed, size, goal_kind)

    def world_process(self, session: Row) -> bool:
        return False  # the tools work on the git workspace themselves

    def prepare(self, session_id: UUID, world: MockRepo | None, args: Any) -> dict[str, Any]:
        """The session's worktree, on a branch of its own, from the mock
        repository written out as a new repository, or from the real one."""
        run_dir = (Path("runs") / str(session_id)).resolve()
        run_dir.mkdir(parents=True, exist_ok=True)
        if world is not None:
            repo = run_dir / "origin"
            repo.mkdir(exist_ok=True)
            ws.init_repo(repo, world.files)
            check, protected = CHECK, PROTECTED
        else:
            repo = Path(args.repo).expanduser().resolve()
            check = args.check
            protected = () if args.allow_test_edits else TEST_FILES
        ws.check_argv(check, None)  # a check without {item} is refused before anything starts
        branch = f"lha/{str(session_id)[-12:]}"
        work = run_dir / "work"
        base = ws.open_worktree(repo, work, branch)
        guard = f", without changing {', '.join(protected)}" if protected else ""
        return {"repo": str(repo), "work": str(work), "branch": branch, "base": base, "check": check,
                "protected": list(protected), "guard": guard}  # fmt: skip

    def tools(self, http, task_key, attempt, log, load, session) -> CodeTools:
        return CodeTools(http, task_key, attempt, log, load, session=session)

    # --- tasks ----------------------------------------------------------------

    def start(self, world: MockRepo | None) -> list[str]:
        return []

    def start_tasks(self, world: MockRepo | None, goal: str) -> list[tuple[str, BaseModel]]:
        return [("check_all", CheckInput())]

    def task_key(self, task_type: str, inp: BaseModel) -> str:
        return task_key(task_type, inp)

    def scope(self, task_type: str, inp: BaseModel) -> list[str]:
        return task_scope(inp)

    def resource(self, task_type: str, inp: BaseModel) -> str | None:
        return inp.item if isinstance(inp, FixInput) else None

    def report_input(self, partial: bool) -> ReportInput:
        return ReportInput(partial=partial)

    def pointer_wanted(self, task: Row, path: str) -> bool:
        return False  # the branch moves, so every attempt reads and checks it afresh

    async def accept(self, coord, conn, task, out) -> None:
        accept = {
            "check_all": rules.accept_check,
            "fix_item": rules.accept_fix,
            "write_report": rules.accept_report,
        }
        await accept[task.type](coord, conn, task, out)

    async def replan(self, coord, conn, task, kind, delay, probe) -> None:
        await rules.replan(coord, conn, task, kind, delay, probe)

    async def goal_met(self, coord, conn, session) -> bool:
        return await rules.goal_met(coord, conn)

    async def reopen(self, coord, conn) -> int:
        return await rules.reopen(coord, conn)

    # --- scoring ---------------------------------------------------------------

    async def score(
        self, conn: AsyncConnection, session: Row, world: MockRepo | None
    ) -> list[tuple[str, bool]]:
        """On the mock repository, the check runs again on the finished branch,
        and the tests must be the ones it started with."""
        if world is None:
            return []
        p = session.params
        work = Path(p["work"])
        template = p["check"].replace("{python}", shlex.quote(sys.executable))
        run = ws.run_check(work, ws.head(work), template, None, [])
        tests = ws.changed_since(work, p["base"], ["tests"])
        return [
            (f"the check passes on the branch ({len(world.bugs)} bug(s) planted)", run.passed),
            ("no test was changed", not tests),
        ]

    async def stats(self, conn: AsyncConnection, sid: UUID, world: MockRepo | None) -> dict[str, Any]:
        f, t = models.facts.c, models.tasks.c
        q = select(f.value).where(
            f.session_id == sid, f.subject.like("item:%"), f.key == F.STATUS, f.status == VERIFIED
        )
        items = [v["status"] for v in (await conn.execute(q)).scalars()]
        rounds = select(func.count()).where(
            t.session_id == sid, t.type == "check_all", t.status == "succeeded"
        )
        e = models.events.c
        stale = select(func.count()).where(
            e.session_id == sid,
            e.kind == "decision",
            e.payload["reason"].astext.like("%changed on the branch%"),
        )
        return {
            "items_fixed": items.count("fixed"),
            "items_passing": items.count("passing"),
            "checks_run": (await conn.execute(rounds)).scalar_one(),
            "stale_fixes_caught": (await conn.execute(stale)).scalar_one(),
        }

    async def progress(self, conn: AsyncConnection, sid: UUID) -> str:
        branch = await _current(conn, sid, F.BRANCH, F.HEAD)
        result = await _current(conn, sid, F.CHECK, F.RESULT)
        check = (
            "not run yet"
            if result is None
            else ("passes" if result["passed"] else f"{len(result['failing'])} failing")
        )
        return f"fixes committed: {branch['fixes'] if branch else 0} | whole check: {check}"


async def _current(conn: AsyncConnection, sid: UUID, subject: str, key: str) -> Any:
    f = models.facts.c
    q = select(f.value).where(
        f.session_id == sid, f.subject == subject, f.key == key, f.status != "superseded"
    )
    return (await conn.execute(q)).scalar_one_or_none()


CODE = CodeDomain()
