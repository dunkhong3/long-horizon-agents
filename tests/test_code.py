"""Code work on a repository: the mock repository, the workspace's rules, and the coordinator's checks."""

import argparse
import shlex
import sys
from pathlib import Path

import pytest
from sqlalchemy import select

from lha.core.coordinator import Coordinator
from lha.core.domain import Rejected
from lha.db import crud
from lha.db.models import sessions, tasks
from lha.domains.code import CODE, rules
from lha.domains.code import facts as F
from lha.domains.code import workspace as ws
from lha.domains.code.tasks import CheckOutput, FixInput, FixOutput
from lha.domains.code.world import CHECK, LONG_FUNCTIONS, PLAIN_BUGS, generate_repo
from lha.run import parse_args, real_world_problem
from tests.conftest import add_task, new_session

# --- the mock repository ----------------------------------------------------------------


def test_every_mock_repository_has_a_helper_bug_and_the_planted_ones():
    for seed in range(10):
        for goal in ("one", "all"):
            repo = generate_repo(seed, 12, goal)
            assert len(repo.bugs) == 1 + PLAIN_BUGS[goal] and repo.bugs[0].function == "scale"
            assert "tests/test_helpers.py" in repo.failing and len(repo.failing) >= 3 + (goal == "all")
            assert repo.files[f"mathlib/{repo.long_module}.py"].count("def ") == LONG_FUNCTIONS
            for bug in repo.bugs:
                assert bug.wrong in repo.files[bug.path] and bug.right != bug.wrong


def test_the_check_fails_on_exactly_the_planted_items(tmp_path):
    repo = generate_repo(3, 12, "all")
    work = _workspace(tmp_path, repo.files)
    run = ws.run_check(work, ws.head(work), _template(), None, [])
    assert not run.passed
    assert sorted(
        {line.split("::")[0][7:] for line in run.output.splitlines() if line.startswith("FAILED ")}
    ) == (repo.failing)


# --- the workspace --------------------------------------------------------------------------


def test_a_check_only_ever_runs_the_template_with_a_checked_item():
    assert ws.check_argv("uv run pytest -q {item}", "tests/test_a.py::test_b[1-2]") == [
        "uv", "run", "pytest", "-q", "tests/test_a.py::test_b[1-2]",
    ]  # fmt: skip
    assert ws.check_argv("uv run pytest -q {item}", None) == ["uv", "run", "pytest", "-q"]
    for bad in ("-rf", "--rootdir=/", "a;b", "a b", "../secret", "$(x)"):
        with pytest.raises(ws.WorkspaceError):
            ws.check_argv("pytest {item}", bad)
    with pytest.raises(ws.WorkspaceError):
        ws.check_argv("pytest -q", None)  # no {item}


def test_a_check_never_sees_a_provider_key(tmp_path, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-not-a-real-key")
    work = _workspace(tmp_path, {"show.py": "import os\nprint(sorted(os.environ))\n"})
    run = ws.run_check(work, ws.head(work), f"{shlex.quote(sys.executable)} {{item}}", "show.py", [])
    assert run.passed and "PATH" in run.output and "ANTHROPIC_API_KEY" not in run.output


def test_edits_replace_text_found_once_and_never_touch_protected_files(tmp_path):
    (tmp_path / "a.py").write_text("x = 1\ny = 1\n")
    with pytest.raises(ws.WorkspaceError, match="2 times"):
        ws.apply_edits(tmp_path, [{"path": "a.py", "old": "1", "new": "2"}])
    with pytest.raises(ws.WorkspaceError, match="0 times"):
        ws.apply_edits(tmp_path, [{"path": "a.py", "old": "z = 3", "new": "z = 4"}])
    for path in ("tests/test_a.py", "../a.py", "/etc/passwd", ".git/config"):
        with pytest.raises(ws.WorkspaceError):
            ws.apply_edits(tmp_path, [{"path": path, "old": "", "new": "x"}], protected=("tests/*",))
    # All or nothing, so a bad second edit leaves the first unwritten.
    with pytest.raises(ws.WorkspaceError):
        ws.apply_edits(
            tmp_path,
            [{"path": "a.py", "old": "x = 1", "new": "x = 2"}, {"path": "b.py", "old": "q", "new": ""}],
        )
    assert (tmp_path / "a.py").read_text() == "x = 1\ny = 1\n"
    two = [{"path": "a.py", "old": "x = 1", "new": "x = 2"}, {"path": "n/b.py", "old": "", "new": "b\n"}]
    assert ws.apply_edits(tmp_path, two) == ["a.py", "n/b.py"]


# --- the coordinator's checks -------------------------------------------------------------------


async def test_a_fix_is_committed_only_when_its_own_run_passed_on_unchanged_files(engine):
    sid, coord = await _session(engine)
    work = Path(coord.session.params["work"])
    base = ws.head(work)
    helpers = (work / "mathlib/helpers.py").read_text()
    right = next(b for b in CODE.make_world(4, 12, "one").bugs if b.function == "scale")
    edits = [{"path": "mathlib/helpers.py", "old": right.wrong.rstrip("\n"), "new": right.right.rstrip("\n")}]
    async with engine.begin() as conn:
        task, run_id = await _fix_task(conn, sid, coord, "tests/test_helpers.py", edits)
        good = FixOutput(item="tests/test_helpers.py", status="fixed", edits=edits, check_event_id=run_id)
        other = [{**edits[0], "new": edits[0]["new"] + "  # changed"}]
        for bad, why in (
            (FixOutput(**{**good.model_dump(), "edits": other}), "not the ones"),
            (FixOutput(**{**good.model_dump(), "status": "passing"}), "comes with none"),
            (FixOutput(**{**good.model_dump(), "item": "tests/test_mod_01.py"}), "the task is"),
        ):
            with pytest.raises(Rejected, match=why):
                await rules.accept_fix(coord, conn, task, bad)
        await rules.accept_fix(coord, conn, task, good)
        fact = await crud.current_fact(conn, sid, F.item_subject("tests/test_helpers.py"), F.STATUS)
        assert fact.status == "verified" and fact.value["commit"] == ws.head(work) != base
        assert (work / "mathlib/helpers.py").read_text() != helpers
    # A second fix of the same file, checked before the first was committed, is stale.
    async with engine.begin() as conn:
        task2, run2 = await _fix_task(conn, sid, coord, "tests/test_helpers.py", edits, commit=base, round_=2)
        stale = FixOutput(item="tests/test_helpers.py", status="fixed", edits=edits, check_event_id=run2)
        with pytest.raises(Rejected, match="changed on the branch"):
            await rules.accept_fix(coord, conn, task2, stale)


async def test_the_whole_check_names_only_what_its_own_run_names(engine):
    sid, coord = await _session(engine)
    work = Path(coord.session.params["work"])
    run = ws.run_check(work, ws.head(work), _template(), None, [])
    async with engine.begin() as conn:
        await add_task(conn, sid, "check_all", CODE.task_types["check_all"].input(), domain=CODE)
        task = await crud.claim_task(conn, sid, "checker", "w")
        payload = {"tool": "run_check", "path": "run_check::", "ok": True,
                   "response": {"item": "", "commit": ws.head(work), "edits_digest": ws.edits_digest([]),
                                "paths": [], "passed": run.passed, "output": run.output}}  # fmt: skip
        eid = str(await crud.log_event(conn, sid, "w", "tool_call", payload, task.id, task.attempt))
        task = (await conn.execute(select(tasks).where(tasks.c.id == task.id))).one()
        failing = generate_repo(4, 12, "one").failing
        for bad, why in (
            (CheckOutput(passed=True, check_event_id=eid), "the run says not"),
            (
                CheckOutput(passed=False, items=[*failing, "tests/test_made_up.py"], check_event_id=eid),
                "doesn't name",
            ),
            (CheckOutput(passed=False, items=["-x"], check_event_id=eid), "not an item"),
        ):
            with pytest.raises(Rejected, match=why):
                await rules.accept_check(coord, conn, task, bad)
        await rules.accept_check(
            coord, conn, task, CheckOutput(passed=False, items=failing, check_event_id=eid)
        )
        made = (
            await conn.execute(
                select(tasks.c.task_key).where(tasks.c.session_id == sid, tasks.c.type == "fix_item")
            )
        ).scalars()
        assert sorted(made) == sorted(f"fix_item:{i}" for i in failing)


def test_a_real_repository_needs_a_real_model_and_a_check_with_an_item():
    args = parse_args(["--repo", ".", "--check", "uv run pytest -q {item}", "--model", "test"])
    assert args.domain == "code" and real_world_problem(args) is None
    assert "real model" in real_world_problem(
        parse_args(["--repo", ".", "--check", "pytest {item}", "--model", "fake"])
    )
    assert "{item}" in real_world_problem(
        parse_args(["--repo", ".", "--check", "pytest -q", "--model", "test"])
    )
    assert "takes no --repo" in real_world_problem(
        parse_args(["--domain", "audit", "--repo", ".", "--model", "test"])
    )


# --- helpers ------------------------------------------------------------------------------------


def _template() -> str:
    return CHECK.replace("{python}", shlex.quote(sys.executable))


def _workspace(tmp_path: Path, files: dict[str, str]) -> Path:
    (tmp_path / "origin").mkdir()
    ws.init_repo(tmp_path / "origin", files)
    ws.open_worktree(tmp_path / "origin", tmp_path / "work", "lha/test")
    return tmp_path / "work"


async def _session(engine):
    """A code session on the mock repository of seed 4, with its workspace."""
    sid = await new_session(engine, domain="code", seed=4, n_hosts=12, start_points=[], world="mock")
    params = CODE.prepare(sid, CODE.make_world(4, 12, "one"), argparse.Namespace())
    async with engine.begin() as conn:
        await conn.execute(sessions.update().where(sessions.c.id == sid).values(params=params))
    coord = Coordinator(engine, sid)
    await coord.load()
    return sid, coord


async def _fix_task(conn, sid, coord, item, edits, commit=None, round_=1):
    """A fix task with one passing run_check of these edits logged in its attempt."""
    work = Path(coord.session.params["work"])
    await add_task(conn, sid, "fix_item", FixInput(item=item, round=round_), domain=CODE)
    task = await crud.claim_task(conn, sid, "coder", "w")
    response = {
        "item": item, "commit": commit or ws.head(work), "edits_digest": ws.edits_digest(edits),
        "paths": sorted({e["path"] for e in edits}), "passed": True, "output": "1 passed",
    }  # fmt: skip
    payload = {"tool": "run_check", "path": f"run_check:{item}", "ok": True, "response": response}
    eid = str(await crud.log_event(conn, sid, "w", "tool_call", payload, task.id, task.attempt))
    return (await conn.execute(select(tasks).where(tasks.c.id == task.id))).one(), eid
