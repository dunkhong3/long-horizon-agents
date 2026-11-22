"""The lha command and the MCP server, which start runs in the background and read them back."""

import os
import re
import subprocess
import sys
import time

from mcp import Client
from sqlalchemy import update

from lha import cli, mcp_server, runs
from lha.db.models import sessions
from tests.conftest import new_session


def test_env_files_fill_in_only_what_the_shell_has_not_set(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("# a comment\nLHA_A=from-file\nexport LHA_B='quoted'\nLHA_C=from-file\nnot a setting\n")
    monkeypatch.delenv("LHA_A", raising=False)
    monkeypatch.delenv("LHA_B", raising=False)
    monkeypatch.setenv("LHA_C", "from-shell")
    cli.load_env(env)
    assert (cli.os.environ["LHA_A"], cli.os.environ["LHA_B"]) == ("from-file", "quoted")
    assert cli.os.environ["LHA_C"] == "from-shell"


async def test_an_unreachable_postgres_is_named_without_its_password(monkeypatch):
    monkeypatch.setattr("lha.db.DATABASE_URL", "postgresql+asyncpg://lha:secret@127.0.0.1:1/lha")
    problem = await cli._database_problem()
    assert "cannot reach Postgres" in problem and "secret" not in problem and "just db-up" in problem


async def test_a_session_is_found_by_the_start_of_its_id(engine):
    first = await new_session(engine)
    second = await new_session(engine)
    assert await runs.find_session(engine) == second
    assert await runs.find_session(engine, str(first)) == first
    assert await runs.find_session(engine, str(first)[:-4]) == first
    assert await runs.find_session(engine, "not-an-id") is None
    shared = os.path.commonprefix([str(first), str(second)])  # ids made in the same ms range share a start
    assert shared and await runs.find_session(engine, shared) is None


def test_a_detached_run_can_be_followed_and_read():
    out = subprocess.run(
        [sys.executable, "-m", "lha.cli", "start", "--domain", "research", "--seed", "4", "--model", "fake",
         "--detach"],
        capture_output=True, text=True, timeout=120,
    )  # fmt: skip
    assert out.returncode == 0, out.stdout + out.stderr
    sid = re.search(r"session (\S+) runs in the background", out.stdout).group(1)

    def lha(*args: str) -> str:
        done = subprocess.run(
            [sys.executable, "-m", "lha.cli", *args], capture_output=True, text=True, timeout=60
        )
        return done.stdout

    for _ in range(120):
        if '"status": "running"' not in lha("status", sid, "--json"):
            break
        time.sleep(1)
    status = lha("status", sid[:13])
    assert "succeeded" in status and "answers verified: 2" in status
    assert "# Research report" in lha("report", sid)
    assert sid in lha("list")


async def test_the_mcp_server_starts_follows_and_reads_runs(engine):
    sid = await new_session(engine, domain="research", goal="Find the license of Atlas.")
    async with Client(mcp_server.server) as client:
        names = {t.name for t in (await client.list_tools()).tools}
        assert names == {
            "start_research",
            "start_code_task",
            "run_status",
            "run_report",
            "list_runs",
            "continue_run",
        }
        code = await client.call_tool("start_code_task", {"repo": ".", "check": "uv run pytest -q {item}"})
        assert code.is_error and "real model" in code.content[0].text
        # The fake model can't read the real web, so nothing starts and nothing is spent.
        refused = await client.call_tool("start_research", {"question": "Who maintains Atlas?"})
        assert refused.is_error and "real model" in refused.content[0].text
        status = await client.call_tool("run_status", {"session_id": str(sid)})
        assert status.structured_content["status"] == "running"
        assert status.structured_content["supervisor"] == "unknown"  # started by no supervisor at all
        pending = await client.call_tool("run_report", {"session_id": str(sid)})
        assert "no report yet" in pending.content[0].text
        async with engine.begin() as conn:
            report = {"findings": [], "summary": "# Research report\n\nNothing yet.\n"}
            await conn.execute(
                update(sessions).where(sessions.c.id == sid).values(status="failed", report=report)
            )
        assert (
            (await client.call_tool("run_report", {"session_id": str(sid)}))
            .content[0]
            .text.startswith("# Research report")
        )
        finished = await client.call_tool("continue_run", {"session_id": str(sid)})
        assert finished.is_error and "already finished" in finished.content[0].text
        assert (await client.call_tool("run_status", {"session_id": "no-such"})).is_error
