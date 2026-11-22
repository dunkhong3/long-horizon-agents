"""An MCP server, so an assistant such as Claude Code can hand long research to lha.

    lha mcp      # speaks MCP over stdin and stdout, and is started by the assistant

MCP, which is the Model Context Protocol, is how an assistant finds and calls
tools that live outside it. start_research starts a research run on the real
web in the background and answers at once with the session id, because a
run takes minutes or hours, while run_status, run_report and list_runs read
how runs are doing and what they found, and continue_run starts a new
supervisor for a run whose supervisor has stopped. Each run is the same as
`lha research --detach` starts, with the same money budget and the same
checks, so its report states nothing that no page says.

Nothing here prints, because stdout carries the protocol, and every run
writes its output to runs/<session>/run.log instead.
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from lha import runs
from lha.config import DEFAULT_BUDGET_USD
from lha.db import init_schema, make_engine
from lha.domains.research import RESEARCH
from lha.run import detach, launch, parse_args

INSTRUCTIONS = """\
lha runs long tasks in the background. It researches a question on the real
web, where every answer in its report is a value that two or more
independent sites state, each quoted word for word from the page it came
from, or it makes a check such as a test suite pass on a git repository, on
a branch of its own, where every commit is a fix that passed its check, or
it fills in a table of fields from a folder of documents, quoting each value.
Start a run with start_research, start_code_task or start_extraction, which
return at once,
then check it now and then with
run_status, which also says what it has spent, and read the result with
run_report once its status is no longer 'running'. A run whose supervisor
stopped, for example because its machine restarted, goes on from where it
was with continue_run.
"""

server = MCPServer("lha", instructions=INSTRUCTIONS)


@server.tool()
async def start_research(
    question: str, budget_usd: float = DEFAULT_BUDGET_USD, max_pages: int = RESEARCH.default_size
) -> dict[str, Any]:
    """Start researching a question on the real web, in the background.

    It plans what to find, searches, reads pages and accepts a value only once
    two or more independent sites agree on it. It answers at once with the
    session id, and the run spends at most budget_usd US dollars and reads at
    most max_pages pages. Follow it with run_status and read it with run_report.
    """
    args = parse_args(["--question", question, "--budget", str(budget_usd), "--size", str(max_pages)])
    try:
        sid = await launch(args)
    except ValueError as e:
        raise ToolError(str(e)) from None
    except (OSError, SQLAlchemyError) as e:
        raise ToolError(_database_hint(e)) from None
    return {"session_id": str(sid), "status": "running", "log": f"runs/{sid}/run.log"}


@server.tool()
async def start_code_task(
    repo: str, check: str, budget_usd: float = DEFAULT_BUDGET_USD, allow_test_edits: bool = False
) -> dict[str, Any]:
    """Make a check pass on a git repository, in the background, on a branch of its own.

    The check is a command with {item} as one of its words, where one failing
    item goes, such as 'uv run pytest -q {item}'. Every fix is committed only
    once that item's check has passed with exactly that fix, the tests are left
    alone unless allow_test_edits is true, and the repository's own folder and
    branches are never touched, so the result is a branch to review. It answers
    at once with the session id, and run_report names the branch once it is done.
    """
    argv = ["--repo", repo, "--check", check, "--budget", str(budget_usd)]
    args = parse_args(argv + (["--allow-test-edits"] if allow_test_edits else []))
    try:
        sid = await launch(args)
    except ValueError as e:
        raise ToolError(str(e)) from None
    except (OSError, SQLAlchemyError) as e:
        raise ToolError(_database_hint(e)) from None
    return {"session_id": str(sid), "status": "running", "log": f"runs/{sid}/run.log"}


@server.tool()
async def start_extraction(
    folder: str, fields: str, budget_usd: float = DEFAULT_BUDGET_USD
) -> dict[str, Any]:
    """Fill in a table of fields from every document in a folder, in the background.

    The fields are written as 'invoice number; total: the amount to pay', each
    with an optional note after a colon on what its value looks like. Every
    value in the table is quoted from its document, and a field a document
    doesn't state is left empty. It answers at once with the session id, and
    run_report says where the table is, as CSV and as JSON, once it is done.
    """
    args = parse_args(["--docs", folder, "--fields", fields, "--budget", str(budget_usd)])
    try:
        sid = await launch(args)
    except ValueError as e:
        raise ToolError(str(e)) from None
    except (OSError, SQLAlchemyError) as e:
        raise ToolError(_database_hint(e)) from None
    return {"session_id": str(sid), "status": "running", "log": f"runs/{sid}/run.log"}


@server.tool()
async def run_status(session_id: str = "") -> dict[str, Any]:
    """How a run is doing (the latest one when no id is given): its status,
    steps, what it has spent and its progress line, and whether its report is ready."""
    async with _engine() as engine:
        return await runs.run_status(engine, await _find(engine, session_id))


@server.tool()
async def run_report(session_id: str = "") -> str:
    """The report of a finished run in markdown (the latest run when no id is
    given), where every answer lists the pages and the sentences behind it."""
    async with _engine() as engine:
        sid = await _find(engine, session_id)
        report = await runs.run_report(engine, sid)
    if report is None:
        return f"Session {sid} has no report yet, because it is still running. Check it with run_status."
    return report


@server.tool()
async def list_runs(limit: int = 10) -> list[dict[str, Any]]:
    """The latest runs, newest first, with their status and goal."""
    async with _engine() as engine:
        return await runs.list_runs(engine, limit)


@server.tool()
async def continue_run(session_id: str) -> dict[str, Any]:
    """Start a new supervisor for an unfinished run whose supervisor has
    stopped, for example after its machine restarted, so it goes on from
    where it was, as `lha resume` does."""
    async with _engine() as engine:
        sid = await _find(engine, session_id)
        status = await runs.run_status(engine, sid)
    if status["status"] != "running":
        raise ToolError(f"session {sid} already finished ({status['status']})")
    if runs.supervisor_state(sid) == "alive":
        raise ToolError(f"session {sid} is still running in the background")
    detach(sid, parse_args(["--resume", str(sid)]))
    return {"session_id": str(sid), "status": "running", "log": f"runs/{sid}/run.log"}


_migrated = False  # the schema is brought up to date once per server process


@asynccontextmanager
async def _engine() -> AsyncIterator[AsyncEngine]:
    global _migrated
    engine = make_engine()
    try:
        if not _migrated:
            await init_schema(engine)
            _migrated = True
        yield engine
    except (OSError, SQLAlchemyError) as e:
        raise ToolError(_database_hint(e)) from None
    finally:
        await engine.dispose()


async def _find(engine: AsyncEngine, ref: str):
    sid = await runs.find_session(engine, ref or None)
    if sid is None:
        raise ToolError(
            f"no session matches {ref!r}, or more than one does" if ref else "there are no runs yet"
        )
    return sid


def _database_hint(e: Exception) -> str:
    return (
        f"lha cannot use its Postgres ({type(e).__name__}: {e}). Start it with `just db-up` in the lha "
        "folder, or set DATABASE_URL where the MCP server is started."
    )


def main() -> None:
    logging.getLogger("alembic").setLevel(logging.WARNING)  # its logs go to stderr, but are noise there
    server.run()  # stdio
