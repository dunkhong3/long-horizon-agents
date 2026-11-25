"""The lha command, which starts runs, follows them and prints what they found.

    lha research "question"            research a question on the real web (needs a real model)
    lha research "question" --detach   the same in the background, printing the session id
    lha code REPO --check "..."        make a check pass on a git repository (needs a real model)
    lha extract DIR --fields "..."     fill in a table of fields from a folder of documents (a real model)
    lha start [options]                any run, with the options of python -m lha.run
    lha list                           the latest runs
    lha status [SESSION]               how a run is doing (default: the latest)
    lha report [SESSION]               what a finished run found (default: the latest)
    lha resume [SESSION]               continue an unfinished run (default: the latest)
    lha join [SESSION]                 lend this machine's workers to a run another machine started
    lha dashboard                      a live view of every run, on http://localhost:8000
    lha mcp                            an MCP server, so Claude Code can start and follow research

A session can be named by the start of its id. Like just, lha first reads a
.env file in the working directory, so DATABASE_URL and ANTHROPIC_API_KEY can
be kept there, while a variable already set in the shell wins.
"""

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

PROBE_TIMEOUT_SECONDS = 5.0
WORLDS = {"mock": "a mock world", "web": "the real web", "repo": "a real repository", "docs": "a real folder"}


def load_env(path: Path = Path(".env")) -> None:
    """The KEY=VALUE lines of a .env file, each set only where the environment has no value yet."""
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        key, sep, value = line.strip().removeprefix("export ").partition("=")
        key = key.strip()
        if sep and key and not key.startswith("#"):
            os.environ.setdefault(key, value.strip().strip("'\""))


def main(argv: list[str] | None = None) -> None:
    load_env()  # before anything imports lha.db, which reads DATABASE_URL once
    parser = _parser()
    args, rest = parser.parse_known_args(argv)
    if rest and not args.passes_options:
        parser.error(f"unrecognized arguments: {' '.join(rest)}")
    if args.command != "mcp" and (problem := asyncio.run(_database_problem())):
        print(problem, file=sys.stderr)
        sys.exit(2)
    sys.exit(args.handler(args, rest))


async def _database_problem() -> str | None:
    """Why Postgres can't be reached, or None, found out within a few seconds
    instead of in the middle of a run."""
    from sqlalchemy import text
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import create_async_engine

    from lha.db import DATABASE_URL

    engine = create_async_engine(DATABASE_URL, connect_args={"timeout": PROBE_TIMEOUT_SECONDS})
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as e:  # any reason at all, since this only probes the connection
        url = make_url(DATABASE_URL).render_as_string(hide_password=True)
        return (
            f"lha: cannot reach Postgres at {url} ({type(e).__name__}: {e}).\n"
            "Start it with `just db-up`, which runs it in docker compose on port 5433, "
            "or point DATABASE_URL at another one, in the shell or in .env."
        )
    finally:
        await engine.dispose()
    return None


# --- the commands -------------------------------------------------------------------


def _research(args: argparse.Namespace, rest: list[str]) -> int:
    return _run(["--question", args.question, *rest])


def _code(args: argparse.Namespace, rest: list[str]) -> int:
    return _run(["--repo", args.repo, *rest])


def _extract(args: argparse.Namespace, rest: list[str]) -> int:
    return _run(["--docs", args.folder, *rest])


def _start(args: argparse.Namespace, rest: list[str]) -> int:
    return _run(rest)


def _resume(args: argparse.Namespace, rest: list[str]) -> int:
    if args.session is None:
        return _run(["--resume", *rest])
    sid = _find(args.session)
    return _run(["--resume", str(sid), *rest]) if sid else 1


def _join(args: argparse.Namespace, rest: list[str]) -> int:
    # The supervisor finds the session itself, because finding it here would
    # upgrade the schema, which a joining machine never does (lha/db/migrate.py).
    return _run(["--join", args.session or "latest", *rest])


def _run(argv: list[str]) -> int:
    from lha import run

    return asyncio.run(run.main(run.parse_args(argv)))


def _list(args: argparse.Namespace, rest: list[str]) -> int:
    from lha.runs import list_runs

    rows = _with_engine(lambda engine: list_runs(engine, args.limit))
    if args.json:
        print(json.dumps(rows, indent=2))
    for r in [] if args.json else rows:
        where = WORLDS.get(r["world"], r["world"])
        print(f"{r['session_id']}  {r['status']:9}  {r['started_at'][:19]}  {r['domain']} on {where}")
        print(f"    {_short(r['goal'])}")
    if not rows and not args.json:
        print("no runs yet")
    return 0


def _status(args: argparse.Namespace, rest: list[str]) -> int:
    from lha.runs import run_status

    sid = _find(args.session)
    if sid is None:
        return 1
    s = _with_engine(lambda engine: run_status(engine, sid))
    if args.json:
        print(json.dumps(s, indent=2))
        return 0
    where = WORLDS.get(s["world"], s["world"])
    print(f"session {s['session_id']}: {s['domain']} on {where}, model {s['model']}")
    print(f"goal: {_short(s['goal'], 300)}")
    minutes, seconds = divmod(s["elapsed_seconds"], 60)
    line = f"{s['status']} for {minutes}m {seconds}s"
    if s["supervisor"] == "gone":
        line += f", but its supervisor has stopped, so continue it with: lha resume {sid}"
    elif s["supervisor"] == "alive":
        line += ", in the background"
    print(line)
    if len(s["machines"]) > 1:
        print(f"machines: {s['machines'][0]} (its home), {', '.join(s['machines'][1:])}")
    print(s["progress"])
    if s["report_ready"]:
        print(f"report: lha report {sid}")
    return 0


def _report(args: argparse.Namespace, rest: list[str]) -> int:
    from lha.runs import run_report

    sid = _find(args.session)
    if sid is None:
        return 1
    report = _with_engine(lambda engine: run_report(engine, sid))
    if report is None:
        print(f"session {sid} has no report yet, so follow it with: lha status {sid}")
        return 1
    print(report.rstrip())
    return 0


def _dashboard(args: argparse.Namespace, rest: list[str]) -> int:
    from lha import dashboard

    dashboard.main(rest)
    return 0


def _mcp(args: argparse.Namespace, rest: list[str]) -> int:
    from lha import mcp_server

    mcp_server.main()
    return 0


# --- helpers ------------------------------------------------------------------------


def _with_engine(work: Callable[[Any], Awaitable[Any]]) -> Any:
    async def go() -> Any:
        from lha.db import init_schema, make_engine

        engine = make_engine()
        try:
            await init_schema(engine)
            return await work(engine)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _find(ref: str | None) -> Any:
    from lha.runs import find_session

    sid = _with_engine(lambda engine: find_session(engine, ref))
    if sid is None:
        print(
            f"no session {'matches ' + repr(ref) if ref else 'yet'}, or more than one does", file=sys.stderr
        )
    return sid


def _short(text: str, width: int = 100) -> str:
    text = " ".join(text.split())
    return text if len(text) <= width else text[: width - 3] + "..."


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="lha", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="command", required=True, metavar="COMMAND")
    run_help = "takes the options of python -m lha.run, such as --budget, --size or --detach"

    def command(
        name: str, handler: Callable, text: str, passes_options: bool = False
    ) -> argparse.ArgumentParser:
        c = sub.add_parser(name, help=text, description=text, epilog=run_help if passes_options else None)
        c.set_defaults(handler=handler, passes_options=passes_options)
        return c

    command(
        "research", _research, "research a question on the real web, with a real model", True
    ).add_argument("question")
    command("code", _code, "make a check pass on a git repository, with a real model", True).add_argument(
        "repo"
    )
    command(
        "extract", _extract, "fill in a table from a folder of documents, with a real model", True
    ).add_argument("folder")
    command("start", _start, "start any run", True)
    command("resume", _resume, "continue an unfinished run", True).add_argument("session", nargs="?")
    command(
        "join", _join, "lend this machine's workers to a run that another machine started", True
    ).add_argument("session", nargs="?")
    c = command("list", _list, "the latest runs")
    c.add_argument("--limit", type=int, default=10)
    c.add_argument("--json", action="store_true")
    c = command("status", _status, "how a run is doing")
    c.add_argument("session", nargs="?")
    c.add_argument("--json", action="store_true")
    command("report", _report, "what a finished run found").add_argument("session", nargs="?")
    command("dashboard", _dashboard, "a live view of every run (--host, --port)", True)
    command("mcp", _mcp, "an MCP server over stdin and stdout, for Claude Code")
    return p


if __name__ == "__main__":
    main()
