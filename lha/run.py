"""The supervisor, which starts every process, watches them and scores the result.

    python -m lha.run --seed 42                 # a full run, prints PASS/FAIL
    python -m lha.run --seed 42 --chaos 0.3     # more faults
    python -m lha.run --seed 42 --goal all      # find every drift, not just one
    python -m lha.run --seed 42 --crashes 0.05  # crash workers and the coordinator
    python -m lha.run --seed 42 --hosts 200 --coordinators 4 --workers 16,16
    python -m lha.run --domain research --seed 42   # verified research on the mock web
    python -m lha.run --question "..."            # verified research on the real web (a real model)
    python -m lha.run --question "..." --detach   # the same in the background (see: lha status)
    python -m lha.run --seed 42 --model anthropic:claude-sonnet-5-5  # a real model, opt-in
    python -m lha.run --seed 42 --kill-at 120   # crash everything after step 120
    python -m lha.run --resume [SESSION]        # continue an unfinished run (default: latest)

It starts the mock network, the coordinators and the worker processes as
separate OS processes, restarts any of them that dies, and prints progress.
All state lives in Postgres, which is why a restart, and --resume, work.
"""

import argparse
import asyncio
import json
import os
import shlex
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from uuid import UUID

import httpx
from pydantic_ai.models import infer_model
from sqlalchemy import func, select, update

from lha.config import (
    CONTEXT_WINDOW_TOKENS,
    CRASH_LOOP_LIMIT,
    DEFAULT_BUDGET_USD,
    DEFAULT_FAULT_RATE,
    DEFAULT_STEP_BUDGET,
    MAX_SECONDS,
    REAL_CONTEXT_WINDOW_TOKENS,
    REAL_MAX_SECONDS,
)
from lha.core.agent import AUTO, FAKE, resolve_model
from lha.core.coordinator import progress_line
from lha.core.domain import create_task, get_domain
from lha.core.scoring import Score, score_session
from lha.db import crud, init_schema, make_engine
from lha.db.models import events, sessions, tasks
from lha.domains import DOMAINS
from lha.ids import uuid7

# Worker processes per role. The first two roles of a domain (discovery and
# analysis in the audit) get two each unless --workers says otherwise, and the
# last one, which writes the report, gets one.
DEFAULT_WORKERS = 2
TICK_SECONDS = 0.05
STOP_GRACE_SECONDS = 3.0  # time for workers to exit by themselves after a finished run
PROGRESS_EVERY_SECONDS = 2.0


class WorldProcess:
    """The mock network running as its own process, on a free local port."""

    def __init__(self, proc: asyncio.subprocess.Process, url: str):
        self.proc = proc
        self.url = url

    async def stop(self) -> None:
        await _stop(self.proc)


async def start_world(
    domain: str, seed: int, size: int, fault_rate: float, goal: str = "one"
) -> WorldProcess:
    port = _free_port()
    proc = await _spawn(
        "lha.core.world", "--domain", domain, "--seed", str(seed), "--size", str(size), "--goal", goal,
        "--fault-rate", str(fault_rate), "--port", str(port),
    )  # fmt: skip
    url = f"http://127.0.0.1:{port}"
    async with httpx.AsyncClient() as client:
        for _ in range(100):
            try:
                if (await client.get(f"{url}/health")).status_code == 200:
                    return WorldProcess(proc, url)
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.1)
    proc.kill()
    raise RuntimeError("the mock network did not start")


class Supervisor:
    def __init__(self, engine, session, workers: dict[str, int], deadline: float, quiet: bool):
        self.engine = engine
        self.session = session
        self.sid = session.id
        self.worker_counts = workers
        self.deadline = deadline
        self.quiet = quiet
        self.world: WorldProcess | None = None
        # One coordinator process per partition, where partition 0 leads.
        self.coordinators: dict[int, asyncio.subprocess.Process] = {}
        self.restarts = 0
        self.crashes_in_a_row: dict[int, int] = {}
        self.decisions_at_last_crash: dict[int, int] = {}
        self.workers: dict[str, tuple[str, asyncio.subprocess.Process]] = {}  # name -> (role, proc)

    async def start(self) -> None:
        s = self.session
        if get_domain(s.domain).world_process(s):  # otherwise the tools reach their world themselves
            self.world = await start_world(s.domain, s.seed, s.n_hosts, s.fault_rate, s.goal_kind)
        for partition in range(s.partitions):
            await self._start_coordinator(partition)
        for role, count in self.worker_counts.items():
            for i in range(1, count + 1):
                await self._start_worker(role, f"{role}-{i}")

    async def _start_coordinator(self, partition: int) -> None:
        args = ["--session", str(self.sid), "--partition", str(partition), "--deadline", str(self.deadline)]
        self.coordinators[partition] = await _spawn(
            "lha.core.coordinator", *args, *(["--quiet"] if self.quiet else []), show=True
        )

    async def _start_worker(self, role: str, name: str) -> None:
        proc = await _spawn(
            "lha.core.worker", "--session", str(self.sid), "--role", role,
            "--name", name, "--world", self.world.url if self.world else "",
        )  # fmt: skip
        self.workers[name] = (role, proc)

    async def watch(self, kill_at: int | None) -> str:
        """Look after the run until it ends. Returns 'finished', 'killed' or 'crash_loop'."""
        last_progress = time.monotonic()
        while True:
            await self._restart_dead_workers()
            if self.coordinators[0].returncode == 0:
                return "finished"  # the leader ends once the session is over
            for partition, proc in list(self.coordinators.items()):
                if proc.returncode in (None, 0):
                    continue
                if await self._crash_loop(partition, proc.returncode):
                    return "crash_loop"
                self.restarts += 1
                await self._start_coordinator(partition)
            if kill_at is not None or not self.quiet:
                async with self.engine.connect() as conn:
                    steps = await crud.step_count(conn, self.sid)
                if kill_at is not None and steps >= kill_at:
                    return "killed"
                if not self.quiet and time.monotonic() - last_progress >= PROGRESS_EVERY_SECONDS:
                    last_progress = time.monotonic()
                    print(f"[supervisor] {await progress_line(self.engine, self.sid)}", flush=True)
            await asyncio.sleep(TICK_SECONDS)

    async def _crash_loop(self, partition: int, code: int) -> bool:
        """A coordinator that keeps crashing without committing anything in
        between is a bug (such as one result that always crashes it), not bad
        luck, so after CRASH_LOOP_LIMIT of those in a row we stop."""
        async with self.engine.begin() as conn:
            q = select(func.count()).where(events.c.session_id == self.sid, events.c.kind == "decision")
            decisions = (await conn.execute(q)).scalar_one()
            await crud.log_event(
                conn, self.sid, "supervisor", "coordinator_restarted",
                {"partition": partition, "exit_code": code, "restarts": self.restarts + 1},
            )  # fmt: skip
        same = decisions == self.decisions_at_last_crash.get(partition)
        self.crashes_in_a_row[partition] = self.crashes_in_a_row.get(partition, 0) + 1 if same else 1
        self.decisions_at_last_crash[partition] = decisions
        print(f"[supervisor] coordinator {partition} exited ({code}); restarting it", flush=True)
        return self.crashes_in_a_row[partition] >= CRASH_LOOP_LIMIT

    async def _restart_dead_workers(self) -> None:
        """A worker that died is replaced. We know it is dead, so its lease is
        expired now instead of in up to LEASE_SECONDS, and the coordinator's
        sweep hands the task out again through the usual retry rule. A worker
        that exited with 0 did so because the session is over, so it stays down."""
        for name, (role, proc) in list(self.workers.items()):
            if proc.returncode not in (None, 0):
                if not self.quiet:
                    print(f"[supervisor] {name} exited ({proc.returncode}); restarting it", flush=True)
                async with self.engine.begin() as conn:
                    await conn.execute(
                        update(tasks)
                        .where(
                            tasks.c.session_id == self.sid,
                            tasks.c.status == "leased",
                            tasks.c.leased_by == name,
                        )
                        .values(lease_expires_at=func.now())
                    )
                await self._start_worker(role, name)

    def kill_all(self) -> None:
        """SIGKILL every child, which is the harshest crash because nothing gets to clean up."""
        for proc in self._procs():
            if proc.returncode is None:
                proc.kill()

    async def stop(self, grace: float = 0.0) -> None:
        """Stop every process. With a grace period, the processes get that long
        to notice the session is over and exit on their own first (workers log
        their claim counts on the way out)."""
        running = [p for p in self._procs(world=False) if p.returncode is None]
        if grace and running:
            await asyncio.wait([asyncio.ensure_future(p.wait()) for p in running], timeout=grace)
        for proc in self._procs():
            if proc.returncode is None:
                proc.terminate()
        for proc in self._procs():
            await _stop(proc)

    def _procs(self, world: bool = True) -> list[asyncio.subprocess.Process]:
        procs = [proc for _, proc in self.workers.values()]
        procs += list(self.coordinators.values())
        return procs + ([self.world.proc] if self.world and world else [])


async def _spawn(module: str, *args: str, show: bool = False) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        sys.executable, "-m", module, *args, stdout=None if show else asyncio.subprocess.DEVNULL
    )


async def _stop(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is None:
        proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except TimeoutError:
        proc.kill()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def world_kind(args: argparse.Namespace) -> str:
    """'web' with --question, 'repo' with --repo, 'docs' with --docs, and
    otherwise 'mock', a mock world generated from the seed."""
    return "web" if args.question else "repo" if args.repo else "docs" if args.docs else "mock"


async def create_session(engine, args: argparse.Namespace) -> UUID:
    sid = uuid7()
    domain = get_domain(args.domain)
    kind = world_kind(args)
    world = domain.make_world(args.seed, args.hosts, args.goal) if kind == "mock" else None
    params = domain.prepare(sid, world, args)
    start = domain.start(world)
    goal = args.question or domain.goals[args.goal].format(
        start=", ".join(start), **domain.goal_args(world), **params
    )
    async with engine.begin() as conn:
        await conn.execute(
            sessions.insert().values(
                id=sid,
                domain=args.domain,
                seed=args.seed,
                n_hosts=args.hosts,
                fault_rate=args.chaos,
                step_budget=args.step_budget,
                goal_kind=args.goal,
                crash_rate=args.crashes,
                partitions=args.coordinators,
                wakeups="poll" if args.poll else "notify",
                model=args.model,
                context_window=args.window,
                budget_usd=args.budget,
                world=kind,
                params=params,
                goal=goal,
                start_points=start,
                status="running",
            )
        )
        await crud.log_event(conn, sid, "supervisor", "session_started", {"seed": args.seed})
        for task_type, inp in domain.start_tasks(world, goal, params):
            await create_task(conn, sid, domain, task_type, inp, partitions=args.coordinators)
    return sid


async def find_resumable(engine) -> UUID | None:
    async with engine.connect() as conn:
        q = (
            select(sessions.c.id)
            .where(sessions.c.status == "running")
            .order_by(sessions.c.created_at.desc())
            .limit(1)
        )
        return (await conn.execute(q)).scalar_one_or_none()


async def release_leases(engine, sid: UUID) -> int:
    """On resume no worker from the old run is alive, so release their leases
    now instead of waiting for them to expire. The attempt bump fences out
    anything that might still arrive from the old run."""
    async with engine.begin() as conn:
        released = await conn.execute(
            update(tasks)
            .where(tasks.c.session_id == sid, tasks.c.status == "leased")
            .values(status="ready", attempt=tasks.c.attempt + 1, leased_by=None)
            .returning(tasks.c.id)
        )
        n = len(released.all())
        await crud.log_event(conn, sid, "supervisor", "resumed", {"leases_released": n})
    return n


def model_problem(model: str) -> str | None:
    """Why a model can't be used, checked before the run starts, or None.

    Building a real model needs its provider's package (uv sync --extra
    models) and usually an API key in the environment, and it makes no call.
    """
    if model == FAKE:
        return None
    try:
        infer_model(model)
    except Exception as e:
        return f"{type(e).__name__}: {e} (a real model needs `uv sync --extra models` and its API key)"
    return None


def print_score(score: Score, sid: UUID, elapsed: float) -> Path:
    out_dir = Path("runs") / str(sid)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = (score.report or {}).get("summary", "No report was written.\n")
    (out_dir / "finding.md").write_text(summary)
    (out_dir / "score.json").write_text(
        json.dumps({"passed": score.passed, "checks": score.checks, "stats": score.stats}, indent=2)
    )
    print()
    print(summary.strip())
    print()
    for name, ok in score.checks:
        print(f"  {'✓' if ok else '✗'} {name}")
    print()
    shown = {k: v for k, v in score.stats.items() if not isinstance(v, dict) and not k.startswith("prompt")}
    print(" ".join(f"{k}={v}" for k, v in shown.items()) + f" elapsed={elapsed:.1f}s")
    print(f"verdict={'PASS' if score.passed else 'FAIL'}  (session {sid}, files in {out_dir})")
    return out_dir


async def main(args: argparse.Namespace) -> int:
    engine = make_engine()
    await init_schema(engine)
    started = time.monotonic()

    if args.resume:
        sid = await find_resumable(engine) if args.resume == "latest" else UUID(args.resume)
        if sid is None:
            print("nothing to resume: no unfinished session")
            return 1
        async with engine.connect() as conn:
            session = await crud.get_session(conn, sid)
        if session.status != "running":
            print(f"session {sid} already finished ({session.status}); nothing to resume")
            return 1
        if problem := model_problem(session.model):
            print(f"[supervisor] cannot resume on the model {session.model!r}: {problem}")
            return 2
        if args.detach:
            return _detached(sid, args)
        n = await release_leases(engine, sid)
        print(f"[supervisor] resuming session {sid} (seed {session.seed}); released {n} lease(s)")
    else:
        if problem := real_world_problem(args):
            print(f"[supervisor] {problem}")
            return 2
        if problem := model_problem(args.model):
            print(f"[supervisor] cannot use the model {args.model!r}: {problem}")
            return 2
        sid = await create_session(engine, args)
        async with engine.connect() as conn:
            session = await crud.get_session(conn, sid)
        print(
            f"[supervisor] session {sid}: {args.domain}, seed {args.seed}, size {args.hosts}, "
            f"fault rate {args.chaos}, "
            f"goal '{args.goal}', crash rate {args.crashes}, {args.coordinators} coordinator(s), "
            f"workers {'+'.join(map(str, args.workers))}, {'polling' if args.poll else 'LISTEN/NOTIFY'}, "
            f"model {args.model}" + (f", budget ${args.budget:.2f}" if args.budget is not None else "")
        )
        if args.question:
            print(f"[supervisor] researching on the real web: {args.question}")
        if args.detach:
            return _detached(sid, args)

    roles = get_domain(session.domain).roles
    workers = {
        role: (args.workers[i] if i < len(args.workers) else DEFAULT_WORKERS)
        for i, role in enumerate(roles[:-1])
    }
    workers[roles[-1]] = 1
    deadline = time.time() + time_limit(args.max_seconds, session.model)
    sup = Supervisor(engine, session, workers, deadline=deadline, quiet=args.quiet)
    await sup.start()
    outcome = "interrupted"
    try:
        outcome = await sup.watch(args.kill_at)
        if outcome == "killed":
            # Act out a hard crash of the whole run, with no cleanup at all.
            sup.kill_all()
            print(f"[supervisor] --kill-at {args.kill_at} reached: killed every process")
            print(f"[supervisor] continue with: just start --resume {sid}")
            sys.stdout.flush()
            os._exit(137)
        if outcome == "crash_loop":
            print(f"[supervisor] the coordinator crashed {CRASH_LOOP_LIMIT} times in a row without progress;")
            print(f"[supervisor] stopping, and the session stays open for: just start --resume {sid}")
            return 2
    finally:
        await sup.stop(grace=STOP_GRACE_SECONDS if outcome == "finished" else 0.0)

    score = await score_session(engine, sid)
    await engine.dispose()
    print_score(score, sid, time.monotonic() - started)
    return 0 if score.passed else 1


def detach(sid: UUID, args: argparse.Namespace) -> Path:
    """Hand a session to a supervisor of its own in the background, which
    runs it to the end the way --resume does and keeps going after this
    process and its terminal are gone. Its output goes to run.log, and its
    process id to pid, next to the run's other files."""
    out_dir = Path("runs") / str(sid)
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "-m", "lha.run", "--resume", str(sid)]
    cmd += ["--workers", ",".join(map(str, args.workers))]
    if args.max_seconds is not None:
        cmd += ["--max-seconds", str(args.max_seconds)]
    with open(out_dir / "run.log", "ab") as log:
        proc = subprocess.Popen(
            cmd, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
        )
    (out_dir / "pid").write_text(str(proc.pid))
    _detached_procs.append(proc)
    return out_dir / "run.log"


# Supervisors this process started with detach, which a long-lived process
# such as the MCP server must reap once they exit, or they linger as zombies
# that still look alive.
_detached_procs: list[subprocess.Popen] = []


def reap_detached() -> None:
    _detached_procs[:] = [p for p in _detached_procs if p.poll() is None]


async def launch(args: argparse.Namespace) -> UUID:
    """Create a session and start it in the background as --detach does, but
    without printing, for a caller whose output is not a terminal, such as
    the MCP server. A problem with the question or the model raises
    ValueError before any session is created."""
    if problem := real_world_problem(args) or model_problem(args.model):
        raise ValueError(problem)
    engine = make_engine()
    try:
        await init_schema(engine)
        sid = await create_session(engine, args)
    finally:
        await engine.dispose()
    detach(sid, args)
    return sid


def _detached(sid: UUID, args: argparse.Namespace) -> int:
    log = detach(sid, args)
    print(f"[supervisor] session {sid} runs in the background, with its output in {log}")
    print(f"[supervisor] follow it with: lha status {sid}, and read it with: lha report {sid}")
    return 0


def time_limit(max_seconds: float | None, model: str) -> float:
    """How long a run may take, which is --max-seconds when given, and
    otherwise longer for a real model, whose money budget usually ends it first."""
    if max_seconds is not None:
        return max_seconds
    return REAL_MAX_SECONDS if model != FAKE else MAX_SECONDS


def real_world_problem(args: argparse.Namespace) -> str | None:
    """Why a run on the real web (--question) or on a real repository (--repo)
    can't start, or None when it can, or when it runs in a mock world."""
    domain = get_domain(args.domain)
    for flag, given, allowed in (
        ("--question", args.question, domain.real_web),
        ("--repo", args.repo, domain.real_repo),
        ("--docs", args.docs, domain.real_docs),
    ):
        if given and not allowed:
            return f"{args.domain} doesn't run that way, so it takes no {flag}"
    if world_kind(args) == "mock":
        return None
    if args.model == FAKE:
        return (
            "the fake model only works in a mock world, so --question, --repo and --docs need a real model "
            "(set ANTHROPIC_API_KEY, or pass --model with a real model's name)"
        )
    if args.repo and "{item}" not in shlex.split(args.check or ""):
        return "--repo needs --check, the command that must pass, with {item} as a word where one item goes"
    if args.docs and not (args.fields or "").strip(" ;"):
        return "--docs needs --fields, the fields to find, such as 'invoice number; total: the amount to pay'"
    return None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    args = _parser().parse_args(argv)
    if args.domain is None:
        args.domain = (
            "research" if args.question else "code" if args.repo else "extract" if args.docs else "audit"
        )
    if args.hosts is None:
        args.hosts = DOMAINS[args.domain].default_size
    args.model = resolve_model(args.model)
    real = args.model != FAKE
    if args.window is None:
        args.window = REAL_CONTEXT_WINDOW_TOKENS if real else CONTEXT_WINDOW_TOKENS
    if args.budget is None and real:
        args.budget = DEFAULT_BUDGET_USD
    return args


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--domain", choices=sorted(DOMAINS), help="what the agents work on (default: audit)")
    p.add_argument(
        "--question",
        help="research this question on the real web, with a real model (the domain is then research)",
    )
    p.add_argument("--repo", help="work on this git repository with a real model (the domain is then code)")
    p.add_argument(
        "--check",
        help="with --repo, the command that must pass, with {item} where one failing test or file goes",
    )
    p.add_argument("--allow-test-edits", action="store_true", help="with --repo, let fixes change tests too")
    p.add_argument(
        "--docs", help="extract fields from the documents in this folder with a real model (domain extract)"
    )
    p.add_argument(
        "--fields", help="with --docs, the fields to find, as 'name; name: what its value looks like'"
    )
    p.add_argument("--seed", type=int, default=42, help="fixes the world and the faults")
    p.add_argument(
        "--hosts", "--size", type=int, help="size of the world (hosts, sources), with a default per domain"
    )
    p.add_argument("--chaos", type=float, default=DEFAULT_FAULT_RATE, help="share of HTTP calls that fail")
    p.add_argument(
        "--goal", choices=["one", "all"], default="one", help="one drift or question, or all of them"
    )
    p.add_argument("--crashes", type=float, default=0.0, help="share of task attempts that crash a process")
    p.add_argument("--coordinators", type=int, default=1, help="split the plan between this many")
    p.add_argument(
        "--workers", type=lambda v: [int(n) for n in v.split(",")], metavar="N,M",
        default=[DEFAULT_WORKERS, DEFAULT_WORKERS], help="workers for the first two roles (default 2,2)",
    )  # fmt: skip
    p.add_argument("--poll", action="store_true", help="poll for work instead of LISTEN/NOTIFY")
    p.add_argument(
        "--model", default=os.environ.get("LHA_MODEL", AUTO),
        help="'auto' (a real model if ANTHROPIC_API_KEY is set, else the fake one), 'fake', "
        "or a Pydantic AI model name such as anthropic:claude-sonnet-5-5",
    )  # fmt: skip
    p.add_argument("--window", type=int, help="tokens per model call (default: 2000 fake, 32000 real)")
    p.add_argument("--budget", type=float, help="the most a real model may spend, in US dollars (default 5)")
    p.add_argument("--step-budget", type=int, default=DEFAULT_STEP_BUDGET)
    p.add_argument(
        "--max-seconds", type=float, help="safety limit on wall-clock time (default: 600 fake, 3 hours real)"
    )
    p.add_argument("--kill-at", type=int, help="crash the whole run after this many steps")
    p.add_argument(
        "--resume", nargs="?", const="latest", metavar="SESSION",
        help="continue an unfinished session (default: the latest one)",
    )  # fmt: skip
    p.add_argument("--quiet", action="store_true", help="no progress lines")
    p.add_argument(
        "--detach",
        action="store_true",
        help="run in the background and print the session id (see: lha status)",
    )
    return p


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    sys.exit(asyncio.run(main(parse_args())))
