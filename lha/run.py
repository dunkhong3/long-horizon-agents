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
    python -m lha.run --seed 42 --listen 0.0.0.0  # let workers on other machines reach the mock world
    python -m lha.run --join [SESSION]          # lend this machine's workers to a run (default: latest)

It starts the mock network, the coordinators and the worker processes as
separate OS processes, restarts any of them that dies, and prints progress.
All state lives in Postgres, which is why a restart, and --resume, work,
and why another machine that reaches the same Postgres can join a run with
workers of its own, while the machine that started the run stays its home,
which keeps the mock world, the coordinators and the run's files.
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
from sqlalchemy.engine import Row

from lha.config import (
    CONTEXT_WINDOW_TOKENS,
    CRASH_LOOP_LIMIT,
    DEFAULT_BUDGET_USD,
    DEFAULT_FAULT_RATE,
    DEFAULT_STEP_BUDGET,
    MAX_SECONDS,
    REAL_CONTEXT_WINDOW_TOKENS,
    REAL_MAX_SECONDS,
    WORLD_MISSES,
    WORLD_PROBE_SECONDS,
)
from lha.core.agent import AUTO, FAKE, resolve_model
from lha.core.coordinator import progress_line
from lha.core.domain import Domain, create_start_tasks, get_domain
from lha.core.scoring import Score, score_session
from lha.db import crud, init_schema, make_engine
from lha.db.migrate import schema_problem
from lha.db.models import events, sessions, tasks
from lha.domains import DOMAINS
from lha.ids import uuid7
from lha.machines import advertised_host, is_loopback, local_address, machine_name, machine_of, new_crew

# Worker processes per role. Every role of a domain but the last (discovery
# and analysis in the audit) gets two unless --workers says otherwise, and
# the last one, which writes the report, gets one.
DEFAULT_WORKERS = 2
LISTEN = "127.0.0.1"  # where the mock world listens, unless --listen or the run says otherwise
TICK_SECONDS = 0.05
STOP_GRACE_SECONDS = 3.0  # time for workers to exit by themselves after a finished run
PROGRESS_EVERY_SECONDS = 2.0
FETCH_MODES = ("direct", "provider")  # how real pages are read (--fetch)


class WorldProcess:
    """The mock network running as its own process, on a free local port."""

    def __init__(self, proc: asyncio.subprocess.Process, url: str):
        self.proc = proc
        self.url = url

    async def stop(self) -> None:
        await _stop(self.proc)


async def start_world(
    domain: str, seed: int, size: int, fault_rate: float, goal: str = "one", listen: str = "127.0.0.1"
) -> WorldProcess:
    """Start the mock world on a free port, listening on `listen`, and return
    the address other processes reach it on, which other machines can use
    only when it listens on more than this machine (see --listen)."""
    port = _free_port(local_address(listen))
    proc = await _spawn(
        "lha.core.world", "--domain", domain, "--seed", str(seed), "--size", str(size), "--goal", goal,
        "--fault-rate", str(fault_rate), "--port", str(port), "--host", listen,
    )  # fmt: skip
    health = f"http://{local_address(listen)}:{port}/health"
    url = f"http://{advertised_host(listen)}:{port}"
    async with httpx.AsyncClient() as client:
        for _ in range(100):
            try:
                if (await client.get(health)).status_code == 200:
                    return WorldProcess(proc, url)
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.1)
    proc.kill()
    raise RuntimeError("the mock network did not start")


class Supervisor:
    """Starts a run's processes on this machine and keeps them going. The one
    that starts or resumes a run also starts its mock world and coordinators
    and makes this machine the run's home, while one that joins a run from
    another machine (join=True) only starts workers. Every worker is named
    after this supervisor's crew, so no two supervisors share a worker name."""

    def __init__(
        self,
        engine,
        session,
        workers: dict[str, int],
        deadline: float,
        quiet: bool,
        machine: str = "",
        join: bool = False,
        listen: str = LISTEN,
    ):
        self.engine = engine
        self.session = session
        self.sid = session.id
        self.worker_counts = workers
        self.deadline = deadline
        self.quiet = quiet
        self.machine = machine or machine_name()
        self.crew = new_crew(self.machine)
        self.join = join
        self.listen = listen
        self.world: WorldProcess | None = None
        # One coordinator process per partition, where partition 0 leads.
        self.coordinators: dict[int, asyncio.subprocess.Process] = {}
        self.restarts = 0
        self.crashes_in_a_row: dict[int, int] = {}
        self.decisions_at_last_crash: dict[int, int] = {}
        self.workers: dict[str, tuple[str, asyncio.subprocess.Process]] = {}  # name -> (role, proc)

    async def start(self) -> None:
        s = self.session
        if not self.join:
            if get_domain(s.domain).world_process(s):  # otherwise the tools reach their world themselves
                self.world = await start_world(
                    s.domain, s.seed, s.n_hosts, s.fault_rate, s.goal_kind, self.listen
                )
            async with self.engine.begin() as conn:
                await conn.execute(
                    update(sessions)
                    .where(sessions.c.id == self.sid)
                    .values(
                        home_machine=self.machine,
                        world_url=self.world.url if self.world else None,
                        world_listen=self.listen,
                    )
                )
            for partition in range(s.partitions):
                await self._start_coordinator(partition)
        for role, count in self.worker_counts.items():
            for i in range(1, count + 1):
                await self._start_worker(role, f"{self.crew}/{role}-{i}")

    async def _start_coordinator(self, partition: int) -> None:
        args = ["--session", str(self.sid), "--partition", str(partition), "--deadline", str(self.deadline)]
        self.coordinators[partition] = await _spawn(
            "lha.core.coordinator", *args, *(["--quiet"] if self.quiet else []), show=True
        )

    async def _start_worker(self, role: str, name: str) -> None:
        proc = await _spawn(
            "lha.core.worker", "--session", str(self.sid), "--role", role,
            "--name", name, "--machine", self.machine,
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

    async def watch_joined(self) -> str:
        """Look after the workers of a joined run until the session is no
        longer running, which the run's home machine decides, or until this
        machine can no longer take part (see join_problem). While the run's
        mock world doesn't answer, as when the home machine is down and the
        run waits for a resume, this machine's workers are stopped, so they
        don't fail the tasks they would take, and they start again once the
        world answers, at whatever address the resume gave it."""
        last_progress = last_probe = time.monotonic()
        needs_world = get_domain(self.session.domain).world_process(self.session)
        misses, waiting = 0, False
        async with httpx.AsyncClient(timeout=WORLD_PROBE_SECONDS) as client:
            while True:
                async with self.engine.connect() as conn:
                    session = await crud.get_session(conn, self.sid)
                if session.status != "running":
                    return session.status
                if problem := join_problem(session, self.machine):
                    print(f"[supervisor] this machine leaves the run: {problem}", flush=True)
                    return "left"
                if needs_world and time.monotonic() - last_probe >= WORLD_PROBE_SECONDS:
                    last_probe = time.monotonic()
                    misses = 0 if await _answers(client, session.world_url) else misses + 1
                    if misses >= WORLD_MISSES and not waiting:
                        waiting = True
                        await self._stop_workers()
                        print(f"[supervisor] the mock world at {session.world_url} doesn't answer, "
                              "so this machine's workers wait for it", flush=True)  # fmt: skip
                    elif misses == 0 and waiting:
                        waiting = False
                        for name, (role, _) in list(self.workers.items()):
                            await self._start_worker(role, name)
                        print(f"[supervisor] the mock world answers at {session.world_url}, "
                              "so this machine's workers start again", flush=True)  # fmt: skip
                if not waiting:
                    await self._restart_dead_workers()
                if not self.quiet and time.monotonic() - last_progress >= PROGRESS_EVERY_SECONDS:
                    last_progress = time.monotonic()
                    print(f"[supervisor] {await progress_line(self.engine, self.sid)}", flush=True)
                await asyncio.sleep(TICK_SECONDS * 4)

    async def _stop_workers(self) -> None:
        for _, proc in self.workers.values():
            await _stop(proc)
        await self.hand_back()

    async def hand_back(self) -> int:
        """Expire the leases this crew's workers still hold, which a machine
        that leaves a run does, so the coordinator's sweep hands those tasks
        out again now, through the usual retry rule, instead of when the
        leases run out."""
        async with self.engine.begin() as conn:
            expired = await conn.execute(
                update(tasks)
                .where(
                    tasks.c.session_id == self.sid,
                    tasks.c.status == "leased",
                    tasks.c.leased_by.startswith(f"{self.crew}/", autoescape=True),
                )
                .values(lease_expires_at=func.now())
                .returning(tasks.c.id)
            )
            return len(expired.all())

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


async def _answers(client: httpx.AsyncClient, url: str | None) -> bool:
    """Whether the mock world at this address answers its health check."""
    try:
        return bool(url) and (await client.get(f"{url}/health")).status_code == 200
    except httpx.HTTPError:
        return False


def _free_port(host: str = LISTEN) -> int:
    with socket.socket() as s:
        s.bind((host, 0))
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
        await create_start_tasks(
            conn, sid, domain, domain.start_tasks(world, goal, params), args.coordinators
        )
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


async def unfinished_session(engine, wanted: str, verb: str) -> Row | str:
    """The session to resume or join, which `wanted` names by its id or the
    start of it, or the latest unfinished one when it is 'latest', or else
    a message that says why there is none."""
    from lha.runs import find_session  # lha.runs imports this module

    if wanted == "latest":
        sid = await find_resumable(engine)
        if sid is None:
            return f"nothing to {verb}: no unfinished session"
    else:
        sid = await find_session(engine, wanted)
        if sid is None:
            return f"no session matches {wanted!r}, or more than one does, so there is nothing to {verb}"
    async with engine.connect() as conn:
        session = await crud.get_session(conn, sid)
    if session.status != "running":
        return f"session {sid} already finished ({session.status}); nothing to {verb}"
    return session


async def release_leases(engine, sid: UUID, machine: str) -> int:
    """On resume no worker of this machine's old run is alive, so release
    their leases now instead of waiting for them to expire, while workers
    that joined from other machines may still be at work and keep theirs.
    The attempt bump fences out anything that might still arrive from the
    old run."""
    async with engine.begin() as conn:
        q = select(tasks.c.id, tasks.c.leased_by).where(tasks.c.session_id == sid, tasks.c.status == "leased")
        mine = [tid for tid, worker in (await conn.execute(q)).all() if machine_of(worker) in (None, machine)]
        if mine:
            await conn.execute(
                update(tasks)
                .where(tasks.c.id.in_(mine))
                .values(status="ready", attempt=tasks.c.attempt + 1, leased_by=None)
            )
        await crud.log_event(conn, sid, "supervisor", "resumed", {"leases_released": len(mine)})
    return len(mine)


def joined_roles(domain: Domain, home: bool) -> list[str]:
    """The roles whose tasks a machine may take, which is every role on the
    run's home machine, and elsewhere each role with a task type that needs
    none of the home machine's files (TaskType.local)."""
    types = domain.task_types.values()
    return [r for r in domain.roles if home or any(t.role == r and not t.local for t in types)]


def join_problem(session: Row, machine: str) -> str | None:
    """Why this machine can't join a running session, or None when it can."""
    home = session.home_machine
    if home is None:
        return f"session {session.id} has no home machine yet, so start or resume it before joining it"
    domain = get_domain(session.domain)
    if home == machine:
        return None  # more workers on the home machine itself, which has everything
    url = session.world_url or ""
    if domain.world_process(session) and (not url or is_loopback(url)):
        return (
            f"the run's mock world listens only on {home} itself ({url or 'no address'}), so start the run "
            "with --listen 0.0.0.0, or stop it and resume it with that, for other machines to reach it"
        )
    report_role = domain.task_types[domain.report_type].role
    if not set(joined_roles(domain, home=False)) - {report_role}:
        return (
            f"every task of a {session.domain} run but its report needs the files on {home}, "
            "so there is nothing for another machine to do"
        )
    return None


def worker_counts(roles: tuple[str, ...] | list[str], counts: list[int]) -> dict[str, int]:
    """Worker processes per role, where --workers gives the roles before the
    last theirs (DEFAULT_WORKERS each when it names fewer), and the last role,
    which writes the report, gets one."""
    workers = {role: counts[i] if i < len(counts) else DEFAULT_WORKERS for i, role in enumerate(roles[:-1])}
    workers[roles[-1]] = 1
    return workers


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


def print_score(score: Score, sid: UUID, elapsed: float, machines: list[str] | None = None) -> Path:
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
    if machines and len(machines) > 1:
        print(f"machines: {machines[0]} (its home), {', '.join(machines[1:])}")
    print(f"verdict={'PASS' if score.passed else 'FAIL'}  (session {sid}, files in {out_dir})")
    return out_dir


async def main(args: argparse.Namespace) -> int:
    engine = make_engine()
    if args.join:
        try:
            return await join(engine, args)
        finally:
            await engine.dispose()
    await init_schema(engine)
    started = time.monotonic()

    if args.resume:
        session = await unfinished_session(engine, args.resume, "resume")
        if isinstance(session, str):
            print(session)
            return 1
        sid = session.id
        if problem := model_problem(session.model):
            print(f"[supervisor] cannot resume on the model {session.model!r}: {problem}")
            return 2
        if args.detach:
            return _detached(sid, args)
        n = await release_leases(engine, sid, args.machine)
        print(f"[supervisor] resuming session {sid} (seed {session.seed}); released {n} lease(s)")
    else:
        if problem := real_world_problem(args):
            print(f"[supervisor] {problem}")
            return 2
        if problem := model_problem(args.model):
            print(f"[supervisor] cannot use the model {args.model!r}: {problem}")
            return 2
        if problem := await fetch_problem(args):
            print(f"[supervisor] {problem}")
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

    workers = worker_counts(get_domain(session.domain).roles, args.workers)
    deadline = time.time() + time_limit(args.max_seconds, session.model)
    listen = args.listen or session.world_listen or LISTEN  # a resume listens where the run did
    sup = Supervisor(
        engine, session, workers, deadline=deadline, quiet=args.quiet, machine=args.machine, listen=listen
    )
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
    async with engine.connect() as conn:
        machines = await crud.session_machines(conn, sid)
    await engine.dispose()
    print_score(score, sid, time.monotonic() - started, machines)
    return 0 if score.passed else 1


async def joinable(engine, args: argparse.Namespace) -> tuple[Row | None, str | None, int]:
    """The session this machine would join, or else why it can't join it,
    with the exit code for that, where 1 means there is nothing to join."""
    if problem := await schema_problem(engine):
        return None, f"[supervisor] {problem}", 2
    session = await unfinished_session(engine, args.join, "join")
    if isinstance(session, str):
        return None, session, 1
    if problem := join_problem(session, args.machine):
        return None, f"[supervisor] cannot join session {session.id}: {problem}", 2
    return session, None, 0


async def join(engine, args: argparse.Namespace) -> int:
    """Lend this machine's workers to a run that its home machine started and
    looks after, until the run ends. A joining machine starts no coordinator
    and no mock world, and it never changes the schema, which the machines
    already at work on the run depend on, so it only joins a database whose
    schema is the one this code knows."""
    give_up = time.monotonic() + args.wait
    while True:
        # A machine may be started before the run it joins, or even before
        # the home machine has brought the schema up to date (see --wait).
        session, problem, code = await joinable(engine, args)
        if problem is None:
            break
        if time.monotonic() >= give_up:
            print(problem)
            return code
        await asyncio.sleep(1.0)
    if problem := model_problem(session.model):
        print(f"[supervisor] cannot join on the model {session.model!r}: {problem}")
        return 2
    domain = get_domain(session.domain)
    roles = joined_roles(domain, home=session.home_machine == args.machine)
    workers = {r: n for r, n in worker_counts(domain.roles, args.workers).items() if r in roles}
    sup = Supervisor(
        engine, session, workers, deadline=0.0, quiet=args.quiet, machine=args.machine, join=True
    )
    async with engine.begin() as conn:
        payload = {"machine": args.machine, "crew": sup.crew, "workers": workers}
        await crud.log_event(conn, session.id, "supervisor", "machine_joined", payload)
    crew = ", ".join(f"{n} {role}" for role, n in workers.items())
    print(f"[supervisor] {args.machine} joins session {session.id}, whose home is {session.home_machine},")
    print(f"[supervisor] with workers {crew}, as crew {sup.crew}")
    await sup.start()
    status = "interrupted"
    try:
        status = await sup.watch_joined()
    finally:
        await sup.stop(grace=STOP_GRACE_SECONDS if status != "interrupted" else 0.0)
        if handed := await sup.hand_back():
            print(f"[supervisor] handed back {handed} task(s) this machine was still working on")
    async with engine.connect() as conn:
        q = select(func.count(func.distinct(events.c.task_id))).where(
            events.c.session_id == session.id, events.c.actor.startswith(f"{sup.crew}/", autoescape=True)
        )
        worked_on = (await conn.execute(q)).scalar_one()
    print(f"[supervisor] session {session.id} is over ({status}),")
    print(f"[supervisor] and this machine worked on {worked_on} of its tasks")
    return 0


def detach(sid: UUID, args: argparse.Namespace) -> Path:
    """Hand a session to a supervisor of its own in the background, which
    runs it to the end the way --resume does and keeps going after this
    process and its terminal are gone. Its output goes to run.log, and its
    process id to pid, next to the run's other files."""
    out_dir = Path("runs") / str(sid)
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "-m", "lha.run", "--resume", str(sid)]
    cmd += ["--workers", ",".join(map(str, args.workers)), "--machine", args.machine]
    if args.listen:
        cmd += ["--listen", args.listen]
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
    if problem := real_world_problem(args) or model_problem(args.model) or await fetch_problem(args):
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
    if args.question and args.fetch not in FETCH_MODES:
        return f"--fetch (or LHA_FETCH) is 'direct' or 'provider', and not {args.fetch!r}"
    if args.question and args.fetch == "provider" and not os.environ.get("ANTHROPIC_API_KEY"):
        return "--fetch provider has the model provider fetch every page, so it needs ANTHROPIC_API_KEY"
    if args.repo and "{item}" not in shlex.split(args.check or ""):
        return "--repo needs --check, the command that must pass, with {item} as a word where one item goes"
    if args.docs and not (args.fields or "").strip(" ;"):
        return "--docs needs --fields, the fields to find, such as 'invoice number; total: the amount to pay'"
    return None


async def fetch_problem(args: argparse.Namespace) -> str | None:
    """Why the model provider won't fetch pages for a run on the real web with
    --fetch provider, or None when it will. Only a real Anthropic model asks
    the provider, so tests, which never use one, make no call."""
    if not (args.question and args.fetch == "provider" and args.model.startswith("anthropic:")):
        return None
    from lha.domains.research.fetch import fetcher

    if message := await fetcher().refusal():
        return (
            f"the model provider refuses to fetch pages from here, so --fetch provider can't work: {message}"
        )
    return None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.join and args.detach:
        parser.error("--join stays in the foreground, so it takes no --detach")
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
    p.add_argument(
        "--fetch", choices=FETCH_MODES, default=os.environ.get("LHA_FETCH", "direct"),
        help="with --question, read pages with httpx ('direct', the default) or through the model "
        "provider's web fetch ('provider'), for a machine that reaches the model's API but not the open web",
    )  # fmt: skip
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
        default=[DEFAULT_WORKERS, DEFAULT_WORKERS],
        help="workers for the roles before the last, in order (default 2 each), where the last role gets one",
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
    again = p.add_mutually_exclusive_group()
    again.add_argument(
        "--resume", nargs="?", const="latest", metavar="SESSION",
        help="continue an unfinished session (default: the latest one)",
    )  # fmt: skip
    again.add_argument(
        "--join", nargs="?", const="latest", metavar="SESSION",
        help="lend this machine's workers to an unfinished session that another machine runs "
        "(default: the latest one), with --workers for how many",
    )  # fmt: skip
    p.add_argument(
        "--wait", type=float, default=0.0, metavar="SECONDS",
        help="with --join, keep looking this long for a run to join, for a machine started before the run",
    )  # fmt: skip
    p.add_argument(
        "--machine", default=machine_name(),
        help="this machine's name, which must differ between the machines of a run "
        "(default: LHA_MACHINE, or else the hostname)",
    )  # fmt: skip
    p.add_argument(
        "--listen", metavar="HOST",
        help="the address the mock world listens on, where 0.0.0.0 lets workers on other machines reach it "
        f"(default {LISTEN}, or for --resume where the run listened before)",
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
