"""The supervisor, which starts every process, watches them and scores the result.

    python -m lha.run --seed 42                 # a full run, prints PASS/FAIL
    python -m lha.run --seed 42 --chaos 0.3     # more faults
    python -m lha.run --seed 42 --goal all      # find every drift, not just one
    python -m lha.run --seed 42 --crashes 0.05  # crash workers and the coordinator
    python -m lha.run --seed 42 --kill-at 120   # crash everything after step 120
    python -m lha.run --resume [SESSION]        # continue an unfinished run (default: latest)

It starts the mock network, the coordinator and the worker processes as
separate OS processes, restarts any of them that dies, and prints progress.
All state lives in Postgres, which is why a restart, and --resume, work.
"""

import argparse
import asyncio
import json
import os
import signal
import socket
import sys
import time
from pathlib import Path
from uuid import UUID

import httpx
from sqlalchemy import func, select, update

from lha.config import ALL_DRIFTS, CRASH_LOOP_LIMIT, DEFAULT_FAULT_RATE, DEFAULT_HOSTS, DEFAULT_STEP_BUDGET
from lha.coordinator import progress_line
from lha.db import crud, init_schema, make_engine
from lha.db.models import events, sessions, tasks
from lha.ids import uuid7
from lha.schemas.tasks import DiscoverInput
from lha.scoring import Score, score_session
from lha.world.model import START_HOSTS

# Worker processes per role.
WORKERS = {"discovery": 2, "analysis": 2, "reporter": 1}
TICK_SECONDS = 0.05
PROGRESS_EVERY_SECONDS = 2.0

GOALS = {
    "one": (
        "Audit the deployment. Exactly one service runs a different number of "
        "replicas than registry.json says. Find it, confirm it with independent "
        "reads, and write a finding backed by verified facts. You start knowing "
        "only these hosts: {hosts}."
    ),
    "all": (
        "Audit the deployment. Some services run a different number of replicas "
        "than registry.json says. Explore every host, give every service a "
        "verdict, confirm every drift with independent reads, and write a "
        "finding listing every verified drift. You start knowing only these "
        "hosts: {hosts}."
    ),
}


class WorldProcess:
    """The mock network running as its own process, on a free local port."""

    def __init__(self, proc: asyncio.subprocess.Process, url: str):
        self.proc = proc
        self.url = url

    async def stop(self) -> None:
        await _stop(self.proc)


async def start_world(seed: int, n_hosts: int, fault_rate: float, n_drifts: int = 1) -> WorldProcess:
    port = _free_port()
    proc = await _spawn(
        "lha.world", "--seed", str(seed), "--hosts", str(n_hosts), "--drifts", str(n_drifts),
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
    def __init__(self, engine, session, deadline: float, quiet: bool):
        self.engine = engine
        self.session = session
        self.sid = session.id
        self.deadline = deadline
        self.quiet = quiet
        self.world: WorldProcess | None = None
        self.coordinator: asyncio.subprocess.Process | None = None
        self.restarts = 0
        self.crashes_in_a_row = 0
        self.decisions_at_last_crash = -1
        self.workers: dict[str, tuple[str, asyncio.subprocess.Process]] = {}  # name -> (role, proc)

    async def start(self) -> None:
        s = self.session
        self.world = await start_world(s.seed, s.n_hosts, s.fault_rate, s.n_drifts)
        await self._start_coordinator()
        for role, count in WORKERS.items():
            for i in range(1, count + 1):
                await self._start_worker(role, f"{role}-{i}")

    async def _start_coordinator(self) -> None:
        args = ["--session", str(self.sid), "--deadline", str(self.deadline)]
        self.coordinator = await _spawn(
            "lha.coordinator", *args, *(["--quiet"] if self.quiet else []), show=True
        )

    async def _start_worker(self, role: str, name: str) -> None:
        proc = await _spawn(
            "lha.agents.worker", "--session", str(self.sid), "--role", role,
            "--name", name, "--world", self.world.url,
        )  # fmt: skip
        self.workers[name] = (role, proc)

    async def watch(self, kill_at: int | None) -> str:
        """Look after the run until it ends. Returns 'finished', 'killed' or 'crash_loop'."""
        last_progress = time.monotonic()
        while True:
            await self._restart_dead_workers()
            code = self.coordinator.returncode
            if code == 0:
                return "finished"
            if code is not None:
                if await self._crash_loop(code):
                    return "crash_loop"
                self.restarts += 1
                await self._start_coordinator()
            if kill_at is not None or not self.quiet:
                async with self.engine.connect() as conn:
                    steps = await crud.step_count(conn, self.sid)
                if kill_at is not None and steps >= kill_at:
                    return "killed"
                if not self.quiet and time.monotonic() - last_progress >= PROGRESS_EVERY_SECONDS:
                    last_progress = time.monotonic()
                    print(f"[supervisor] {await progress_line(self.engine, self.sid)}", flush=True)
            await asyncio.sleep(TICK_SECONDS)

    async def _crash_loop(self, code: int) -> bool:
        """A coordinator that keeps crashing without committing anything in
        between is a bug (such as one result that always crashes it), not bad
        luck, so after CRASH_LOOP_LIMIT of those in a row we stop."""
        async with self.engine.begin() as conn:
            q = select(func.count()).where(events.c.session_id == self.sid, events.c.kind == "decision")
            decisions = (await conn.execute(q)).scalar_one()
            await crud.log_event(
                conn, self.sid, "supervisor", "coordinator_restarted",
                {"exit_code": code, "restarts": self.restarts + 1},
            )  # fmt: skip
        self.crashes_in_a_row = self.crashes_in_a_row + 1 if decisions == self.decisions_at_last_crash else 1
        self.decisions_at_last_crash = decisions
        print(f"[supervisor] coordinator exited ({code}); restarting it", flush=True)
        return self.crashes_in_a_row >= CRASH_LOOP_LIMIT

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

    async def stop(self) -> None:
        for proc in self._procs():
            if proc.returncode is None:
                proc.terminate()
        for proc in self._procs():
            await _stop(proc)

    def _procs(self) -> list[asyncio.subprocess.Process]:
        procs = [proc for _, proc in self.workers.values()]
        procs += [self.coordinator] if self.coordinator else []
        return procs + ([self.world.proc] if self.world else [])


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


async def create_session(engine, args: argparse.Namespace) -> UUID:
    sid = uuid7()
    async with engine.begin() as conn:
        await conn.execute(
            sessions.insert().values(
                id=sid,
                seed=args.seed,
                n_hosts=args.hosts,
                fault_rate=args.chaos,
                step_budget=args.step_budget,
                goal_kind=args.goal,
                n_drifts=ALL_DRIFTS if args.goal == "all" else 1,
                crash_rate=args.crashes,
                goal=GOALS[args.goal].format(hosts=", ".join(START_HOSTS)),
                start_hosts=list(START_HOSTS),
                status="running",
            )
        )
        await crud.log_event(conn, sid, "supervisor", "session_started", {"seed": args.seed})
        for host in START_HOSTS:
            await crud.create_task(conn, sid, "discover_host", DiscoverInput(host=host))
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
    s = score.stats
    print()
    print(
        f"steps={s['steps']} tool_calls={s['tool_calls']} model_calls={s['model_calls']} "
        f"faults_injected={s['faults_injected']} model_errors={s['model_errors_injected']} "
        f"rejected={s['outputs_rejected']} retries={s['retries']} replans={s['replans']} "
        f"splits={s['splits']} breaker_opened={s['breaker_opened']} reopened={s['reopened']} "
        f"crashes={s['crashes_injected']} pointers={s['pointer_fetches']} "
        f"hosts={s['hosts_checked']} decoys_refuted={s['decoys_refuted']} "
        f"elapsed={elapsed:.1f}s"
    )
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
        n = await release_leases(engine, sid)
        print(f"[supervisor] resuming session {sid} (seed {session.seed}); released {n} lease(s)")
    else:
        sid = await create_session(engine, args)
        async with engine.connect() as conn:
            session = await crud.get_session(conn, sid)
        print(
            f"[supervisor] session {sid}: seed {args.seed}, {args.hosts} hosts, fault rate {args.chaos}, "
            f"goal '{args.goal}', crash rate {args.crashes}"
        )

    sup = Supervisor(engine, session, deadline=time.time() + args.max_seconds, quiet=args.quiet)
    await sup.start()
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
        await sup.stop()

    score = await score_session(engine, sid)
    await engine.dispose()
    print_score(score, sid, time.monotonic() - started)
    return 0 if score.passed else 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seed", type=int, default=42, help="fixes the world and the faults")
    p.add_argument("--hosts", type=int, default=DEFAULT_HOSTS, help="size of the mock network")
    p.add_argument("--chaos", type=float, default=DEFAULT_FAULT_RATE, help="share of HTTP calls that fail")
    p.add_argument("--goal", choices=sorted(GOALS), default="one", help="find one drift, or all of them")
    p.add_argument("--crashes", type=float, default=0.0, help="share of task attempts that crash a process")
    p.add_argument("--step-budget", type=int, default=DEFAULT_STEP_BUDGET)
    p.add_argument("--max-seconds", type=float, default=600, help="safety limit on wall-clock time")
    p.add_argument("--kill-at", type=int, help="crash the whole run after this many steps")
    p.add_argument(
        "--resume", nargs="?", const="latest", metavar="SESSION",
        help="continue an unfinished session (default: the latest one)",
    )  # fmt: skip
    p.add_argument("--quiet", action="store_true", help="no progress lines")
    return p.parse_args(argv)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    sys.exit(asyncio.run(main(parse_args())))
