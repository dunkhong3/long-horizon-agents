"""The supervisor, which starts everything, runs the coordinator and scores the result.

    python -m lha.run --seed 42                 # a full run, prints PASS/FAIL
    python -m lha.run --seed 42 --chaos 0.3     # more faults
    python -m lha.run --seed 42 --kill-at 120   # crash everything after step 120
    python -m lha.run --resume [SESSION]        # continue an unfinished run (default: latest)

It starts the mock network and the worker processes as separate OS
processes, runs the coordinator loop itself, and restarts any worker that
dies. All state lives in Postgres, which is why --resume works.
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
from sqlalchemy import select, update

from lha.config import DEFAULT_FAULT_RATE, DEFAULT_HOSTS, DEFAULT_STEP_BUDGET
from lha.coordinator import Coordinator
from lha.db import crud, init_schema, make_engine
from lha.db.models import sessions, tasks
from lha.ids import uuid7
from lha.schemas.tasks import DiscoverInput
from lha.scoring import Score, score_session
from lha.world.model import START_HOSTS

# Worker processes per role.
WORKERS = {"discovery": 2, "analysis": 1, "reporter": 1}

GOAL = (
    "Audit the deployment. Exactly one service runs a different number of "
    "replicas than registry.json says. Find it, confirm it with independent "
    "reads, and write a finding backed by verified facts. You start knowing "
    "only these hosts: {hosts}."
)


class WorldProcess:
    """The mock network running as its own process, on a free local port."""

    def __init__(self, proc: asyncio.subprocess.Process, url: str):
        self.proc = proc
        self.url = url

    async def stop(self) -> None:
        if self.proc.returncode is None:
            self.proc.terminate()
        try:
            await asyncio.wait_for(self.proc.wait(), timeout=5)
        except TimeoutError:
            self.proc.kill()


async def start_world(seed: int, n_hosts: int, fault_rate: float) -> WorldProcess:
    port = _free_port()
    proc = await _spawn(
        "lha.world", "--seed", str(seed), "--hosts", str(n_hosts),
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
    def __init__(self, session_id: UUID, seed: int, n_hosts: int, fault_rate: float):
        self.session_id = session_id
        self.seed = seed
        self.n_hosts = n_hosts
        self.fault_rate = fault_rate
        self.world: asyncio.subprocess.Process | None = None
        self.world_url = ""
        self.workers: dict[str, tuple[str, asyncio.subprocess.Process]] = {}  # name -> (role, proc)

    async def start(self) -> None:
        world = await start_world(self.seed, self.n_hosts, self.fault_rate)
        self.world, self.world_url = world.proc, world.url
        for role, count in WORKERS.items():
            for i in range(1, count + 1):
                await self._start_worker(role, f"{role}-{i}")

    async def _start_worker(self, role: str, name: str) -> None:
        proc = await _spawn(
            "lha.agents.worker", "--session", str(self.session_id), "--role", role,
            "--name", name, "--world", self.world_url,
        )  # fmt: skip
        self.workers[name] = (role, proc)

    async def restart_dead_workers(self) -> None:
        """Called every coordinator loop. A worker that died is replaced, and its
        task's lease expires so the task is handed out again."""
        for name, (role, proc) in list(self.workers.items()):
            if proc.returncode is not None:
                print(f"[supervisor] {name} exited ({proc.returncode}); restarting it")
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
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except TimeoutError:
                proc.kill()

    def _procs(self) -> list[asyncio.subprocess.Process]:
        procs = [proc for _, proc in self.workers.values()]
        return procs + ([self.world] if self.world else [])


async def _spawn(module: str, *args: str) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        sys.executable, "-m", module, *args, stdout=asyncio.subprocess.DEVNULL
    )


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def create_session(engine, seed: int, n_hosts: int, fault_rate: float, step_budget: int) -> UUID:
    sid = uuid7()
    async with engine.begin() as conn:
        await conn.execute(
            sessions.insert().values(
                id=sid,
                seed=seed,
                n_hosts=n_hosts,
                fault_rate=fault_rate,
                step_budget=step_budget,
                goal=GOAL.format(hosts=", ".join(START_HOSTS)),
                start_hosts=list(START_HOSTS),
                status="running",
            )
        )
        await crud.log_event(conn, sid, "supervisor", "session_started", {"seed": seed})
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
        sid = await create_session(engine, args.seed, args.hosts, args.chaos, args.step_budget)
        async with engine.connect() as conn:
            session = await crud.get_session(conn, sid)
        print(f"[supervisor] session {sid}: seed {args.seed}, {args.hosts} hosts, fault rate {args.chaos}")

    sup = Supervisor(sid, session.seed, session.n_hosts, session.fault_rate)
    await sup.start()
    coordinator = Coordinator(
        engine, sid, kill_at=args.kill_at, max_seconds=args.max_seconds,
        on_tick=sup.restart_dead_workers, quiet=args.quiet,
    )  # fmt: skip
    try:
        outcome = await coordinator.run()
        if outcome == "killed":
            # Act out a hard crash of the whole run, with no cleanup at all.
            sup.kill_all()
            print(f"[supervisor] --kill-at {args.kill_at} reached: killed every process")
            print(f"[supervisor] continue with: just start --resume {sid}")
            sys.stdout.flush()
            os._exit(137)
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
