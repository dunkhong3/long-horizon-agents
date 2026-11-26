"""Workers on more than one machine, which share one Postgres and join a run that its home machine runs."""

import asyncio
import re
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

import httpx
import pytest
from alembic.script import ScriptDirectory
from sqlalchemy import func, select, update

from lha import run
from lha.core import coordinator as coordinator_module
from lha.core.coordinator import Coordinator, db_now
from lha.core.worker import Worker
from lha.db import crud, make_engine
from lha.db.models import events, sessions, tasks
from lha.domains.audit.tasks import DiscoverInput
from lha.domains.code import CODE
from lha.domains.code.tasks import CheckInput
from lha.machines import advertised_host, is_loopback, local_address, machine_of, new_crew
from tests.conftest import add_task, new_session, session_in


def test_a_worker_name_says_which_machine_it_belongs_to(monkeypatch):
    crew = new_crew("my-mac")
    assert re.fullmatch(r"my-mac-[0-9a-f]{4}", crew)
    assert machine_of(f"{crew}/discovery-1") == "my-mac"
    assert machine_of("discovery-1") is None  # a name from before workers were named by crew
    monkeypatch.delenv("LHA_ADVERTISE_HOST", raising=False)
    assert advertised_host("127.0.0.1") == "127.0.0.1" and local_address("0.0.0.0") == "127.0.0.1"
    assert advertised_host("0.0.0.0") == socket.gethostname()
    monkeypatch.setenv("LHA_ADVERTISE_HOST", "box.lan")
    assert advertised_host("0.0.0.0") == "box.lan"
    assert is_loopback("http://127.0.0.1:80") and is_loopback("http://localhost:1")
    assert not is_loopback("http://box.lan:80")


async def test_a_local_task_is_only_taken_on_the_home_machine(engine):
    sid = await new_session(engine, domain="code", home_machine="alpha")
    async with engine.begin() as conn:
        check = await add_task(conn, sid, "check_all", CheckInput(), domain=CODE)
        assert (await conn.execute(select(tasks.c.local).where(tasks.c.id == check))).scalar_one()
        assert await crud.claim_task(conn, sid, "checker", "beta-0001/checker-1", "beta") is None
        backoff = (
            update(tasks).where(tasks.c.id == check).values(not_before=func.now() + timedelta(seconds=30))
        )
        await conn.execute(backoff)  # a worker that waits for it only waits where it may take it
        assert await crud.seconds_until_due(conn, sid, "checker", "beta") is None
        assert 0 < await crud.seconds_until_due(conn, sid, "checker", "alpha") <= 30
        await conn.execute(update(tasks).where(tasks.c.id == check).values(not_before=None))
        assert (await crud.claim_task(conn, sid, "checker", "alpha-0001/checker-1", "alpha")).id == check


async def test_a_task_that_needs_no_files_goes_to_any_machine_and_a_session_without_a_home_to_all(engine):
    away = await new_session(engine, home_machine="alpha")
    legacy = await new_session(engine, domain="code")  # from before sessions had a home
    async with engine.begin() as conn:
        discover = await add_task(conn, away, "discover_host", DiscoverInput(host="host-1"))
        assert (
            await crud.claim_task(conn, away, "discovery", "beta-0001/discovery-1", "beta")
        ).id == discover
        check = await add_task(conn, legacy, "check_all", CheckInput(), domain=CODE)
        assert (await crud.claim_task(conn, legacy, "checker", "beta-0001/checker-1", "beta")).id == check


async def test_a_backoff_is_timed_by_the_database_clock(engine, monkeypatch):
    """Machines' clocks differ, so a Python clock an hour off changes nothing."""

    class _Skewed(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(tz) + timedelta(hours=1)

    monkeypatch.setattr(coordinator_module, "datetime", _Skewed)
    sid = await new_session(engine)
    coord = Coordinator(engine, sid, quiet=True)
    await coord.load()
    async with engine.begin() as conn:
        task_id = await add_task(conn, sid, "discover_host", DiscoverInput(host="host-1"))
        task = (await conn.execute(select(tasks).where(tasks.c.id == task_id))).one()
        await coord._retry_or_fail(conn, task, "tool_error", "the host timed out")
        not_before = (
            await conn.execute(select(tasks.c.not_before).where(tasks.c.id == task_id))
        ).scalar_one()
        wait = (not_before - await db_now(conn)).total_seconds()
    assert 0 < wait < 60


async def test_the_database_clock_is_read_once_a_second_and_carried_forward(engine, monkeypatch):
    """A decision costs no round trip for the time, which still comes from Postgres."""
    sid = await new_session(engine)
    coord = Coordinator(engine, sid, quiet=True)
    await coord.load()
    reads = []
    real = coordinator_module.db_now

    async def counted(conn):
        reads.append(1)
        return await real(conn)

    monkeypatch.setattr(coordinator_module, "db_now", counted)
    async with engine.connect() as conn:
        first = await coord.now(conn)
        second = await coord.now(conn)
        assert len(reads) == 1 and first <= second < first + timedelta(seconds=1)
        coord._clock = (coord._clock[0], coord._clock[1] - 2.0)  # as if two seconds went by
        assert await coord.now(conn) > second and len(reads) == 2


async def test_a_resume_releases_only_the_leases_of_its_own_machine(engine):
    sid = await new_session(engine, home_machine="alpha")
    names = {"alpha": "alpha-0001/discovery-1", "beta": "beta-0002/discovery-1", "old": "discovery-1"}
    ids = {}
    async with engine.begin() as conn:
        for i, (who, name) in enumerate(names.items()):
            ids[who] = await add_task(conn, sid, "discover_host", DiscoverInput(host=f"host-{i}"))
            await conn.execute(
                update(tasks).where(tasks.c.id == ids[who]).values(status="leased", leased_by=name)
            )
    assert await run.release_leases(engine, sid, "alpha") == 2
    async with engine.connect() as conn:
        rows = dict(
            (await conn.execute(select(tasks.c.id, tasks.c.status).where(tasks.c.session_id == sid))).all()
        )
    assert rows[ids["alpha"]] == rows[ids["old"]] == "ready" and rows[ids["beta"]] == "leased"


async def test_a_machine_that_leaves_hands_back_only_its_own_tasks(engine):
    sid = await new_session(engine, home_machine="alpha")
    async with engine.connect() as conn:
        session = await crud.get_session(conn, sid)
    sup = run.Supervisor(engine, session, {}, deadline=0.0, quiet=True, machine="beta", join=True)
    async with engine.begin() as conn:
        mine = await add_task(conn, sid, "discover_host", DiscoverInput(host="host-1"))
        other = await add_task(conn, sid, "discover_host", DiscoverInput(host="host-2"))
        for name in (f"{sup.crew}/discovery-1", "alpha-0001/discovery-1"):  # mine, then the other
            await crud.claim_task(conn, sid, "discovery", name, "beta")
    assert await sup.hand_back() == 1
    async with engine.connect() as conn:
        q = select(tasks.c.id, tasks.c.lease_expires_at <= func.now()).where(tasks.c.session_id == sid)
        expired = dict((await conn.execute(q)).all())
    assert expired == {mine: True, other: False}


async def test_a_worker_follows_the_world_address_the_session_names(engine):
    sid = await new_session(engine, home_machine="alpha", world_url="http://box.lan:8001")
    worker = Worker(engine, sid, "discovery", "beta-0001/discovery-1", "beta")
    try:
        assert await worker._session_running()
        assert str(worker.http.base_url) == "http://box.lan:8001"
        async with engine.begin() as conn:  # a resume started the world again on another port
            await conn.execute(
                update(sessions).where(sessions.c.id == sid).values(world_url="http://box.lan:8002")
            )
        assert await worker._session_running()
        assert str(worker.http.base_url) == "http://box.lan:8002"
    finally:
        await worker.http.aclose()


def test_which_machine_may_join_and_with_which_roles():
    def session(**kw):
        values = dict(
            id="s", domain="audit", world="mock", home_machine="alpha", world_url="http://box.lan:1"
        )
        return SimpleNamespace(**(values | kw))

    assert run.join_problem(session(), "beta") is None
    assert "no home machine yet" in run.join_problem(session(home_machine=None), "beta")
    assert "listens only on alpha" in run.join_problem(session(world_url="http://127.0.0.1:1"), "beta")
    assert run.join_problem(session(world_url="http://127.0.0.1:1"), "alpha") is None  # the same machine
    assert run.join_problem(session(domain="research", world="web", world_url=None), "beta") is None
    assert "nothing for another machine" in run.join_problem(session(domain="code", world_url=None), "beta")
    assert run.join_problem(session(domain="code", world_url=None), "alpha") is None
    assert run.joined_roles(CODE, home=False) == ["writer"]
    assert run.joined_roles(CODE, home=True) == ["coder", "checker", "writer"]
    assert run.worker_counts(("discovery", "analysis", "reporter"), [3]) == {
        "discovery": 3, "analysis": run.DEFAULT_WORKERS, "reporter": 1,
    }  # fmt: skip


async def test_a_join_is_refused_before_anything_starts(engine, capsys, monkeypatch):
    finished = await new_session(engine, status="succeeded", home_machine="alpha")
    loopback = await new_session(engine, home_machine="alpha", world_url="http://127.0.0.1:9")
    cases = [
        (["--join", "ffffffff-no-such"], 1, "no session matches"),
        (["--join", str(finished)], 1, "already finished"),
        (["--join", str(loopback), "--machine", "beta"], 2, "--listen 0.0.0.0"),
    ]
    for argv, code, message in cases:
        assert await run.join(engine, run.parse_args(argv)) == code
        assert message in capsys.readouterr().out
    homeless = await new_session(engine)
    started = time.monotonic()
    assert await run.join(engine, run.parse_args(["--join", str(homeless), "--wait", "1.5"])) == 2
    assert time.monotonic() - started >= 1.5 and "no home machine yet" in capsys.readouterr().out
    monkeypatch.setattr(ScriptDirectory, "get_current_head", lambda self: "9999")
    assert await run.join(engine, run.parse_args(["--join", str(loopback)])) == 2
    assert "this code's is at 9999" in capsys.readouterr().out
    async with engine.connect() as conn:
        q = select(func.count()).where(events.c.session_id == loopback, events.c.kind == "machine_joined")
        assert (await conn.execute(q)).scalar_one() == 0
    for argv in (["--join", "--detach"], ["--join", "--resume"]):
        with pytest.raises(SystemExit):
            run.parse_args(argv)


async def test_a_joined_machine_waits_while_the_world_does_not_answer(engine, capsys):
    host = _own_hostname()
    sid = await new_session(engine, home_machine="alpha", world_url=f"http://{host}:{run._free_port()}")
    async with engine.connect() as conn:
        session = await crud.get_session(conn, sid)
    sup = run.Supervisor(
        engine, session, {"discovery": 1}, deadline=0.0, quiet=True, machine="beta", join=True
    )
    await sup.start()
    watching = asyncio.create_task(sup.watch_joined())
    world = None
    try:
        await _until(lambda: all(proc.returncode is not None for _, proc in sup.workers.values()))
        assert "doesn't answer, so this machine's workers wait" in capsys.readouterr().out
        world = await run.start_world("audit", 1, 20, 0.0, "one", "0.0.0.0")  # as a resume would
        async with engine.begin() as conn:
            await conn.execute(update(sessions).where(sessions.c.id == sid).values(world_url=world.url))
        await _until(lambda: all(proc.returncode is None for _, proc in sup.workers.values()))
        assert f"answers at {world.url}" in capsys.readouterr().out
        async with engine.begin() as conn:
            await conn.execute(update(sessions).where(sessions.c.id == sid).values(status="succeeded"))
        assert await asyncio.wait_for(watching, 10) == "succeeded"
    finally:
        watching.cancel()
        await sup.stop()
        if world is not None:
            await world.stop()


def test_a_second_machine_joins_a_run_and_does_its_work(tmp_path):
    """The home machine starts the run with no workers of its own for the
    first two roles, so the run can only pass if the joining machine works.
    Both machines are this one under two names, which share the Postgres.
    The home's output goes to a file, because a pipe that nobody reads while
    the joined machine works would fill up and stop the home's processes, and
    the session is found in it as soon as the home prints it, because the
    supervisor writes every line out at once."""
    _own_hostname()
    cmd = [sys.executable, "-m", "lha.run", "--quiet", "--model", "fake"]
    alpha = ["--machine", "alpha", "--listen", "0.0.0.0", "--workers", "0,0"]
    log = tmp_path / "home.log"
    with open(log, "w") as out_file:
        home = subprocess.Popen(
            [*cmd, "--seed", "131", "--chaos", "0.2", *alpha], stdout=out_file, stderr=out_file
        )
    try:
        sid = session_in(log)
        url = asyncio.run(_world_url(sid))
        if not asyncio.run(_answers_soon(url)):
            pytest.fail(_why_no_answer(url, home, log))
        joined = subprocess.run(
            [*cmd, "--join", sid, "--wait", "30", "--machine", "beta", "--workers", "2,2"],
            capture_output=True, text=True, timeout=300,
        )  # fmt: skip
        home.wait(timeout=300)
    finally:
        home.kill()
    out = log.read_text()
    assert not is_loopback(url)
    assert home.returncode == 0 and "verdict=PASS" in out and "machines: alpha (its home), beta" in out, out
    assert (
        joined.returncode == 0
        and "is over (succeeded)" in joined.stdout
        and "worked on 0 " not in joined.stdout
    ), joined.stdout + joined.stderr
    machines, by_beta = asyncio.run(_who_worked(sid))
    assert machines == ["alpha", "beta"] and by_beta > 0


def test_a_joined_machine_waits_through_a_crash_of_the_home_and_follows_the_resume(tmp_path):
    """The home machine crashes with every process it had, while a joined
    machine's workers find the mock world gone and wait, and a resume
    without --listen still listens where the run did, so the joined
    machine finds the world at its new address and finishes the run with it."""
    _own_hostname()
    cmd = [sys.executable, "-m", "lha.run", "--quiet", "--model", "fake"]
    crashed = subprocess.run(
        [*cmd, "--seed", "133", "--machine", "alpha", "--listen", "0.0.0.0", "--kill-at", "60"],
        capture_output=True, text=True, timeout=300,
    )  # fmt: skip
    assert crashed.returncode == 137, crashed.stdout + crashed.stderr
    sid = re.search(r"--resume ([0-9a-f-]+)", crashed.stdout).group(1)
    log = tmp_path / "joined.log"
    with open(log, "w") as out_file:
        joined = subprocess.Popen(
            [*cmd, "--join", sid, "--machine", "beta", "--workers", "2,2"], stdout=out_file, stderr=out_file
        )
    try:
        for _ in range(300):
            if "so this machine's workers wait for it" in log.read_text() or joined.poll() is not None:
                break
            time.sleep(0.1)
        assert "so this machine's workers wait for it" in log.read_text(), log.read_text()
        resumed = subprocess.run(
            [*cmd, "--resume", sid, "--machine", "alpha"], capture_output=True, text=True, timeout=300
        )
        joined.wait(timeout=60)
    finally:
        joined.kill()
    out = log.read_text()
    assert resumed.returncode == 0 and "verdict=PASS" in resumed.stdout, resumed.stdout + resumed.stderr
    assert "machines: alpha (its home), beta" in resumed.stdout
    assert joined.returncode == 0 and "so this machine's workers start again" in out, out
    assert "is over (succeeded)" in out


def _why_no_answer(url: str, home: subprocess.Popen, log: Path) -> str:
    """What is known when the world doesn't answer at its advertised address,
    which is whether the home is alive, whether the world answers on this
    machine's own loopback address, and what the advertised name resolves to."""
    parts = urlsplit(url)
    local = asyncio.run(_answers_soon(f"http://127.0.0.1:{parts.port}", tries=2))
    try:
        addresses = sorted(
            {a[4][0] for a in socket.getaddrinfo(parts.hostname, parts.port, type=socket.SOCK_STREAM)}
        )
    except OSError as e:
        addresses = [f"none ({e})"]
    alive = home.poll() is None
    home.kill()
    return (
        f"no answer at {url}, while the home is {'alive' if alive else 'gone'}, the world "
        f"{'answers' if local else 'does not answer'} at 127.0.0.1, and {parts.hostname} is {addresses}; "
        f"the home wrote this:\n{log.read_text()}"
    )


def _own_hostname() -> str:
    """This machine's hostname, which a mock world that listens on every address advertises."""
    try:
        socket.gethostbyname(socket.gethostname())
    except OSError:
        pytest.skip("this machine's own hostname doesn't resolve")
    return socket.gethostname()


async def _until(done, seconds: float = 15.0) -> None:
    for _ in range(int(seconds / 0.1)):
        if done():
            return
        await asyncio.sleep(0.1)
    raise AssertionError("it never happened")


async def _answers_soon(url: str, tries: int = 30) -> bool:
    """Whether the world answers at its advertised address within a few seconds,
    asked the way a joined machine asks it."""
    async with httpx.AsyncClient(timeout=1.0) as client:
        for _ in range(tries):
            if await run._answers(client, url):
                return True
            await asyncio.sleep(0.5)
    return False


async def _world_url(sid: str) -> str:
    """The world's address, once the home machine has started the world and become the run's home."""
    engine = make_engine()
    try:
        for _ in range(300):
            async with engine.connect() as conn:
                q = select(sessions.c.home_machine, sessions.c.world_url).where(sessions.c.id == sid)
                home, url = (await conn.execute(q)).one()
            if home:
                return url
            await asyncio.sleep(0.1)
        raise AssertionError("the home machine never started the run")
    finally:
        await engine.dispose()


async def _who_worked(sid: str) -> tuple[list[str], int]:
    engine = make_engine()
    try:
        async with engine.connect() as conn:
            q = select(func.count()).where(
                events.c.session_id == sid, events.c.kind == "model_call", events.c.actor.startswith("beta-")
            )
            return await crud.session_machines(conn, sid), (await conn.execute(q)).scalar_one()
    finally:
        await engine.dispose()
