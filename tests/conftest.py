import os
import re
import time
from pathlib import Path

import pytest_asyncio

# Tests only ever run on the fake model, even when a provider key is set, so
# they cost nothing (and every process they start inherits this).
os.environ["LHA_MODEL"] = "fake"

from lha.core.domain import create_task
from lha.db import init_schema, make_engine
from lha.db.models import sessions
from lha.domains.audit import AUDIT
from lha.ids import uuid7


@pytest_asyncio.fixture
async def engine():
    engine = make_engine()
    await init_schema(engine)
    yield engine
    await engine.dispose()


async def new_session(engine, **overrides):
    """A bare session row for tests that work on the tables directly."""
    sid = uuid7()
    values = dict(
        id=sid, domain="audit", seed=1, n_hosts=20, fault_rate=0.0, step_budget=100, goal_kind="one",
        crash_rate=0.0, partitions=1, wakeups="notify", goal="test", start_points=["host-1"],
        status="running",
    )  # fmt: skip
    async with engine.begin() as conn:
        await conn.execute(sessions.insert().values(**(values | overrides)))
    return sid


def session_in(log: Path) -> str:
    """The session a supervisor writing to this file started, from its first
    line, which it prints as soon as the session exists."""
    for _ in range(300):
        if found := re.search(r"session ([0-9a-f-]{36})", log.read_text()):
            return found.group(1)
        time.sleep(0.1)
    raise AssertionError(f"no session in {log}: {log.read_text()!r}")


async def add_task(conn, sid, task_type, inp, partitions=1, domain=AUDIT):
    """Create a task the way the coordinator does, for a domain (the audit by default)."""
    return await create_task(conn, sid, domain, task_type, inp, partitions=partitions)
