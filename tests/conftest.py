import pytest_asyncio

from lha.db import init_schema, make_engine
from lha.db.models import sessions
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
        id=sid, seed=1, n_hosts=20, fault_rate=0.0, step_budget=100, goal_kind="one", n_drifts=1,
        crash_rate=0.0, goal="test", start_hosts=["host-1"], status="running",
    )  # fmt: skip
    async with engine.begin() as conn:
        await conn.execute(sessions.insert().values(**(values | overrides)))
    return sid
