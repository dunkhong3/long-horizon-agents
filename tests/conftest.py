import pytest_asyncio

from lha.db import init_schema, make_engine


@pytest_asyncio.fixture
async def engine():
    engine = make_engine()
    await init_schema(engine)
    yield engine
    await engine.dispose()
