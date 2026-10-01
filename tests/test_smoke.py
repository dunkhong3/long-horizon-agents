from sqlalchemy import text

from lha.db import make_engine


async def test_db_reachable():
    engine = make_engine()
    async with engine.connect() as conn:
        assert (await conn.execute(text("select 1"))).scalar_one() == 1
    await engine.dispose()
