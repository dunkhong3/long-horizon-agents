"""The migrations build exactly the tables in lha/db/models.py."""

from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy import text

from lha.db.migrate import config, upgrade


async def test_database_is_at_the_newest_revision(engine):
    await upgrade(engine)  # a second upgrade does nothing
    async with engine.connect() as conn:
        head = await conn.run_sync(lambda c: ScriptDirectory.from_config(config(c)).get_current_head())
        current = (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalar_one()
    assert current == head


async def test_models_match_the_migrations(engine):
    # Raises when models.py has a change that no revision makes yet.
    async with engine.connect() as conn:
        await conn.run_sync(lambda c: command.check(config(c)))
