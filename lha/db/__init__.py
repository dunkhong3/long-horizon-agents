import os

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+asyncpg://lha:lha@localhost:5432/lha")


def make_engine(url: str = DATABASE_URL) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=5)


async def init_schema(engine: AsyncEngine) -> None:
    """Create the tables if they don't exist yet (no migrations for this project)."""
    from lha.db.models import metadata

    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
