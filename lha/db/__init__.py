import os

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+asyncpg://lha:lha@localhost:5433/lha")


def make_engine(url: str = DATABASE_URL, pool_size: int = 5) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True, pool_size=pool_size, max_overflow=pool_size)


async def init_schema(engine: AsyncEngine) -> None:
    """Bring the database to the newest schema with the migrations (lha/db/migrate.py)."""
    from lha.db.migrate import upgrade

    await upgrade(engine)
