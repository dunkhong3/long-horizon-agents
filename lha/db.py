import os

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+asyncpg://lha:lha@localhost:5432/lha")


def make_engine(url: str = DATABASE_URL) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True)
