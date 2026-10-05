import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+asyncpg://lha:lha@localhost:5433/lha")

# Postgres tells the processes waiting on a channel when a task becomes ready
# for a role, or when a result is submitted for a coordinator's partition, so
# nobody has to poll (see lha/db/notify.py).
NOTIFY_TRIGGER = [
    """
    CREATE OR REPLACE FUNCTION lha_notify_tasks() RETURNS trigger AS $$
    BEGIN
      IF NEW.status = 'ready' AND (TG_OP = 'INSERT' OR OLD.status <> 'ready'
                                   OR OLD.not_before IS DISTINCT FROM NEW.not_before) THEN
        PERFORM pg_notify('lha_ready', NEW.session_id::text || ':' || NEW.role);
      ELSIF NEW.status = 'submitted' AND (TG_OP = 'INSERT' OR OLD.status <> 'submitted') THEN
        PERFORM pg_notify('lha_submitted', NEW.session_id::text || ':' || NEW.partition);
      END IF;
      RETURN NEW;
    END $$ LANGUAGE plpgsql
    """,
    "DROP TRIGGER IF EXISTS tasks_notify ON tasks",
    """
    CREATE TRIGGER tasks_notify AFTER INSERT OR UPDATE OF status, not_before ON tasks
    FOR EACH ROW EXECUTE FUNCTION lha_notify_tasks()
    """,
]


def make_engine(url: str = DATABASE_URL, pool_size: int = 5) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True, pool_size=pool_size, max_overflow=pool_size)


async def init_schema(engine: AsyncEngine) -> None:
    """Create the tables and the trigger if they don't exist yet (no migrations for this project).

    Several supervisors can start at once (the benchmark does that), so the
    whole thing runs under one transaction-level advisory lock.
    """
    from lha.db.models import metadata

    async with engine.begin() as conn:
        await conn.execute(text("SELECT pg_advisory_xact_lock(hashtext('lha:init_schema'))"))
        await conn.run_sync(metadata.create_all)
        for statement in NOTIFY_TRIGGER:
            await conn.execute(text(statement))
