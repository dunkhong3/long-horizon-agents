"""Schema migrations with Alembic, run in code and not from an alembic.ini.

Every run calls `upgrade` before it starts, so a database is always brought
to the newest schema, and pulling a version that changed the tables no
longer means starting from an empty database. A database created before
there were migrations has the tables but no `alembic_version` table, so it
is stamped as the first revision, which is the schema it already has, and
upgraded from there.

    python -m lha.db.migrate                    # upgrade to the newest revision
    python -m lha.db.migrate revision "add x"   # write a new revision from models.py
    python -m lha.db.migrate check              # fail if models.py and the migrations differ
"""

import argparse
import asyncio
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

FIRST_REVISION = "0001"


def config(conn: Connection) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
    cfg.attributes["connection"] = conn
    return cfg


def next_revision_id(conn: Connection) -> str:
    """Revisions are numbered in order, 0001, 0002 and so on, so their files sort by age."""
    script = ScriptDirectory.from_config(config(conn))
    return f"{len(list(script.walk_revisions())) + 1:04d}"


def _upgrade(conn: Connection) -> None:
    tables = set(inspect(conn).get_table_names())
    if "sessions" in tables and "alembic_version" not in tables:
        command.stamp(config(conn), FIRST_REVISION)
    command.upgrade(config(conn), "head")


async def upgrade(engine: AsyncEngine) -> None:
    """Bring the database to the newest schema. Several supervisors can start
    at once (the benchmark does that), so it runs under one advisory lock."""
    async with engine.begin() as conn:
        await conn.execute(text("SELECT pg_advisory_xact_lock(hashtext('lha:migrate'))"))
        await conn.run_sync(_upgrade)


async def main(args: argparse.Namespace) -> None:
    from lha.db import make_engine

    engine = make_engine()
    try:
        if args.action == "upgrade":
            await upgrade(engine)
        else:
            async with engine.begin() as conn:
                if args.action == "revision":
                    await conn.run_sync(
                        lambda c: command.revision(
                            config(c), args.message, autogenerate=True, rev_id=next_revision_id(c)
                        )
                    )
                else:
                    await conn.run_sync(lambda c: command.check(config(c)))
    finally:
        await engine.dispose()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("action", nargs="?", choices=["upgrade", "revision", "check"], default="upgrade")
    p.add_argument("message", nargs="*", help="what the new revision changes")
    args = p.parse_args()
    args.message = " ".join(args.message) or "change the schema"
    asyncio.run(main(args))
