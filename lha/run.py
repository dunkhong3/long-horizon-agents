"""CLI entry point. Placeholder until the orchestrator lands: verifies the DB is reachable."""

import asyncio

from sqlalchemy import text

from lha.db import make_engine


async def main() -> None:
    engine = make_engine()
    async with engine.connect() as conn:
        version = (await conn.execute(text("select version()"))).scalar_one()
    await engine.dispose()
    print(f"db ok: {version.split(',')[0]}")


if __name__ == "__main__":
    asyncio.run(main())
