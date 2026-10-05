"""Waking up on Postgres LISTEN/NOTIFY instead of polling.

A trigger on `tasks` (lha/db/__init__.py) sends a notification on the
channel 'lha_ready' with '<session>:<role>' when a task becomes ready, and on
'lha_submitted' with '<session>:<partition>' when a result is submitted.
A Listener holds one plain asyncpg connection that listens on one channel
for one key, and wait() returns as soon as a matching notification arrives,
or when the timeout runs out, which still catches anything a notification
can't announce, such as a backoff ending.

A notification sent inside a transaction is only delivered when that
transaction commits, so a woken process always finds the row it was told
about.
"""

import asyncio

import asyncpg

from lha.db import DATABASE_URL


class Listener:
    def __init__(self, channel: str, key: str, url: str = DATABASE_URL):
        self.channel = channel
        self.key = key
        self.dsn = url.replace("postgresql+asyncpg://", "postgresql://")
        self.event = asyncio.Event()
        self.conn: asyncpg.Connection | None = None
        self.wakeups = 0  # how many times a notification woke us

    async def __aenter__(self) -> "Listener":
        self.conn = await asyncpg.connect(self.dsn)
        await self.conn.add_listener(self.channel, self._on_notify)
        return self

    async def __aexit__(self, *exc) -> None:
        if self.conn is not None:
            await self.conn.close()

    def _on_notify(self, conn, pid, channel, payload) -> None:
        if payload == self.key:
            self.event.set()

    async def wait(self, seconds: float) -> bool:
        """True if a notification woke us, False if `seconds` ran out first."""
        try:
            await asyncio.wait_for(self.event.wait(), seconds)
            self.wakeups += 1
            return True
        except TimeoutError:
            return False
        finally:
            self.event.clear()
