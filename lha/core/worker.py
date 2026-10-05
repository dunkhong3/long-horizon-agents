"""A worker process, which claims a task, runs its agent, submits the result and repeats.

    python -m lha.core.worker --session <id> --role discovery --name discovery-1

Workers share nothing with each other or with the coordinator except
Postgres. They write `events` (every model and tool call) and their own
task's `result`, and they never write facts, because deciding what a result
means is the coordinator's job.
"""

import argparse
import asyncio
import contextlib
import os
import sys
from typing import Any
from uuid import UUID

import httpx
from pydantic_ai.exceptions import UnexpectedModelBehavior
from sqlalchemy import select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from lha.config import HEARTBEAT_SECONDS, IDLE_MAX_SECONDS, LEASE_SECONDS, POLL_SECONDS
from lha.core.agent import AgentContext
from lha.core.context import ContextOverflow, build_packet
from lha.core.domain import get_domain
from lha.core.tools import ToolFailure
from lha.db import crud, make_engine
from lha.db.models import events
from lha.db.notify import Listener
from lha.faults import roll


class LeaseLost(Exception):
    """Our task was reclaimed or cancelled while we were working on it."""


class Worker:
    def __init__(self, engine: AsyncEngine, session_id: UUID, role: str, name: str, world_url: str):
        self.engine = engine
        self.session_id = session_id
        self.role = role
        self.name = name
        self.world_url = world_url
        self.parent_pid = os.getppid()

    async def run(self) -> None:
        async with self.engine.connect() as conn:
            self.session = await crud.get_session(conn, self.session_id)
        self.domain = get_domain(self.session.domain)
        listener = None
        if self.session.wakeups == "notify":
            listener = Listener("lha_ready", f"{self.session_id}:{self.role}")
        claims = empty = 0
        async with httpx.AsyncClient(base_url=self.world_url) as http, listener or contextlib.nullcontext():
            self.http = http
            while await self._session_running() and not self._orphaned():
                async with self.engine.begin() as conn:
                    task = await crud.claim_task(conn, self.session_id, self.role, self.name)
                claims += 1
                if task is None:
                    empty += 1
                    await self._idle(listener)
                    continue
                await self._handle(task)
        # How often we asked for work and found none, which is what the
        # benchmark compares between polling and LISTEN/NOTIFY.
        stats = {"claims": claims, "empty_claims": empty, "wakeups": listener.wakeups if listener else 0}
        async with self.engine.begin() as conn:
            await crud.log_event(conn, self.session_id, self.name, "worker_stats", stats)

    async def _idle(self, listener: Listener | None) -> None:
        """Wait for work, which is a NOTIFY saying a task for our role is ready,
        or the moment a backed-off task becomes due, whichever comes first."""
        if listener is None:
            await asyncio.sleep(POLL_SECONDS)
            return
        async with self.engine.connect() as conn:
            due = await crud.seconds_until_due(conn, self.session_id, self.role)
        await listener.wait(IDLE_MAX_SECONDS if due is None else min(due, IDLE_MAX_SECONDS))

    def _orphaned(self) -> bool:
        """The supervisor died without stopping us (e.g. it was SIGKILLed)."""
        return os.getppid() != self.parent_pid

    async def _session_running(self) -> bool:
        async with self.engine.connect() as conn:
            return await crud.session_status(conn, self.session_id) == "running"

    async def _log(self, task: Row, kind: str, payload: dict[str, Any]) -> UUID:
        async with self.engine.begin() as conn:
            return await crud.log_event(
                conn, self.session_id, self.name, kind, payload, task.id, task.attempt
            )

    async def _handle(self, task: Row) -> None:
        """Run one attempt of one task, with a heartbeat keeping the lease alive."""
        work = asyncio.create_task(self._attempt(task))
        beat = asyncio.create_task(self._heartbeat(task, work))
        try:
            result = await work
        except (asyncio.CancelledError, LeaseLost):
            await self._log(task, "lease_lost", {"reason": "reclaimed or cancelled"})
            return
        finally:
            beat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await beat

        crash = self._injected_crash(task)
        if crash is not None:
            await self._log(task, "crash_injected", {"kind": crash})
            if crash == "exit":
                os._exit(1)  # die holding the lease, which the supervisor then expires
            # 'hang': stay alive but silent past the lease, like a stuck process,
            # and then try to submit as if nothing happened.
            await asyncio.sleep(LEASE_SECONDS + 2 * HEARTBEAT_SECONDS)

        async with self.engine.begin() as conn:
            accepted = await crud.submit_result(conn, task.id, self.name, task.attempt, result)
        if not accepted:
            # We were fenced out, meaning the task moved on (a new attempt, or
            # it was cancelled) while we were finishing, so our result is stale
            # and we drop it.
            await self._log(task, "lease_lost", {"reason": "submit fenced out"})

    def _injected_crash(self, task: Row) -> str | None:
        """A seeded crash after the work is done and before it is submitted (see --crashes)."""
        r = roll(self.session.seed, "worker", task.task_key, task.attempt)
        rate = self.session.crash_rate
        if r < rate / 2:
            return "exit"
        if r < rate:
            return "hang"
        return None

    async def _heartbeat(self, task: Row, work: asyncio.Task) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_SECONDS)
            async with self.engine.begin() as conn:
                alive = await crud.heartbeat(conn, task.id, self.name, task.attempt)
            if not alive:
                work.cancel()  # stop working on a task that isn't ours anymore
                return

    async def _attempt(self, task: Row) -> dict[str, Any]:
        """Returns the result to submit, which is the agent's output or an error."""

        async def log(kind: str, payload: dict[str, Any]) -> UUID:
            return await self._log(task, kind, payload)

        async def load(event_id: str) -> dict[str, Any] | None:
            """A successful tool call this task may reuse (see crud.earlier_reads)."""
            try:
                eid = UUID(event_id)
            except ValueError:
                return None
            async with self.engine.connect() as conn:
                q = select(events.c.payload).where(
                    events.c.id == eid,
                    events.c.session_id == self.session_id,
                    events.c.kind == "tool_call",
                    await crud.earlier_reads(conn, task),
                )
                payload = (await conn.execute(q)).scalar_one_or_none()
            if payload is None or not payload.get("ok") or payload["tool"] == "fetch_pointer":
                return None
            return payload

        try:
            async with self.engine.connect() as conn:
                packet = await build_packet(conn, self.domain, self.session, task)
            await log("context_packet", packet.model_dump(mode="json"))
            ctx = AgentContext(
                seed=self.session.seed,
                task_id=task.id,
                task_key=task.task_key,
                task_type=task.type,
                attempt=task.attempt,
                packet=packet,
                tools=self.domain.tools(self.http, task.task_key, task.attempt, log, load),
                log=log,
            )
            output = await self.domain.task_types[task.type].agent(ctx)
            return output.model_dump(mode="json")
        except ToolFailure as e:
            return _error(e.kind, e.message)
        except ContextOverflow as e:
            return _error("context_overflow", str(e))
        except UnexpectedModelBehavior as e:
            return _error("invalid_output", str(e))
        except asyncio.CancelledError:
            raise
        except Exception as e:  # a bug in an agent shouldn't kill the worker
            return _error("worker_exception", f"{type(e).__name__}: {e}")


def _error(kind: str, message: str) -> dict[str, Any]:
    return {"error": {"kind": kind, "message": message[:500]}}


async def main(args: argparse.Namespace) -> None:
    engine = make_engine(pool_size=2)  # many workers share one Postgres
    try:
        await Worker(engine, UUID(args.session), args.role, args.name, args.world).run()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", required=True)
    parser.add_argument("--role", required=True, help="which tasks to claim, such as discovery")
    parser.add_argument("--name", required=True)
    parser.add_argument("--world", required=True, help="base URL of the mock network")
    try:
        asyncio.run(main(parser.parse_args()))
    except KeyboardInterrupt:
        sys.exit(0)
