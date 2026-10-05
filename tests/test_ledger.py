"""Leases, fencing and facts, against a real Postgres."""

import asyncio

import pytest

from lha.db import crud
from lha.ids import uuid7
from lha.schemas.tasks import DiscoverInput
from tests.conftest import new_session


async def test_task_creation_is_idempotent(engine):
    sid = await new_session(engine)
    async with engine.begin() as conn:
        first = await crud.create_task(conn, sid, "discover_host", DiscoverInput(host="host-4"))
        again = await crud.create_task(conn, sid, "discover_host", DiscoverInput(host="host-4"))
    assert first is not None and again is None


async def test_concurrent_claims_get_different_tasks(engine):
    """FOR UPDATE SKIP LOCKED: two workers asking at once never share a task."""
    sid = await new_session(engine)
    async with engine.begin() as conn:
        for h in ("host-1", "host-2", "host-3"):
            await crud.create_task(conn, sid, "discover_host", DiscoverInput(host=h))

    async def claim(name):
        async with engine.begin() as conn:
            return await crud.claim_task(conn, sid, "discovery", name)

    claimed = await asyncio.gather(*(claim(f"w{i}") for i in range(5)))
    keys = [c.task_key for c in claimed if c is not None]
    assert sorted(keys) == ["discover_host:host-1", "discover_host:host-2", "discover_host:host-3"]


async def test_stale_attempt_is_fenced_out(engine):
    sid = await new_session(engine)
    async with engine.begin() as conn:
        await crud.create_task(conn, sid, "discover_host", DiscoverInput(host="host-1"))
        task = await crud.claim_task(conn, sid, "discovery", "worker-a")
    # The coordinator reclaims the task (attempt 1 -> 2) while worker-a is slow.
    async with engine.begin() as conn:
        from sqlalchemy import update

        from lha.db.models import tasks

        await conn.execute(update(tasks).where(tasks.c.id == task.id).values(status="ready", attempt=2))
        retried = await crud.claim_task(conn, sid, "discovery", "worker-a")
    async with engine.begin() as conn:
        assert not await crud.heartbeat(conn, task.id, "worker-a", 1)
        assert not await crud.submit_result(conn, task.id, "worker-a", 1, {"late": True})
        assert await crud.submit_result(conn, task.id, "worker-a", retried.attempt, {"ok": True})


async def test_facts_supersede_and_never_replace_verified(engine):
    sid = await new_session(engine)
    tid = uuid7()
    async with engine.begin() as conn:
        a = await crud.upsert_fact(conn, sid, "service:x@host-1", "config.replicas", 1, source_task_id=tid)
        same = await crud.upsert_fact(conn, sid, "service:x@host-1", "config.replicas", 1, source_task_id=tid)
        b = await crud.upsert_fact(conn, sid, "service:x@host-1", "config.replicas", 2, source_task_id=tid)
        assert same == a and b != a  # same value: no-op; new value: new row
        assert (await crud.get_fact(conn, a)).status == "superseded"
        assert (await crud.current_fact(conn, sid, "service:x@host-1", "config.replicas")).value == 2

        await crud.update_fact(conn, sid, b, status="verified")
        with pytest.raises(crud.FactConflict):
            await crud.upsert_fact(conn, sid, "service:x@host-1", "config.replicas", 3, source_task_id=tid)


async def test_batched_fact_writes_follow_the_same_rules(engine):
    sid = await new_session(engine)
    tid = uuid7()
    async with engine.begin() as conn:
        await crud.upsert_facts(conn, sid, [("service:x@host-1", "config.replicas", 1, None),
                                            ("service:y@host-1", "config.replicas", 2, None)],
                                source_task_id=tid)  # fmt: skip
        x = await crud.current_fact(conn, sid, "service:x@host-1", "config.replicas")
        # The same value changes nothing, and a new value supersedes the old row.
        await crud.upsert_facts(conn, sid, [("service:x@host-1", "config.replicas", 1, None),
                                            ("service:y@host-1", "config.replicas", 3, None)],
                                source_task_id=tid)  # fmt: skip
        assert (await crud.current_fact(conn, sid, "service:x@host-1", "config.replicas")).id == x.id
        assert (await crud.current_fact(conn, sid, "service:y@host-1", "config.replicas")).value == 3

        await crud.update_fact(conn, sid, x.id, status="verified")
        with pytest.raises(crud.FactConflict):
            await crud.upsert_facts(
                conn, sid, [("service:x@host-1", "config.replicas", 5, None)], source_task_id=tid
            )
