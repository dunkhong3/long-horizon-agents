"""Tasks planned ahead of their inputs, which wait on other tasks (depends_on)."""

import pytest
from sqlalchemy import select, update

from lha.core.coordinator import Coordinator
from lha.core.domain import Planned, create_start_tasks, task_partition
from lha.db import crud
from lha.db.models import events, task_deps, tasks
from lha.domains.audit import AUDIT
from lha.domains.audit.tasks import DiscoverInput
from lha.domains.code.tasks import CheckInput
from tests.conftest import add_task, new_session


async def test_a_planned_task_waits_until_what_it_waits_on_is_done(engine):
    sid = await new_session(engine)
    coord = await _coordinator(engine, sid)
    async with engine.begin() as conn:
        a = await add_task(conn, sid, "discover_host", DiscoverInput(host="host-a"))
        b = await add_task(conn, sid, "discover_host", DiscoverInput(host="host-b"))
        c = await coord.create(
            conn, "discover_host", DiscoverInput(host="host-c"), None, after=[a], needs=[b]
        )
        assert (await _status(conn, c)) == "waiting"
        await coord.settle_waiting(conn)
        assert (await _status(conn, c)) == "waiting"  # a and b are not done
        for claimed in (
            await crud.claim_task(conn, sid, "discovery", "w"),
            await crud.claim_task(conn, sid, "discovery", "w"),
        ):
            assert claimed.id in (a, b)
        assert await crud.claim_task(conn, sid, "discovery", "w") is None  # c is never handed out early
        await _set(conn, a, "succeeded")
        await coord.settle_waiting(conn)
        assert (await _status(conn, c)) == "waiting"  # b still runs
        await _set(conn, b, "succeeded")
        assert await coord.settle_waiting(conn) == 1
        assert (await _status(conn, c)) == "ready"
        assert (await crud.claim_task(conn, sid, "discovery", "w")).id == c
        actions = [p["action"] for p in await _decisions(conn, sid)]
        assert actions == ["release"]


async def test_a_task_that_needs_a_failed_one_fails_while_one_after_it_still_runs(engine):
    sid = await new_session(engine)
    coord = await _coordinator(engine, sid)
    async with engine.begin() as conn:
        a = await add_task(conn, sid, "discover_host", DiscoverInput(host="host-a"))
        needs = await coord.create(conn, "discover_host", DiscoverInput(host="host-n"), None, needs=[a])
        after = await coord.create(conn, "discover_host", DiscoverInput(host="host-t"), None, after=[a])
        await _set(conn, a, "failed")  # failed for good, and nothing replaced it
        await coord.settle_waiting(conn)
        assert (await _status(conn, needs)) == "failed"
        assert (await _status(conn, after)) == "ready"
        reasons = [p["reason"] for p in await _decisions(conn, sid) if p["action"] == "fail"]
        assert reasons == ["upstream_failed: discover_host:host-a failed for good"]


async def test_a_replacement_takes_over_what_waited_on_the_task_it_replaces(engine):
    sid = await new_session(engine)
    coord = await _coordinator(engine, sid)
    async with engine.begin() as conn:
        a = await add_task(conn, sid, "discover_host", DiscoverInput(host="host-a"))
        c = await coord.create(conn, "discover_host", DiscoverInput(host="host-c"), None, needs=[a])
        await _set(conn, a, "failed")
        old = await _row(conn, a)
        # A replan that makes the next round, and a task that only happens
        # to be created during it, which takes over nothing.
        a2 = await coord.create(conn, "discover_host", DiscoverInput(host="host-a", round=2), a, replaces=old)
        await coord.create(conn, "discover_host", DiscoverInput(host="host-x"), a)
        await coord._settle_replacement(conn, old)
        assert await _waits_on(conn, c) == {a2: True}
        await coord.settle_waiting(conn)
        assert (await _status(conn, c)) == "waiting"  # round 2 hasn't run yet
        await _set(conn, a2, "succeeded")
        await coord.settle_waiting(conn)
        assert (await _status(conn, c)) == "ready"


async def test_every_part_of_a_split_task_takes_over_what_waited_on_it(engine):
    sid = await new_session(engine)
    coord = await _coordinator(engine, sid)
    coord.domain = _Splitting()
    async with engine.begin() as conn:
        a = await add_task(conn, sid, "discover_host", DiscoverInput(host="host-a"))
        c = await coord.create(conn, "discover_host", DiscoverInput(host="host-c"), None, needs=[a])
        assert await coord._split(conn, await _row(conn, a), "too big")
        parts = set((await _waits_on(conn, c)).keys())
        assert len(parts) == 2 and a not in parts
        for part in parts:
            await _set(conn, part, "succeeded")
        await coord.settle_waiting(conn)
        assert (await _status(conn, c)) == "ready"


async def test_a_repeated_create_adds_no_edges(engine):
    sid = await new_session(engine)
    coord = await _coordinator(engine, sid)
    async with engine.begin() as conn:
        a = await add_task(conn, sid, "discover_host", DiscoverInput(host="host-a"))
        b = await add_task(conn, sid, "discover_host", DiscoverInput(host="host-b"))
        c = await coord.create(conn, "discover_host", DiscoverInput(host="host-c"), None, after=[a])
        again = await coord.create(conn, "discover_host", DiscoverInput(host="host-c"), None, needs=[b])
        assert again is None and await _waits_on(conn, c) == {a: False}


async def test_start_tasks_wait_only_on_start_tasks_before_them(engine):
    sid = await new_session(engine)
    first, second = DiscoverInput(host="host-1"), DiscoverInput(host="host-2")
    async with engine.begin() as conn:
        planned = [
            ("discover_host", first),
            Planned("discover_host", second, needs=("discover_host:host-1",)),
        ]
        await create_start_tasks(conn, sid, AUDIT, planned, partitions=1)
        one = await crud.task_id_by_key(conn, sid, "discover_host:host-1")
        two = await crud.task_id_by_key(conn, sid, "discover_host:host-2")
        assert (await _status(conn, one)) == "ready" and (await _status(conn, two)) == "waiting"
        assert await _waits_on(conn, two) == {one: True}
        later = [Planned("discover_host", DiscoverInput(host="host-3"), after=("discover_host:host-9",))]
        with pytest.raises(ValueError, match="not start tasks before it"):
            await create_start_tasks(conn, sid, AUDIT, later, partitions=1)


async def test_only_a_tasks_own_coordinator_releases_it(engine):
    sid = await new_session(engine, partitions=2)
    hosts = [f"host-{i}" for i in range(1, 30)]
    mine = next(h for h in hosts if task_partition(h, 2) == 1)
    other = next(h for h in hosts if task_partition(h, 2) == 0)
    leader, second = await _coordinator(engine, sid, 0), await _coordinator(engine, sid, 1)
    async with engine.begin() as conn:
        a = await add_task(conn, sid, "discover_host", DiscoverInput(host=other), partitions=2)
        c = await leader.create(conn, "discover_host", DiscoverInput(host=mine), None, needs=[a])
        await _set(conn, a, "succeeded")
        assert await leader.settle_waiting(conn) == 0  # it belongs to partition 1
        assert await second.settle_waiting(conn) == 1
        assert (await _status(conn, c)) == "ready"


async def test_waiting_work_keeps_a_run_going_and_is_cancelled_at_the_end(engine):
    sid = await new_session(engine, step_budget=1)
    coord = await _coordinator(engine, sid)
    async with engine.begin() as conn:
        a = await add_task(conn, sid, "discover_host", DiscoverInput(host="host-a"))
        c = await coord.create(conn, "discover_host", DiscoverInput(host="host-c"), None, after=[a])
        await _set(conn, a, "leased")
        assert await coord._has_active_tasks(conn)
        await crud.log_event(conn, sid, "w", "model_call", {"final": True})  # the step budget runs out
        await coord._check_goal_and_budget(conn)
        assert (await _status(conn, c)) == "cancelled"


async def test_the_next_whole_check_waits_for_every_fix_of_its_round(engine):
    """The round ends when its last fix does, however the fixes are split
    between coordinators, which a count of the active fixes could miss."""
    from lha.domains.code import CODE, rules

    sid = await new_session(engine, domain="code", partitions=2)
    coord = await _coordinator(engine, sid)
    async with engine.begin() as conn:
        check = await add_task(conn, sid, "check_all", CheckInput(), domain=CODE, partitions=2)
        task = await _row(conn, check)
        items = ["tests/test_a.py", "tests/test_b.py"]
        await rules.plan_round(coord, conn, task, CheckInput(), items)
        fixes = [await crud.task_id_by_key(conn, sid, f"fix_item:{i}") for i in items]
        nxt = await crud.task_id_by_key(conn, sid, "check_all#2")
        assert (await _status(conn, nxt)) == "waiting"
        assert await _waits_on(conn, nxt) == {f: False for f in fixes}
        await _set(conn, fixes[0], "succeeded")
        await _set(conn, fixes[1], "failed")  # given up for this round, and the check names it again
        released = 0
        for partition in (0, 1):
            released += await (await _coordinator(engine, sid, partition)).settle_waiting(conn)
        assert released == 1 and (await _status(conn, nxt)) == "ready"


# --- helpers ------------------------------------------------------------------------------------


class _Splitting(type(AUDIT)):
    """The audit, with a split that makes two batches of any host."""

    async def split(self, coord, conn, task, reason):
        inp = DiscoverInput(**task.input)
        for part in (1, 2):
            await coord.create(
                conn, "discover_host", DiscoverInput(host=inp.host, part=part, parts=2), task.id
            )
        await coord.mark_split(conn, task, reason)
        return True


async def _coordinator(engine, sid, partition=0):
    coord = Coordinator(engine, sid, partition=partition, quiet=True)
    await coord.load()
    return coord


async def _row(conn, task_id):
    return (await conn.execute(select(tasks).where(tasks.c.id == task_id))).one()


async def _status(conn, task_id):
    return (await _row(conn, task_id)).status


async def _set(conn, task_id, status):
    await conn.execute(update(tasks).where(tasks.c.id == task_id).values(status=status))


async def _waits_on(conn, task_id):
    q = select(task_deps.c.depends_on, task_deps.c.required).where(task_deps.c.task_id == task_id)
    return dict((await conn.execute(q)).all())


async def _decisions(conn, sid):
    q = select(events.c.payload).where(events.c.session_id == sid, events.c.kind == "decision")
    return [p for (p,) in (await conn.execute(q.order_by(events.c.created_at))).all()]
