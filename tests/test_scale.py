"""LISTEN/NOTIFY wake-ups and the split of the plan between coordinators."""

from collections import Counter

from lha.core.domain import task_partition
from lha.db import crud
from lha.db.notify import Listener
from lha.domains.audit.tasks import DiscoverInput
from tests.conftest import add_task, new_session


async def test_a_ready_task_wakes_its_role_and_only_its_role(engine):
    sid = await new_session(engine)
    async with (
        Listener("lha_ready", f"{sid}:discovery") as discovery,
        Listener("lha_ready", f"{sid}:analysis") as analysis,
    ):
        async with engine.begin() as conn:
            await add_task(conn, sid, "discover_host", DiscoverInput(host="host-1"))
        assert await discovery.wait(5)  # delivered on commit
        assert not await analysis.wait(0.2)


async def test_a_submitted_result_wakes_its_partition(engine):
    sid = await new_session(engine, partitions=2)
    inp = DiscoverInput(host="host-1")
    part = task_partition(inp.host, 2)
    async with (
        Listener("lha_submitted", f"{sid}:{part}") as mine,
        Listener("lha_submitted", f"{sid}:{1 - part}") as other,
    ):
        async with engine.begin() as conn:
            await add_task(conn, sid, "discover_host", inp, partitions=2)
            task = await crud.claim_task(conn, sid, "discovery", "w")
            await crud.submit_result(conn, task.id, "w", task.attempt, {"error": {"kind": "timeout"}})
        assert await mine.wait(5)
        assert not await other.wait(0.2)


def test_partitions_are_stable_and_spread():
    hosts = [f"host-{i}" for i in range(1, 401)]
    counts = Counter(task_partition(h, 4) for h in hosts)
    assert set(counts) == {0, 1, 2, 3} and min(counts.values()) > 70
    assert [task_partition(h, 4) for h in hosts] == [task_partition(h, 4) for h in hosts]
    assert task_partition(None, 4) == 0  # a task without a resource (the report) goes to the leader
    assert task_partition(hosts[0], 1) == 0
