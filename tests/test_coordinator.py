"""The coordinator's recovery rules, against a real Postgres, without any workers."""

from datetime import UTC, datetime

from sqlalchemy import select, text

import lha.core.coordinator as coordinator_module
from lha.core.context import build_packet
from lha.core.coordinator import Coordinator
from lha.db import crud
from lha.db.models import tasks
from lha.domains.audit import AUDIT
from lha.domains.audit import facts as F
from lha.domains.audit.rules import everything_checked
from lha.domains.audit.tasks import DiscoverInput, VerifyInput
from lha.ids import uuid7
from tests.conftest import add_task, new_session


async def coordinator(engine, sid, partition=0):
    """A coordinator ready to have its rules called directly, without its loop."""
    coord = Coordinator(engine, sid, partition=partition)
    await coord.load()
    return coord


async def task_row(conn, sid, key):
    q = select(tasks).where(tasks.c.session_id == sid, tasks.c.task_key == key)
    return (await conn.execute(q)).one()


async def test_breaker_opens_holds_and_closes(engine):
    sid = await new_session(engine)
    coord = await coordinator(engine, sid)
    async with engine.begin() as conn:
        await add_task(conn, sid, "discover_host", DiscoverInput(host="host-7"))
        await add_task(conn, sid, "verify_drift", VerifyInput(service="x", host="host-7"))
        for _ in range(3):  # three failed attempts in a row against host-7
            task = await task_row(conn, sid, "discover_host:host-7")
            await coord._retry_or_fail(conn, task, "server_error", "")
        breaker = await crud.current_fact(conn, sid, "resource:host-7", "breaker")
        assert breaker.value["state"] == "open"
        until = datetime.fromisoformat(breaker.value["open_until"])

        # The failed task's next round is the probe, and it may run when the
        # cooldown ends, while the other task for the host waits longer.
        probe = await task_row(conn, sid, "discover_host:host-7#2")
        held = await task_row(conn, sid, "verify_drift:x@host-7#1")
        assert abs((probe.not_before - until).total_seconds()) < 0.5
        assert held.not_before > probe.not_before

        await coord._breaker_success(conn, probe)
        breaker = await crud.current_fact(conn, sid, "resource:host-7", "breaker")
        assert breaker.value == {"state": "closed", "failures": 0, "open_until": None}
        held = await task_row(conn, sid, "verify_drift:x@host-7#1")
        assert held.not_before <= datetime.now(UTC)


async def test_reopen_gives_put_off_work_one_more_round(engine):
    sid = await new_session(engine)
    coord = await coordinator(engine, sid)
    tid = uuid7()
    async with engine.begin() as conn:
        await crud.upsert_fact(conn, sid, "host:host-9", F.UNREACHABLE, True, source_task_id=tid)
        await crud.upsert_fact(
            conn, sid, "service:cache@host-4", F.DRIFT, {"expected": 3, "actual": 1},
            status=F.INFERRED, evidence=[], source_task_id=tid,
        )  # fmt: skip
        await crud.upsert_fact(conn, sid, "service:search@host-3", F.REPLICAS, 2, source_task_id=tid)
        await crud.upsert_fact(conn, sid, "registry:search", F.EXPECTED, 2, source_task_id=tid)

        assert await AUDIT.reopen(coord, conn) == 3
        for key in (
            "discover_host:host-9#3",
            "verify_drift:cache@host-4#4",
            "compare_service:search@host-3#3",
        ):
            assert (await task_row(conn, sid, key)).status == "ready"
        # Those tasks are now active, and their keys exist, so nothing more re-opens.
        assert await AUDIT.reopen(coord, conn) == 0


async def test_a_discovery_too_big_for_one_attempt_is_split(engine):
    sid = await new_session(engine)
    coord = await coordinator(engine, sid)
    services = [f"s{i}" for i in range(20)]
    async with engine.begin() as conn:
        await add_task(conn, sid, "discover_host", DiscoverInput(host="host-13"))
        task = await task_row(conn, sid, "discover_host:host-13")
        response = {"host": "host-13", "services": services, "documents": ["runbook.md"]}
        await crud.log_event(
            conn, sid, "w", "tool_call", {"tool": "get_host", "path": "/hosts/host-13", "ok": True,
                                          "response": response}, task.id, task.attempt,
        )  # fmt: skip
        await coord._retry_or_fail(conn, task, "context_overflow", "the attempt needs 2100 tokens")

        # Three batches of services and one batch for the document.
        assert (await task_row(conn, sid, "discover_host:host-13")).status == "split"
        parts = [await task_row(conn, sid, f"discover_host:host-13/{n}of4") for n in (1, 2, 3, 4)]
        assert [p.input["services"] for p in parts] == [services[:8], services[8:16], services[16:], []]
        assert parts[3].input["documents"] == ["runbook.md"]

        # A batch may fetch what the too-big attempt already read, by pointer.
        packet = await build_packet(conn, AUDIT, await crud.get_session(conn, sid), parts[0])
        assert [(p.tool, p.path) for p in packet.pointers] == [("get_host", "/hosts/host-13")]

        # The document turns out to be too big on its own, so it is read a page at a time.
        doc = parts[3]
        page = {"host": "host-13", "name": "runbook.md", "page": 0, "pages": 3, "content": "..."}
        for tool, resp in (("get_host", response), ("fetch_document", page)):
            payload = {"tool": tool, "path": "-", "ok": True, "response": resp}
            await crud.log_event(conn, sid, "w", "tool_call", payload, doc.id, doc.attempt)
        await coord._retry_or_fail(conn, doc, "context_overflow", "the attempt needs 1900 tokens")
        pages = [await task_row(conn, sid, f"discover_host:host-13/4of4/p{n}") for n in (0, 1, 2)]
        assert [p.input["page"] for p in pages] == [0, 1, 2]


async def test_a_pointer_copy_counts_only_if_it_matches_its_original(engine):
    sid = await new_session(engine)
    coord = await coordinator(engine, sid)
    async with engine.begin() as conn:
        await add_task(conn, sid, "discover_host", DiscoverInput(host="host-2"))
        task = await task_row(conn, sid, "discover_host:host-2")
        read = {"host": "host-2", "service": "cache", "replicas": 3}
        path = "/hosts/host-2/services/cache"
        original = await crud.log_event(
            conn, sid, "w", "tool_call", {"tool": "get_service", "path": path, "ok": True, "response": read},
            task.id, 1,
        )  # fmt: skip
        await conn.execute(tasks.update().where(tasks.c.id == task.id).values(attempt=2))
        task = await task_row(conn, sid, "discover_host:host-2")

        def copy(response):
            return {"tool": "fetch_pointer", "path": path, "pointer": str(original), "of": "get_service",
                    "ok": True, "response": response}  # fmt: skip

        true_copy = await crud.log_event(conn, sid, "w", "tool_call", copy(read), task.id, 2)
        forged = await crud.log_event(conn, sid, "w", "tool_call", copy({**read, "replicas": 4}), task.id, 2)
        cited = await coord.cited_responses(conn, task)
        assert cited[str(true_copy)]["tool"] == "get_service"
        assert str(forged) not in cited


async def test_only_one_coordinator_per_session(engine, monkeypatch):
    sid = await new_session(engine)
    monkeypatch.setattr(coordinator_module, "LOCK_WAIT_SECONDS", 0.0)
    async with engine.connect() as holder:
        await holder.execution_options(isolation_level="AUTOCOMMIT")
        await holder.execute(
            text("SELECT pg_advisory_lock(hashtextextended(:k, 0))"), {"k": f"coordinator:{sid}:0"}
        )
        assert await Coordinator(engine, sid, quiet=True).run() == "locked"


async def test_find_all_is_done_only_when_everything_is_checked(engine):
    sid = await new_session(engine, goal_kind="all", start_points=["host-1"])
    coord = await coordinator(engine, sid)
    tid = uuid7()
    async with engine.begin() as conn:
        session = await crud.get_session(conn, sid)

        async def fact(subject, key, value, status=F.OBSERVED):
            await crud.upsert_fact(conn, sid, subject, key, value, status=status, source_task_id=tid)

        await fact("registry:cache", F.EXPECTED, 3)
        await fact("host:host-1", F.EXISTS, True)
        await fact("host:host-1", F.LISTING, {"services": ["cache"], "documents": ["runbook.md"]})
        await fact("doc:runbook.md@host-1", F.MENTIONS, ["host-2"])
        await fact("service:cache@host-1", F.REPLICAS, 3)
        await fact("service:cache@host-1", F.VERDICT, "match")
        assert not await everything_checked(coord, conn, session)  # host-2 not explored

        await fact("host:host-2", F.EXISTS, True)
        await fact("host:host-2", F.LISTING, {"services": ["cache", "search"], "documents": []})
        await fact("service:cache@host-2", F.REPLICAS, 1)
        await fact("service:cache@host-2", F.DRIFT, {"expected": 3, "actual": 1}, F.INFERRED)
        assert not await everything_checked(coord, conn, session)  # the drift is undecided

        drift = await crud.current_fact(conn, sid, "service:cache@host-2", F.DRIFT)
        await crud.update_fact(conn, sid, drift.id, status=F.VERIFIED)
        assert not await everything_checked(coord, conn, session)  # search was never read

        # Re-opening reads what is left of host-2.
        assert await AUDIT.reopen(coord, conn) == 1
        assert (await task_row(conn, sid, "discover_host:host-2/1of1#3")).input["services"] == ["search"]
        await fact("service:search@host-2", F.REPLICAS, 2)
        await fact("service:search@host-2", F.VERDICT, "match")
        assert await everything_checked(coord, conn, session)
