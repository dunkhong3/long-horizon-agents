"""Tools never fail silently, and faults are retried with a fresh roll."""

import httpx
import pytest

from lha.tools import NotFound, ToolBox, ToolFailure
from lha.world.app import create_app
from lha.world.model import generate_world


def toolbox(fault_rate: float, log: list, task_key: str = "discover_host:host-1") -> ToolBox:
    app = create_app(generate_world(1, 20), fault_rate)
    http = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://world")

    async def record(kind, payload):
        log.append(payload)
        return f"event-{len(log)}"

    return ToolBox(http, task_key, 1, record)


async def test_successful_call_returns_data_and_event_id():
    log = []
    data = await toolbox(0.0, log).get_host("host-1")
    assert data["host"] == "host-1" and data["event_id"] == "event-1"
    assert log[0]["ok"] is True


async def test_404_is_permanent_and_not_retried():
    log = []
    with pytest.raises(NotFound):
        await toolbox(0.0, log).get_host("host-999")
    assert len(log) == 1 and log[0]["error"] == "not_found"


async def test_every_fault_is_logged_and_retried_then_raised():
    from lha.faults import pick_fault, roll

    # In-process (ASGI) transport doesn't enforce client timeouts, so pick a
    # task key whose first three rolls are faults other than "timeout".
    key = next(
        f"discover_host:host-{i}"
        for i in range(1000)
        if all(pick_fault(roll(1, f"discover_host:host-{i}", 1, n), 1.0) != "timeout" for n in range(3))
    )
    log = []
    with pytest.raises(ToolFailure) as err:
        await toolbox(1.0, log, key).get_host("host-1")  # every call faults
    assert err.value.kind in {"server_error", "rate_limited", "timeout", "empty_response"}
    assert [p["call_no"] for p in log] == [0, 1, 2]  # each retry is a new call number
    assert all(p["ok"] is False for p in log)


async def test_empty_200_is_a_failure():
    """Find a call number whose roll is an empty response, and check it fails."""
    from lha.faults import pick_fault, roll

    call_no = next(
        n for n in range(500) if pick_fault(roll(1, "discover_host:host-1", 1, n), 1.0) == "empty_response"
    )
    tb = toolbox(1.0, [])
    with pytest.raises(ToolFailure) as err:
        await tb._request("/hosts/host-1", call_no, ("host",))
    assert err.value.kind == "empty_response"


async def test_fetch_pointer_copies_an_earlier_read_without_the_network():
    log = []
    original = {"tool": "get_host", "path": "/hosts/host-1", "response": {"host": "host-1"}}

    async def load(event_id):
        return original if event_id == "e-1" else None

    tb = toolbox(1.0, log)  # every network call would fault
    tb.load = load
    data = await tb.fetch_pointer("e-1")
    assert data["host"] == "host-1" and data["_of"] == "get_host"
    assert (
        log[0]["tool"] == "fetch_pointer"
        and log[0]["pointer"] == "e-1"
        and log[0]["response"] == {"host": "host-1"}
    )
    with pytest.raises(ToolFailure):
        await tb.fetch_pointer("e-2")
