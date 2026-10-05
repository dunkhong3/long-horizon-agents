"""The dashboard's API, read straight from the tables."""

import httpx

from lha.core.coordinator import Coordinator
from lha.dashboard import create_app
from lha.db import crud
from lha.domains.audit.tasks import DiscoverInput
from tests.conftest import add_task, new_session


async def test_session_view_counts_tasks_steps_and_decisions(engine):
    sid = await new_session(engine)
    coord = Coordinator(engine, sid)
    await coord.load()
    async with engine.begin() as conn:
        await add_task(conn, sid, "discover_host", DiscoverInput(host="host-1"))
        await crud.log_event(conn, sid, "w", "tool_call", {"tool": "get_host"})
        await coord.decide(conn, None, "replan", "a test decision")

    transport = httpx.ASGITransport(app=create_app(engine))
    async with httpx.AsyncClient(transport=transport, base_url="http://dash") as client:
        assert "lha dashboard" in (await client.get("/")).text
        assert any(s["id"] == str(sid) for s in (await client.get("/api/sessions")).json())
        view = (await client.get(f"/api/sessions/{sid}")).json()
        assert (await client.get("/api/sessions/01a10a87-0000-7000-8000-000000000000")).status_code == 404

    assert view["steps"] == 1
    assert view["tasks"] == {"discover_host": {"ready": 1}}
    assert view["timeline"][-1][1] == 1
    assert view["decisions"][0]["reason"] == "a test decision"
    assert view["progress"].startswith("step    1 | tasks: 0 done, 1 active")
