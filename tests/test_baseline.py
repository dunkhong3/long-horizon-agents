"""The naive baseline sees only what fits in its window and trusts what it reads."""

import json

from pydantic_ai.messages import ModelRequest, ToolReturnPart, UserPromptPart

from lha.baseline import Next, policy, read_view, run_baseline, visible
from lha.core.context import estimate_tokens
from lha.run import start_world


def ret(tool: str, content: dict) -> ToolReturnPart:
    return ToolReturnPart(tool_name=tool, content=content, tool_call_id="x")


GOAL = UserPromptPart(content="You start knowing only these hosts: host-1, host-2, host-3.")
HOST_1 = ret("get_host", {"host": "host-1", "services": ["cache"], "documents": ["registry.json"],
                          "args": {"host": "host-1"}})  # fmt: skip
REGISTRY = ret("fetch_document", {"host": "host-1", "name": "registry.json", "page": 0, "pages": 1,
                                  "content": json.dumps({"cache": 3}, indent=2),
                                  "args": {"host": "host-1", "name": "registry.json",
                                           "page": 0}})  # fmt: skip
STALE = ret("get_service", {"host": "host-1", "service": "cache", "replicas": 1,
                            "args": {"host": "host-1", "service": "cache"}})  # fmt: skip


def test_window_keeps_the_goal_and_the_newest_parts():
    parts = [GOAL, HOST_1, REGISTRY, STALE]
    assert visible([ModelRequest(parts=parts)], 0) == parts
    room = estimate_tokens(GOAL.content) + estimate_tokens(STALE.content)
    assert visible([ModelRequest(parts=parts)], room) == [GOAL, STALE]


def test_naive_believes_the_first_mismatch():
    view = read_view([GOAL, HOST_1, REGISTRY, STALE])
    assert policy(view, reread=False)["service"] == "cache"


def test_reread_checks_a_mismatch_first():
    view = read_view([GOAL, HOST_1, REGISTRY, STALE])
    nxt = policy(view, reread=True)
    assert isinstance(nxt, Next) and nxt.args == {"host": "host-1", "service": "cache", "recheck": True}


def test_forgetting_the_registry_means_reading_it_again():
    """With the registry out of the window, the model can't compare and goes back for it."""
    view = read_view([GOAL, STALE])
    assert policy(view, reread=True) == Next("get_host", {"host": "host-1"})


async def test_full_baseline_runs():
    world = await start_world("audit", 42, 20, 0.15)
    try:
        naive = await run_baseline(world.url, 42, 20, window=0, reread=False, step_budget=3000)
        careful = await run_baseline(world.url, 42, 20, window=0, reread=True, step_budget=3000)
    finally:
        await world.stop()
    assert not naive["passed"]  # it reports the first decoy it reads
    assert careful["passed"]
    # The whole history is the prompt, so by the end it is far bigger than a packet.
    assert careful["stats"]["prompt_tokens_max"] > 3000
