"""Context packets are built top-down within a token budget."""

import pytest

from lha.context import ContextOverflow, estimate_tokens, pack
from lha.schemas.context import EventView, FactView, Pinned

PINNED = Pinned(goal="find the drift", task_type="discover_host", task_key="k", attempt=1, input={})


def fact(i: int) -> FactView:
    return FactView(
        id=str(i), subject=f"service:s{i}@host-1", key="config.replicas", value=i, status="observed"
    )


def test_everything_fits_in_a_big_window():
    p = pack(PINNED, [fact(1)], [EventView(kind="decision", attempt=1, summary="x")], ["e1"], window=10_000)
    assert len(p.facts) == 1 and len(p.recent) == 1 and p.pointers == ["e1"]
    assert p.omitted == {}


def test_lowest_layers_are_cut_first():
    facts = [fact(i) for i in range(5)]
    budget = estimate_tokens(PINNED) + sum(estimate_tokens(f) for f in facts)
    p = pack(PINNED, facts, [EventView(kind="decision", attempt=1, summary="retry")], ["e1", "e2"],
             window=budget, reserve=0)  # fmt: skip
    assert len(p.facts) == 5  # facts fit exactly...
    assert p.recent == [] and p.pointers == []  # ...so recent and pointers are left out
    assert p.omitted == {"recent": 1, "pointers": 2}
    assert p.tokens <= budget


def test_strict_priority_once_full():
    """Once an item doesn't fit, nothing of lower priority sneaks in."""
    big = FactView(id="big", subject="x", key="k", value="v" * 400, status="observed")
    budget = estimate_tokens(PINNED) + 20
    p = pack(PINNED, [big], [], ["tiny"], window=budget, reserve=0)
    assert p.facts == [] and p.pointers == []
    assert p.omitted == {"facts": 1, "pointers": 1}


def test_pinned_is_never_truncated():
    with pytest.raises(ContextOverflow):
        pack(PINNED, [], [], [], window=5, reserve=0)
