"""The mock world is fully determined by its seed."""

from lha.domains.audit.world import START_HOSTS, WIDE_SERVICES, generate_world
from lha.faults import pick_fault, roll


def test_same_seed_same_world():
    a, b = generate_world(7, 20), generate_world(7, 20)
    assert a.services == b.services
    assert a.documents == b.documents
    assert a.decoys == b.decoys


def test_drift_is_the_single_deepest_host():
    for seed in range(20):
        w = generate_world(seed, 20)
        drift_depth = w.depth[w.drift.host]
        others = [d for h, d in w.depth.items() if h != w.drift.host]
        assert drift_depth > max(others)
        assert w.drift.actual != w.drift.expected
        assert w.drift.name not in w.decoys


def test_trouble_hosts_and_extra_drifts():
    for seed in range(20):
        w = generate_world(seed, 20, n_drifts=3)
        assert len(w.hosts[w.wide_host]) == WIDE_SERVICES
        assert w.wide_host != w.outage_host
        assert w.drift.host not in (w.wide_host, w.outage_host)
        assert "runbook.md" not in w.documents[w.outage_host]  # a leaf, so it hides no other host
        assert len({s.name for s in w.drifts}) == 3
        assert all(s.actual != s.expected for s in w.drifts)
        assert not set(w.drift_services) & set(w.decoys)


def test_every_hidden_host_is_mentioned_somewhere():
    w = generate_world(3, 20)
    text = " ".join(t for docs in w.documents.values() for t in docs.values())
    for host in w.hosts:
        if host not in START_HOSTS:
            assert host in text


def test_rolls_are_deterministic_and_spread():
    assert roll(1, "k", 1, 0) == roll(1, "k", 1, 0)
    assert roll(1, "k", 1, 0) != roll(1, "k", 2, 0)  # a retry gets a new roll
    faults = [pick_fault(roll(1, i), 0.2) for i in range(5000)]
    rate = sum(f is not None for f in faults) / len(faults)
    assert 0.17 < rate < 0.23
