"""The mock cloud deployment the agents audit, generated from a seed.

Nothing here is stored anywhere: the world service, a resumed run and the
scorer all call generate_world(seed, n_hosts) and get the exact same world.

Shape of a world:

- hosts host-1 .. host-N. The agents start knowing only host-1..host-3.
- every hidden host is mentioned in a document on another host, so the
  hosts form a tree rooted at the starting hosts.
- the drifted service sits on the single deepest host, at the end of a
  chain of documents that starts at one of the deepest *other* hosts.
  Discovery works breadth-first, so the chain can only be entered after
  most of the network has been explored.
- registry.json on host-1 lists the expected replica count of every service.
- a few healthy services are decoys: a discovery read returns a stale count.
- one document mentions a host that doesn't exist (a dead reference).
"""

import json
import random
from dataclasses import dataclass, field

START_HOSTS = ("host-1", "host-2", "host-3")
REGISTRY_HOST = "host-1"
REGISTRY_DOC = "registry.json"
CHAIN_LENGTH = 3  # extra hops below the deepest other host to reach the drift
N_DECOYS = 3

SERVICE_NAMES = (
    "payments", "search", "cache", "auth", "billing", "queue", "mailer",
    "metrics", "gateway", "images", "ledger", "profile", "inventory", "orders",
    "shipping", "pricing", "catalog", "reviews", "notify", "sessions",
    "analytics", "recommend", "upload", "thumbnail", "geo", "chat", "feed",
    "audit", "export", "scheduler", "webhooks", "ratelimit", "tokens", "config",
    "flags", "backup", "logs", "tracing", "dns", "proxy",
)  # fmt: skip


@dataclass(frozen=True)
class Service:
    name: str
    host: str
    expected: int  # what the registry says
    actual: int  # what is really running


@dataclass
class World:
    seed: int
    n_hosts: int
    hosts: dict[str, list[str]] = field(default_factory=dict)  # host -> service names
    services: dict[str, Service] = field(default_factory=dict)  # name -> service
    documents: dict[str, dict[str, str]] = field(default_factory=dict)  # host -> name -> text
    decoys: dict[str, int] = field(default_factory=dict)  # service -> stale count
    drift_service: str = ""
    depth: dict[str, int] = field(default_factory=dict)  # host -> hops from start

    @property
    def drift(self) -> Service:
        return self.services[self.drift_service]

    def registry(self) -> dict[str, int]:
        return {name: s.expected for name, s in sorted(self.services.items())}


def generate_world(seed: int, n_hosts: int = 20) -> World:
    if n_hosts < len(START_HOSTS) + CHAIN_LENGTH + 2:
        raise ValueError(f"need at least {len(START_HOSTS) + CHAIN_LENGTH + 2} hosts")
    rng = random.Random(f"world|{seed}|{n_hosts}")
    world = World(seed=seed, n_hosts=n_hosts)

    # --- the host tree --------------------------------------------------
    hidden = [f"host-{i}" for i in range(len(START_HOSTS) + 1, n_hosts + 1)]
    rng.shuffle(hidden)
    children: dict[str, list[str]] = {h: [] for h in (*START_HOSTS, *hidden)}
    for h in START_HOSTS:
        world.depth[h] = 0

    # First the bulk of the network: every other hidden host hangs off a
    # host at depth <= 2, so these hosts are 1-3 hops from the start.
    chain, others = hidden[:CHAIN_LENGTH], hidden[CHAIN_LENGTH:]
    for host in others:
        candidates = [h for h, d in world.depth.items() if d <= 2]
        parent = rng.choice(candidates)
        children[parent].append(host)
        world.depth[host] = world.depth[parent] + 1

    # Then the chain that leads to the drift. It starts at one of the deepest
    # hosts above, so breadth-first discovery reaches it last:
    #   deepest host -> c1 -> c2 -> drift host
    deepest = max(world.depth.values())
    parent = rng.choice(sorted(h for h, d in world.depth.items() if d == deepest))
    for host in chain:
        children[parent].append(host)
        world.depth[host] = world.depth[parent] + 1
        parent = host
    drift_host = chain[-1]

    # --- services -------------------------------------------------------
    names = list(SERVICE_NAMES)
    rng.shuffle(names)
    all_hosts = [*START_HOSTS, *sorted(hidden, key=_host_number)]
    for host in all_hosts:
        count = rng.randint(1, 3)
        world.hosts[host] = []
        for _ in range(count):
            name = names.pop() if names else f"svc-{len(world.services)}"
            expected = rng.randint(2, 5)
            world.services[name] = Service(name, host, expected, expected)
            world.hosts[host].append(name)

    # The drift: one service on the deepest host runs the wrong count.
    drift_name = rng.choice(world.hosts[drift_host])
    s = world.services[drift_name]
    wrong = rng.choice([n for n in range(1, s.expected + 3) if n != s.expected])
    world.services[drift_name] = Service(s.name, s.host, s.expected, wrong)
    world.drift_service = drift_name

    # Decoys: healthy services whose first (discovery) read is stale.
    healthy = sorted(n for n in world.services if n != drift_name)
    for name in rng.sample(healthy, N_DECOYS):
        expected = world.services[name].expected
        world.decoys[name] = rng.choice([n for n in range(1, expected + 3) if n != expected])

    # --- documents ------------------------------------------------------
    for host in all_hosts:
        world.documents[host] = {}
        kids = children[host]
        if kids:
            world.documents[host]["runbook.md"] = _runbook(rng, host, world.hosts[host], kids)
        elif rng.random() < 0.5:
            world.documents[host]["notes.md"] = f"Notes for {host}: nothing unusual this week."
    world.documents[REGISTRY_HOST][REGISTRY_DOC] = json.dumps(world.registry(), indent=2)

    # A dead reference: a document names a host that doesn't exist.
    dead = f"host-{n_hosts + rng.randint(10, 99)}"
    target = rng.choice([h for h in all_hosts if h not in (*chain, REGISTRY_HOST)])
    world.documents[target]["migration.md"] = f"{dead} was decommissioned last quarter."
    return world


def _runbook(rng: random.Random, host: str, services: list[str], kids: list[str]) -> str:
    lines = [f"# Runbook for {host}", ""]
    for kid in kids:
        service = rng.choice(services)
        template = rng.choice(
            [
                "The {s} service sends traffic to {k}.",
                "If {s} is slow, check {k} first.",
                "{k} holds the backups for {s}.",
            ]
        )
        lines.append("- " + template.format(s=service, k=kid))
    return "\n".join(lines)


def _host_number(host: str) -> int:
    return int(host.split("-")[1])
