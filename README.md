# long-horizon-agents

Multiple agents, running as separate processes, work toward one goal over hundreds
of dependent steps without losing coherence. They share state only through
Postgres. Each step gets a small, fresh context. Faults are injected randomly on
purpose, and the system has to recover from them.

## The task

Agents audit a mock cloud deployment: a set of fake servers (**hosts**), each
running some **services**. A **registry** says how many **replicas** (running
copies) each service should have. Exactly one service is running a different
number of replicas. The agents must find it and write a report backed by
verified evidence.

```
THE MOCK NETWORK  (fake servers the agents explore)

host-1  └─ file registry.json         the "should be" list:
                                      payments 3 · search 2 · cache 3
host-2  └─ payments: 3 replicas       ✓ matches
host-3  ├─ search: "1 replica"        ? mismatched because the first answer
        │                             was stale (out of date)
        │  ask again → 2 replicas     ✓ matches, it was a decoy
        └─ file runbook.md:           the only clue that host-4 exists
           "cache runs on host-4"
host-4  └─ cache: 1 replica           ✗ registry says 3 → this is the drift
           ask again → 1 replica      ✓ confirmed

report: "cache on host-4 runs 1 replica; the registry expects 3"
```

The agents start knowing only `host-1`, `host-2` and `host-3`.

A real run has ~20 hosts and ~200–300 steps, with timeouts, 500s, empty
responses, crashed workers and a killed coordinator along the way. Every run is
scored against the planted answer: **PASS / FAIL**.

## Architecture

```
          Coordinator (plain code: plan, validate, retry, replan)
                              │
            Postgres tables: sessions · events · tasks · facts
                 ▲            ▲             ▲
           Discovery      Analysis      Reporter      ← agent worker processes,
               │              │                         fake LLMs
               └──── Mock network (FastAPI, randomly seeded faults)
```

- **Context:** every model call gets a fresh `ContextPacket` built from the database. The raw log never goes into a prompt.
- **Recovery:** tool failures are explicit, crashed workers' tasks are reclaimed, claims are verified before they count, and the plan is revised when progress stalls.
- **Resume:** kill the coordinator at any point, and `--resume` continues from the database.
- **No API keys:** agents use deterministic fake models (Pydantic AI `FunctionModel`).

Full design: [docs/design.md](docs/design.md).

## Run

Requires Python 3.11+, [uv](https://docs.astral.sh/uv/), [just](https://github.com/casey/just), and Postgres 16 (Docker or local; set `DATABASE_URL`).

```bash
just install && just db-up
just test
just start --seed 42                  # full run, prints PASS/FAIL
just start --seed 42 --chaos 0.3      # more faults
just start --seed 42 --kill-at 120    # kill the coordinator after step 120...
just start --seed 42 --resume         # ...and resume
```

A **step** is one model call or one tool call. `--seed` fixes the network and
the fault pattern, so the same seed gives the same run.
