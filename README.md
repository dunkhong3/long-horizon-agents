# long-horizon-agents

[![ci](https://github.com/dunkhong3/long-horizon-agents/actions/workflows/ci.yml/badge.svg)](https://github.com/dunkhong3/long-horizon-agents/actions/workflows/ci.yml)

## What this is

This project is a small system where several agents, each running as its own process, work towards one goal over hundreds of dependent steps without losing track of what they are doing, and the only thing they share is a Postgres database. Each attempt at a task gets a small, fresh context instead of the whole history, faults and crashes are injected on purpose, and the system has to find its way back from all of them. The full design is in [docs/design.md](docs/design.md), and the decisions, what is still not built and how it was made are in [NOTES.md](NOTES.md).

## The task

The agents audit a mock cloud deployment, which is a set of fake servers (hosts) where each host runs some services, and each service runs a number of replicas, meaning copies of itself running at the same time. A file called the registry says how many replicas each service should have, a service that runs a different number has drifted, and the agents have to find the drift and write a report backed by verified evidence. The small example below shows the idea.

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

The agents start knowing only `host-1`, `host-2` and `host-3`, and the example is simplified. A real run has 20 hosts, between about 65 and 90 services, 3 decoys (healthy services whose first read is stale, so they look broken until they are read again) and a dead reference, which is a document that names a host that doesn't exist. One host runs 40 services, too many to read in one attempt's context, so its work has to be split, and another is down for the first round of work, so its circuit breaker opens and a later round has to bring it back. The default goal plants one drift 5–6 document hops away from the start (`lha/domains/audit/world.py`, lines 101–119), and `--goal all` plants three and only ends once every host has been explored and every service has a verdict. A run takes roughly 250–500 steps at the default fault rate, about 470–530 when finding all drifts, and up to about 660 at a 45% fault rate, and timeouts, 500 errors, empty responses and made-up model output all happen along the way, plus crashes of workers and of the coordinator with `--crashes`, and a crash of every process with `--kill-at`. Every run is scored against the planted answer as PASS or FAIL (`lha/core/scoring.py`, lines 31–43).

The audit is one domain, and the core that keeps the run coherent knows nothing about it. A second domain, a research brief, runs on the same core with `--domain research`, where the agents follow citations through a library of about 60 sources to find when some projects launched, and a year only counts once two or more sources agree on it and outnumber every other year, because some sources are outdated ('The research brief' in [docs/design.md](docs/design.md); `lha/domains/research/`).

## How it is built

```mermaid
flowchart TB
    S["Supervisor<br/>starts every process, restarts the ones that die"]
    C["Coordinator processes, one per partition of the plan<br/>plain code that checks, retries, splits and replans"]
    DB[("Postgres<br/>sessions · events · tasks · facts")]
    W["Agent worker processes with fake models<br/>discovery · analysis · reporter"]
    M["Mock network<br/>FastAPI with seeded faults"]
    S --> C
    S --> W
    C <-->|"decisions, facts, follow-up tasks"| DB
    W <-->|"claim a task, submit a result"| DB
    W -->|"tool calls"| M
```

Every process wakes up on Postgres `LISTEN/NOTIFY` instead of polling, and the only thing they share is the database.

The two things this project goes deep on are state and context management, and failure detection and recovery. For context, every attempt at a task gets a fresh `ContextPacket`, which is a small bundle with the goal, the task and only the facts that matter for it, built from the database within a token budget (`lha/core/context.py`, lines 51–87), and within that attempt the model sees only the packet and the results of its own tool calls, so the raw log never goes into a prompt, and a task whose own work doesn't fit is split into batches. For recovery, tool failures are never silent (`lha/core/tools.py`, lines 112–141), the tasks of crashed workers are handed out again, a host that keeps failing has its work held back by a circuit breaker, a coordinator that crashes is started again, work that was put off is re-opened when the run stalls, claims are verified before they count, and failed work is retried or rerouted and never passed downstream. Workers never write facts themselves, because the coordinator checks every claimed fact against the raw tool response it cites (`lha/domains/audit/rules.py`, lines 44–74), so a made-up number or host is rejected. The whole run can be killed at any point and `--resume` carries on from the database, and no API keys are needed because the agents use deterministic fake models through Pydantic AI's `FunctionModel` (`lha/core/agent.py`, lines 110–145).

We also measured the design against a naive agent whose prompt is its whole history, on the same worlds with the same faults (`lha/baseline.py`, `lha/bench.py`). At 60 hosts the system's prompts average about 330 tokens and never go over its 1600-token limit, while the naive agent's average about 7,300 and grow with every step, so a run sends about 11 times more tokens, and the naive agent names a decoy in every run unless it re-reads a mismatch, and even then, once its history has to fit a 2000-token window, it loses track of its work and runs out of steps in 8 runs out of 10. The numbers and their limits are in 'The benchmark' in [docs/design.md](docs/design.md).

At scale, a world of 200 hosts finishes the 'find all drifts' goal in about 21 s with 8+8 workers against about 41 s with 2+2, and splitting the plan between 4 coordinators cuts the time a result waits for its decision from about 300 ms to about 50 ms at the median (`lha/scale.py`, and 'Limits and scaling' in the design doc).

## How to run it

It needs Python 3.11 or newer, [uv](https://docs.astral.sh/uv/), [just](https://github.com/casey/just), and Postgres 16. `just db-up` starts Postgres in Docker on port 5433, not the usual 5432, so it doesn't clash with another Postgres already running on the machine, and the default `DATABASE_URL` is `postgresql+asyncpg://lha:lha@localhost:5433/lha` to match (`docker-compose.yml`; `lha/db/__init__.py`, line 6). To use a Postgres you already have instead, create a user and a database in it and point `DATABASE_URL` at them, for example in a `.env` file, which `just` loads automatically. Every run brings the database to the newest schema with Alembic migrations first, so a database from an older version is upgraded in place (`lha/db/migrate.py`). With only Docker installed, `docker compose up --build` builds the image, starts Postgres and runs one full run, and `just up` does the same. Many workers need many connections, so for `just scale` an existing Postgres needs `max_connections` of about 300, which the docker compose one already has.

```bash
psql -c "CREATE USER lha WITH PASSWORD 'lha';" -c "CREATE DATABASE lha OWNER lha;"
echo 'DATABASE_URL=postgresql+asyncpg://lha:lha@localhost:5432/lha' > .env
```

```bash
just up                               # Postgres and one full run, both in Docker
just install && just db-up
just test                             # unit tests + full runs + crashes + resume
just start --seed 42                  # full run, prints PASS/FAIL (~10 s)
just start --seed 42 --chaos 0.3      # more faults
just start --seed 42 --goal all       # find every drift, not just one
just start --seed 42 --crashes 0.05   # crash workers and the coordinator on purpose
just start --seed 7 --kill-at 120     # crash every process after step 120...
just start --resume                   # ...and resume from Postgres
just demo                             # a full run, then a crash at step 120 and a resume
just bench                            # the system against a naive full-history agent (~6 min)
just scale                            # one big world with more workers and coordinators (~3 min)
just start --domain research --goal all  # the second domain, a research brief
just start --seed 7 --hosts 200 --goal all --step-budget 50000 --coordinators 4 --workers 8,8
```

Without `just`, the same run is `uv sync && uv run python -m lha.run --seed 42`, but then `.env` is not loaded, so `DATABASE_URL` has to be exported in the shell if it isn't the default. A step is one model call or one tool call, and `--seed` fixes the network, the faults and the injected crashes, so the same seed gives the same faults and the same result. Each run writes `runs/<session>/finding.md` and `score.json`.

## Sample output

```
[supervisor] session <id>: audit, seed 42, size 20, fault rate 0.15, goal 'one', crash rate 0.0, 1 coordinator(s), workers 2+2, LISTEN/NOTIFY
[supervisor] step  245 | tasks: 42 done, 3 active, 1 failed | hosts found: 14 | drift claims: 0 inferred, 0 verified, 1 refuted
[supervisor] step  417 | tasks: 83 done, 14 active, 2 failed | hosts found: 16 | drift claims: 2 inferred, 0 verified, 1 refuted
[coordinator] goal met: a verified drift backed by the registry; cancelled 1 leftover task(s); writing report

# Finding: replica drift

- **oncall** on **host-12** runs **2** replica(s); the registry expects **5** (verified drift fact `<id>`).

  ✓ session succeeded
  ✓ found the drifted service (oncall)
  ✓ on the right host (host-12)
  ✓ right counts (expected 5, actual 2)
  ✓ every cited fact is verified

steps=476 model_calls=281 tool_calls=195 faults_injected=35 model_errors_injected=9 outputs_rejected=3 retries=11 replans=2 splits=1 breaker_opened=1 reopened=0 crashes_injected=0 pointer_fetches=38 leases_lost=0 repeated_reads=0 hosts_checked=19/20 decoys_refuted=3/3 drifts_planted=1 elapsed=10.8s
verdict=PASS  (session <id>, files in runs/<id>)
```
