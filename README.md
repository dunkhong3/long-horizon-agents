# long-horizon-agents

[![ci](https://github.com/dunkhong3/long-horizon-agents/actions/workflows/ci.yml/badge.svg)](https://github.com/dunkhong3/long-horizon-agents/actions/workflows/ci.yml)

## What this is

This project is a small system where several agents, each running as its own process, work towards one goal over hundreds of dependent steps without losing track of what they are doing, and the only thing they share is a Postgres database. Each step gets a small, fresh context instead of the whole history, faults are injected randomly on purpose, and the system has to find its way back from all of them. The full design is in [docs/design.md](docs/design.md), and the decisions, the parts we cut and what comes next are in [NOTES.md](NOTES.md).

## The task

The agents audit a mock cloud deployment, which is a set of fake servers (hosts) where each host runs some services, and each service runs a number of replicas, meaning copies of itself running at the same time. A file called the registry says how many replicas each service should have, exactly one service is running a different number, and the agents have to find that service and write a report backed by verified evidence. The small example below shows the idea.

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

The agents start knowing only `host-1`, `host-2` and `host-3`, and the example is simplified. A real run has 20 hosts, about 35 services, 3 decoys (healthy services whose first read is stale, so they look broken until they are read again) and a dead reference, which is a document that names a host that doesn't exist. The drift sits 5–6 document hops away from the start (`lha/world/model.py`, lines 77–95), so a run takes about 250 steps, and timeouts, 500 errors, empty responses, made-up model output and crashes all happen along the way. Every run is scored against the planted answer as PASS or FAIL (`lha/scoring.py`, lines 30–52).

## How it is built

```
          Coordinator (plain code: plan, validate, retry, replan)
                              │
            Postgres tables: sessions · events · tasks · facts
                 ▲            ▲             ▲
           Discovery      Analysis      Reporter      ← agent worker processes,
               │              │                         fake LLMs
               └──── Mock network (FastAPI, randomly seeded faults)
```

The two things this project goes deep on are state and context management, and failure detection and recovery. For context, every model call gets a fresh `ContextPacket`, which is a small bundle with the goal, the task and only the facts that matter for it, built from the database (`lha/context.py`, lines 81–94), and the raw log never goes into a prompt. For recovery, tool failures are never silent (`lha/tools.py`, lines 91–120), the tasks of crashed workers are handed out again, claims are verified before they count, and failed work is retried or rerouted and never passed downstream. Workers never write facts themselves, because the coordinator checks every claimed fact against the raw tool response it cites (`lha/coordinator.py`, lines 167–185), so a made-up number or host is rejected. The whole run can be killed at any point and `--resume` carries on from the database, and no API keys are needed because the agents use deterministic fake models through Pydantic AI's `FunctionModel` (`lha/agents/base.py`, lines 82–111).

## How to run it

It needs Python 3.11 or newer, [uv](https://docs.astral.sh/uv/), [just](https://github.com/casey/just), and Postgres 16, either through Docker or a local install, with `DATABASE_URL` pointing at it (the default is `postgresql+asyncpg://lha:lha@localhost:5432/lha`).

```bash
just install && just db-up
just test                             # unit tests + full runs + crash/resume
just start --seed 42                  # full run, prints PASS/FAIL (~10 s)
just start --seed 42 --chaos 0.3      # more faults
just start --seed 7 --kill-at 120     # crash every process after step 120...
just start --resume                   # ...and resume from Postgres
just demo                             # all of the above in one go
```

Without `just`, the same run is `uv sync && uv run python -m lha.run --seed 42`. A step is one model call or one tool call, and `--seed` fixes both the network and the fault pattern, so the same seed gives the same faults and the same result. Each run writes `runs/<session>/finding.md` and `score.json`.

## Sample output

```
[supervisor] session <id>: seed 42, 20 hosts, fault rate 0.15
[coordinator] step  190 | tasks: 45 done, 5 active, 1 failed | hosts found: 15 | drift claims: 0 inferred, 0 verified, 2 refuted
[coordinator] step  215 | tasks: 54 done, 2 active, 1 failed | hosts found: 17 | drift claims: 0 inferred, 0 verified, 3 refuted
[coordinator] goal met: a verified drift backed by the registry; cancelled 0 leftover task(s); writing report

# Finding: replica drift

**dns** on **host-12** runs **3** replica(s); the registry expects **5**.

  [x] session succeeded
  [x] found the drifted service (dns)
  [x] on the right host (host-12)
  [x] right counts (expected 5, actual 3)
  [x] cited drift fact is verified

steps=255 tool_calls=100 model_calls=155 faults_injected=16 model_errors=5 rejected=2 retries=5 replans=1 hosts=20/20 decoys_refuted=3/3 elapsed=7.6s
verdict=PASS
```
