# Design

Design notes. README has the short version. NOTES.md (written when v1 is
done) is the retrospective: what was cut, what's next, how it was built.

## The goal the agents pursue

**Audit a mock cloud deployment for replica drift and write a verified finding.**

### Glossary

| Term | Plain meaning |
|---|---|
| **Host** | A fake server in the mock network (`host-1`, `host-2`, …). Hosts are what the agents explore. |
| **Service** | A program running on a host (`payments`, `search`, `cache`). |
| **Replicas** | How many copies of a service are running, like pods in Kubernetes. |
| **Registry** | One file (`registry.json`) on one host, listing how many replicas each service *should* have. The "source of truth" the agents compare against. |
| **Drift** | A service running a different number of replicas than the registry says. |
| **Document** | A text file on a host (e.g. `runbook.md`, a team's operations notes). Documents can mention other hosts. That is how hidden hosts are found. |
| **Stale read** | The mock server sometimes answers with an old, out-of-date value (like a cache that hasn't refreshed). |
| **Fresh read** | Asking the same question again with a new call. |
| **Decoy** | A service that *looks* drifted because of a stale read, but matches on a fresh read. |

### Example

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
        evidence: fact #12 (registry), reads in events #41 and #57
```

The agents start knowing only `host-1`, `host-2` and `host-3`. A real run has
~20 hosts.

Why it is long-horizon:
- **Multi-hop.** The drifted service sits on a host that is not in the starting host list. It is only referenced from a document on another host.
- **Decoys.** Some services *look* drifted on first read (a stale cached response) but match the registry on re-check. A claim only counts once a second, independent read confirms it.
- **Faults everywhere.** Tool calls time out, return 500s, or return an empty 200 ("silent success"). Workers crash. The coordinator gets killed and resumed.
- Roughly 200–300 steps per run: discovery calls, analysis, verification, retries.

The goal is **not** "scan the entire network". Coverage is a metric, not the
success criterion. A run that finds and verifies the drift early is a good run.

**Any vs. all.** The goal is "find *the* drift", and exactly one is planted, so
finding one verified drift ends the run. If the world planted several, the goal
would have to become "check every reachable service", and done would mean
"no unexplored hosts left and every service has a verdict". That gives a
longer, more predictable horizon, and it's the natural next step.

### Is this the only task the system can run?

No. The core (ledger, task DAG, context packets, coordinator, recovery) knows
nothing about networks. The domain lives in four swappable pieces: the mock
world, the tools, each agent's fake model, and the goal criteria. A
research-brief goal would swap those four and keep the core.

## The coordinator, in plain words

The coordinator is an ordinary loop, not an AI. Think of a project manager
with a to-do board (the `tasks` table) and a notebook of findings (the `facts`
table):

```
loop:
  1. look at tasks workers just finished
       - output invalid?          → reject it, put the task back (retry)
       - output valid?            → save its facts, mark the task done
  2. plan follow-ups from new facts
       - new host found           → "discover host X"
       - service read             → "compare service Y with the registry"
       - mismatch found           → "verify service Y" (read it again)
  3. handle trouble
       - task failed too often    → give up on it, try another route
       - worker went silent       → put its task back on the board
       - nothing moved for a while → replan
  4. check the goal: is there a verified drift?
       - yes → create the reporter task, cancel all leftover tasks
  5. stop if the report is written or the step budget is used up
```

Workers never decide what happens next. They pick a task off the board, do
it, and hand back a result. The coordinator decides what that result means.

## Roles

| Role | Does | Input | Output |
|---|---|---|---|
| **Coordinator** | Plain code, not an LLM. Owns the plan. Validates and commits worker output, unlocks dependent tasks, retries, replans, detects stalls, decides when the goal is met. | Ledger | Tasks, plan events |
| **Discovery** | Breadth. Lists hosts, reads host and service details and documents, records what exists. | A host or host list | Facts: host exists, service X runs N replicas, doc Z mentions host H |
| **Analysis** | Depth. Compares a service's replica count with the registry, raises a drift hypothesis. A verify task re-reads from a fresh call and confirms or refutes it. | A service plus its registry entry | Facts: `inferred` drift → `verified` / `refuted` |
| **Reporter** | Runs once at the end. Writes the finding from **verified** facts only, citing fact IDs. | Verified facts | `finding.md` |

Several worker processes run in parallel (e.g. 2 discovery, 1 analysis,
1 reporter). Workers never talk to each other. They share findings only
through the fact table. Each task's prompt is built from the **latest** facts
at the moment a worker claims it, so nobody misses anyone else's findings.

### Task types

Every task has a **type**, and each type belongs to exactly one role. A worker
only claims tasks of its own role (`WHERE role = :role`). Each type has its own
input and output schema.

| Task type | Role | Input | Output |
|---|---|---|---|
| `discover_host` | Discovery | a host name | facts: services + replica counts, documents, hosts mentioned |
| `compare_service` | Analysis | a service + its registry entry | a match, or an `inferred` drift fact |
| `verify_drift` | Analysis | an `inferred` drift fact | a fresh read: agrees or disagrees |
| `write_report` | Reporter | all verified facts | `finding.md` |

## Verification ("checked twice")

1. An analysis task reads a service and compares it with the registry. A mismatch becomes an **`inferred`** drift fact. It doesn't count yet.
2. The coordinator creates a **verify** task. Any analysis worker can pick it up. It reads the service again with a fresh call.
3. **Two reads must agree.** If the fresh read agrees with the first, the drift becomes **`verified`**. If they disagree, neither read is trusted; a third read breaks the tie (2 out of 3). If the majority matches the registry, the drift becomes **`refuted`** (a decoy).

The second read is **not** automatically the truth. It could be stale or
broken too. What counts is agreement between independent reads.

A read that *matches* the registry is accepted after one read. Stale reads in
the mock world only ever make a service look drifted, never hide a real drift.
That's a simplification, noted as a cut.

Only verified facts can satisfy the goal or appear in the report. This is what
stops one bad read (a stale cache, a silently broken tool, a malformed model
output) from ending up as the answer.

## Storage (Postgres)

| Table | Purpose | Notes |
|---|---|---|
| `sessions` | One row per long-running task (a "run"): goal, seed, status, budgets | Every other table has `session_id` |
| `events` | Append-only log of everything: LLM input/output, tool calls, errors, plan changes | Source of truth. Never put into prompts wholesale |
| `tasks` | Work queue and plan DAG: role, input, `depends_on`, status, lease, attempts | |
| `facts` | Structured findings with provenance (task, tool call) and status | Deduplicated by `(subject, key)` |

### Task ordering (a DAG, not a sorted queue)

- Tasks are added during the run, so the queue can't be pre-sorted.
- Each task lists `depends_on`. It becomes `ready` only when all its dependencies have `succeeded`.
- **The coordinator flips tasks to `ready`.** A task with no dependencies is created as `ready`. Every time the coordinator commits a successful task, it runs one statement in its loop:

  ```sql
  UPDATE tasks t SET status = 'ready'
   WHERE t.session_id = :sid AND t.status = 'pending'
     AND NOT EXISTS (
       SELECT 1 FROM tasks d
        WHERE d.id = ANY (t.depends_on) AND d.status <> 'succeeded');
  ```

  If a dependency fails for good, its dependents are `cancelled`, because failed work never flows downstream.
- Workers claim ready tasks with `SELECT … FOR UPDATE SKIP LOCKED`. The topological order emerges as the run goes.
- `SKIP LOCKED` is Postgres's job-queue primitive. `FOR UPDATE` locks the row a worker picked. `SKIP LOCKED` makes every other worker skip rows that are already locked, instead of waiting for them. Two workers never get the same task, and nobody blocks.
- Status flow: `pending → ready → leased → succeeded | failed | cancelled`.

```sql
-- claim one ready task (inside a transaction)
BEGIN;
SELECT id FROM tasks
 WHERE session_id = :sid AND status = 'ready' AND role = :role
 ORDER BY id
 LIMIT 1
 FOR UPDATE SKIP LOCKED;          -- lock it; skip rows others have locked
UPDATE tasks SET status = 'leased', leased_by = :worker,
       lease_expires_at = now() + interval '30 seconds'
 WHERE id = :task_id;
COMMIT;                           -- the row lock is released here
```

There is no explicit "unlock". A row lock lasts until the transaction ends.
The *lease* (status `leased` plus `lease_expires_at`) is what keeps the task
claimed after that.

**Heartbeat.** While a worker runs a task, a background asyncio task extends
the lease every 10 s (shorter than the 30 s lease, so one late beat isn't
fatal):

```sql
UPDATE tasks SET lease_expires_at = now() + interval '30 seconds'
 WHERE id = :task_id AND leased_by = :worker AND attempt = :attempt
   AND status = 'leased';
```

- **Worker dies:** the heartbeats stop and the lease expires. The coordinator sets the task back to `ready` with `attempt + 1`.
- **Worker was only slow:** its next heartbeat or result submission matches 0 rows, because the `attempt` changed. The worker drops its result. This stops a late "zombie" worker from overwriting the retry's work.

### Facts

Raw tool output goes into `events` in full (that's what pointers refer to).
The tool's validator then extracts zero or more **facts** from it. A fact is one
small, typed claim.

| Column | Example |
|---|---|
| `id` | `41` |
| `session_id` | `3f2b9c1e-…` (UUID of the session) |
| `subject` | `service:cache@host-4` |
| `key` | `config.replicas` |
| `value` (jsonb) | `1` |
| `status` | `observed` · `inferred` · `verified` · `refuted` · `superseded` |
| `source_task_id`, `source_event_id` | which task and which tool call produced it |
| `evidence` (jsonb) | event IDs of the reads backing it (for drift facts) |
| `created_at` | `2026-10-03 14:02:11+00` |

Two kinds of facts per service:
- **A read:** `key = config.replicas`, `value = 1`, status `observed`.
- **A drift claim:** `key = drift.replicas`, `value = {"expected": 3, "actual": 1}`, status `inferred → verified | refuted`.

**Facts are never deleted.** A fact's `status` can change (`inferred →
verified`, or `→ superseded`), and every change is also written to `events`,
so the full history is always there. (`events` is the strictly append-only
table.)

**Current facts** = `status <> 'superseded'`. `refuted` facts stay visible:
"we checked this and it was wrong" is useful knowledge.

**Same fact = same `(session_id, subject, key)`.** A partial unique index
lets the database itself guarantee at most one current fact per key:

```sql
CREATE UNIQUE INDEX facts_one_current
    ON facts (session_id, subject, key)
 WHERE status <> 'superseded';
```

The index is a **rule, not storage**. It doesn't keep, sort or return rows.
It makes Postgres refuse any write that would leave two current rows for the
same key. So reading the current fact needs no `ORDER BY created_at DESC
LIMIT 1`. This query returns at most one row, and the index makes it fast:

```sql
SELECT * FROM facts
 WHERE session_id = :sid AND subject = :subject AND key = :key
   AND status <> 'superseded';
```

Example. Discovery's first read of `search` hit a stale answer (1); a later
fresh read says 2:

| id | subject | key | value | status |
|---|---|---|---|---|
| 1 | `service:search@host-3` | `config.replicas` | `1` | `superseded` |
| 2 | `service:search@host-3` | `config.replicas` | `2` | `observed` ← current |

- Inserting another `superseded` row for this key is allowed.
- Inserting another `observed` row while row 2 is current fails with `duplicate key value violates unique constraint "facts_one_current"`.
- So replacing a fact is one transaction: mark the old row `superseded`, then insert the new one.

On write:
- **Same value:** no-op, so retries are idempotent.
- **Different value:** the old fact is marked `superseded` and the new one is inserted.
- **New value contradicts a `verified` fact:** nothing is silently replaced. The coordinator creates a verify task.

### Relevance

When the coordinator creates a task it sets its **scope**: the subjects the
task is about, e.g. `host:host-3` and its services. The context builder
selects facts whose subject is in scope, plus the registry entries for those
services. It's a plain SQL filter, with no embeddings or similarity search.

### Growth vs. context

- The tables grow, and that's fine: it's storage, not prompt. Postgres handles millions of rows.
- Facts grow with the **size of the world**, not the number of steps. A newer fact with the same `(subject, key)` supersedes the old one.
- Retention and archiving are out of scope for this project.

### Steps

A **step** is one model call or one tool call. Budgets and `--kill-at N`
(kill the coordinator after step N, to test resume) count steps.

## Context packets

A `ContextPacket` is a Pydantic model built in memory right before one LLM
call, from a fresh database query:

1. **Pinned.** Goal, done criteria, this task's spec. Always included.
2. **Facts.** Only facts relevant to this task (e.g. this host or service).
3. **Recent.** The last few events of *this* task (e.g. the previous attempt's error).
4. **Pointers.** IDs of raw tool outputs, fetchable on demand. The payloads themselves are left out.

The four layers are listed from highest to lowest priority, and each has a
token budget. When the packet is over budget, cut from the bottom up:
**pointers** first, then the oldest **recent** events, then the least relevant
**facts**. **Pinned** is never cut.

A packet is used for one call only, then discarded. A copy is logged to
`events`, so we can always see exactly what an agent saw, but the next call
never starts from it. It builds a fresh packet, because:
- other workers have added facts since then;
- the next call is usually a different task, with a different scope;
- if the old packet held a bad or stale fact, rebuilding from the database picks up the correction. Reusing the packet would carry the error forward.

On a retry, the new packet's *recent* layer includes the previous attempt's
error. The prompt therefore scales with the **task**, not with the run's history.

Compaction happens on the prompt, never on the log:
- **Write time.** Raw tool output is turned into facts.
- **Read time.** A relevance query plus budgets.
- **Summaries.** Any summary (the coordinator's situation report) is recomputed by code from the facts. Never summarize a summary.

## Retries and replanning

Plain rules in the coordinator, applied in order.

**1. Classify every failure.**
- **Retryable:** timeout, 500, 429, empty 200, invalid model output, expired lease. The task goes back to `ready` after a backoff (`not_before = now() + 2^attempt s`).
- **Permanent:** 404 (the host or service doesn't exist). The task fails right away; retrying won't help.

**2. Retry budget.** Each task gets `max_attempts = 3`. After that it is
`failed`, and its dependents are `cancelled`.

**3. Replan after a failure.** A fixed table per task type:

| Failed task | What the coordinator does |
|---|---|
| `discover_host` | Record a `host unreachable` fact. Re-queue it once more later, after other work. |
| `compare_service` | Re-read the service (new `discover_host`), then compare again. |
| `verify_drift` | Schedule another verify later. The drift stays `inferred`, so it can't count yet. |
| `write_report` | Retry. If it keeps failing, the session fails, and the verified facts are still in the database. |

**4. Circuit breaker (per host).** After 3 failures in a row against the same
host, the coordinator stops creating tasks for that host for a cooldown period
(the breaker is "open"). Then it lets one probe task through. Success closes
the breaker; failure re-opens it. This stops the run from wasting its whole
budget retrying one broken host.

**5. Stall.** No task succeeded in the last K steps and nothing is ready →
re-open what was deferred (hosts whose cooldown ended, drifts still
`inferred`). If there is nothing to re-open → the session fails, and the
reporter writes a partial report.

Every replan decision is written to `events` with its reason.

## Session lifecycle

The coordinator checks the goal **after each task commit** (and on a timer),
not after each model call. The check is plain code over facts: *is there a
`verified` drift fact backed by a registry fact?*

- **Running.** The coordinator keeps adding tasks as new facts unlock them (a new host → read its services).
- **Goal met.** The coordinator creates the **reporter** task and cancels leftover tasks. The reporter runs only now, reading only verified facts.
- **Succeeded.** The report is written.
- **Stalled.** No ready tasks and no progress for K steps → replan (retry a different route, re-verify). If replanning produces nothing → `failed`.
- **Failed.** Step or time budget exhausted. The reporter still runs and writes a partial report from what was verified.

## Fake LLMs

No API keys, no spend. Each agent's model is a deterministic Python function,
plugged in through Pydantic AI `FunctionModel`. The agent loop, tool calling,
and output validation are real; only the "brain" is scripted.

- **It reacts to its input.** It is a rule-based policy (e.g. "read every service of my host not yet in facts"), not a fixed tape, so it adapts when tools fail.
- **Two error sources:**
  1. **Tool errors** come from the mock world. The agent sees them and reports them.
  2. **Model errors.** The fake model sometimes emits malformed output on purpose (wrong schema, a host that doesn't exist). Validation rejects it before it reaches the ledger, and the task is retried.

## Determinism

Faults are **pseudo-random but seeded**. The same seed gives the same run,
and different seeds give different fault patterns.

Each fault decision is derived from `hash(seed, task_id, attempt)`, not from
one shared random generator. So it doesn't depend on which worker runs first,
and a retry (new `attempt`) can succeed where the first try failed.

## Mock world (FastAPI)

The mock network is its own FastAPI service. Tools make real HTTP calls to it,
so this is a second runtime boundary.

- **Endpoints:** list hosts, get host, get service, fetch document.
- **Fault middleware** (seeded, as above): 500, 429, timeout (responds after the client gives up), and an empty 200 (silent failure).
- **404 is not a fault.** It means the host or service doesn't exist, and that is a valid answer. Telling a real 404 apart from a fault is part of tool validation.

## Failure modes and mechanisms

| Problem (where long runs collapse) | Mechanism |
|---|---|
| Context corrupts as the window fills | The raw log never reaches a prompt. Each call gets a fresh, budgeted `ContextPacket`. |
| Summaries of summaries drift | Summaries are recomputed from facts by code. |
| Bad output poisons shared state | Output is schema-validated before commit. Facts carry provenance. Only `verified` facts count. |
| Tools fail silently | Each tool has a validator, so an empty 200 or malformed output is a failure. Failures are classified as retryable or permanent. |
| A worker dies mid-task | Leases with heartbeats. An expired lease requeues the task to the same role. Idempotent fact writes. |
| The coordinator dies | All state is in Postgres, so `--resume` continues from it. |
| Plans drift | Replan on new facts, exhausted retries, contradictions, and no progress for K steps. Every plan change is logged with its reason. |
| Agents work at cross purposes | Workers touch state only via task leases and facts. One writer (the coordinator) owns the plan. |

## Layout

```
lha/
  db/
    models.py      SQLAlchemy tables (internal: how rows are stored)
    crud.py        queries: claim_task, heartbeat, upsert_fact, ...
  schemas/         Pydantic models (external contracts between processes)
    tasks.py       one input + output schema per task type
    facts.py
    context.py     ContextPacket
  agents/
    worker.py      shared loop: claim, heartbeat, run, submit
    discovery.py   prompt, tools, output schema, fake model
    analysis.py
    reporter.py
  world/           FastAPI mock network: data, planted drift, faults
  tools.py         HTTP tool functions + validators
  context.py       builds a ContextPacket from the database
  coordinator.py   task DAG, validation, retries, replanning, goal check
  run.py           CLI entry point + scoring
```

- **`db/models.py` vs `schemas/`.** Database rows are internal. Pydantic schemas are the contract a worker's output must satisfy before the coordinator commits it. Keeping them apart keeps that validation boundary explicit.
- **One file per agent.** Everything about an agent (prompt, tools, output schema, fake model) lives together, so adding an agent means adding one file.
