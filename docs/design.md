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
| **Scorer** | Code that runs at the end (`run.py`): regenerates the world from the seed, compares the report's finding with the planted drift, and prints PASS or FAIL. |

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
        evidence: the registry fact + the two read events (by ID)
```

The agents start knowing only `host-1`, `host-2` and `host-3`. The example is
simplified: a real run has ~20 hosts, and the chain of documents leading to the
drift is 3+ hosts deep.

Why it is long-horizon:
- **Multi-hop.** The drifted service sits at the end of a chain of documents (depth 3+): a doc on a starting host names a hidden host, whose doc names another, and so on. Discovery works breadth-first, so most of the ~20 hosts get explored before the last hop. The run can't end in a handful of steps.
- **Decoys.** Some services *look* drifted on first read (a stale cached response) but match the registry on re-check. A claim only counts once a second, independent read confirms it.
- **Faults everywhere.** Tool calls time out, return 500s, or return an empty 200 ("silent success"). Workers crash. The whole run gets killed and resumed.
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

## Scope: v1 first

Build the core end to end before anything else. The core alone shows both
areas this project goes deep on: state and context management, and failure
recovery. Sections below mark later items with **(later)**.

**v1 (must work end to end):**
- Mock network: seeded world, document chain, decoys, fault middleware.
- Tables; claim, lease and heartbeat with `attempt` fencing; worker submit → coordinator commit in one transaction.
- `ContextPacket` builder with a token budget.
- Three agents with fake models (including seeded malformed output), four task types, two-reads-agree verification.
- Retries with backoff and `max_attempts`; capped new rounds after that.
- Goal check, reporter, scorer, `--chaos`.
- `--kill-at` / `--resume`.

**Later (designed, built after v1 works):**
- Circuit breaker per host; stall detection.
- Supervisor auto-restarts, injected worker crashes, the coordinator advisory lock.
- Splitting oversized tasks (v1 only fails them with `context_overflow`).
- A tool to fetch raw output by pointer.
- The "find all drifts" goal.
- `depends_on` for tasks planned ahead of their inputs (e.g. an up-front multi-step plan).

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
       - nothing moved for a while → replan (later)
  4. check the goal: is there a verified drift?
       - yes → create the reporter task, cancel all leftover tasks
  5. stop if the report is written or the step budget is used up
```

Workers never decide what happens next. They pick a task off the board, do
it, and hand back a result. The coordinator decides what that result means.

## Roles

| Role | Does | Input | Output |
|---|---|---|---|
| **Coordinator** | Plain code, not an LLM. Owns the plan. Validates and commits worker output, creates follow-up tasks, retries, replans, decides when the goal is met. | Ledger | Tasks, plan events |
| **Discovery** | Breadth. Reads a host's services and documents, records what exists and which other hosts are mentioned. | A host name | Facts: host exists, service X runs N replicas, doc Z mentions host H |
| **Analysis** | Depth. Compares a service's discovered replica count with the registry, raises a drift hypothesis. A verify task re-reads the service with a fresh call. | A service plus its registry entry | Facts: `inferred` drift → `verified` / `refuted` |
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
| `compare_service` | Analysis | a service's read fact + its registry entry (no network call) | a match, or an `inferred` drift fact |
| `verify_drift` | Analysis | an `inferred` drift fact | the replica count from a fresh read |
| `write_report` | Reporter | all verified facts | `finding.md` |

**Tasks are created only when their inputs exist.** The coordinator creates a
follow-up task from an *accepted* result, so a task never has to wait for
another one. `compare_service` is only created once **both** the service's
read and the registry entry exist as facts. Until the registry is found,
compare work isn't created; when the registry fact arrives, the coordinator
creates a compare for every service already read. Each task records `parent_task_id`
(the task whose result created it), which gives the plan's DAG for auditing.

## Verification ("checked twice")

1. Discovery's read of the service is read #1. A `compare_service` task compares it with the registry. A mismatch becomes an **`inferred`** drift fact. It doesn't count yet.
2. The coordinator creates a **verify** task. Any analysis worker can pick it up. It reads the service again with a fresh call and reports the number it saw. It does **not** decide anything.
3. **The coordinator decides, and two reads must agree.** If the fresh read agrees with the first, the drift becomes **`verified`**. If they disagree, neither read is trusted; a third read breaks the tie (2 out of 3). If the majority matches the registry, the drift becomes **`refuted`** (a decoy).

The second read is **not** automatically the truth. It could be stale or
broken too. What counts is agreement between independent reads.

A read that *matches* the registry is accepted after one read, and so is the
registry itself. Stale reads in the mock world only ever make a service look
drifted, never hide a real drift. That's a simplification, noted as a cut.

The verify read also updates the service's read fact (superseding the stale
one). That doesn't trigger a second compare: `compare_service:search@host-3`
already exists, so creating it again is a no-op (see `task_key`).

Verify rounds are capped at 3 per drift. A drift still undecided after that
stays `inferred` and never counts.

Only verified facts can satisfy the goal or appear in the report. This is what
stops one bad read (a stale cache, a silently broken tool, a malformed model
output) from ending up as the answer.

## Storage (Postgres)

**IDs.** Every table's primary key is a **UUIDv7**, generated in Python
(`uuid-utils`; Postgres 16 has no built-in v7). A v7 UUID starts with a
timestamp, so sorting by `id` is sorting by creation time, while staying
globally unique. `created_at` uses `clock_timestamp()`, not `now()`: `now()`
returns the same time for every row in one transaction, and the coordinator
creates several tasks per transaction.

| Table | Purpose | Notes |
|---|---|---|
| `sessions` | One row per run: goal, starting hosts, seed, fault rate, step budget, status (`running` · `succeeded` · `failed`) | Every other table has `session_id` |
| `events` | Append-only log of everything: context packets, model calls, tool calls (with raw responses), errors, fact changes, coordinator decisions. Columns: `id`, `session_id`, `task_id`, `attempt`, `kind`, `payload` (jsonb), `created_at` | Workers and the coordinator both write here. Never put into prompts wholesale |
| `tasks` | Work queue and plan: type, role, input, scope, status, lease, attempts, result | Columns below |
| `facts` | Structured findings with provenance (task, tool call) and status | Written **only** by the coordinator. Deduplicated by `(subject, key)` |

### Task queue

- Tasks are added during the run, as results come in. A new task is created `ready`, because its inputs already exist.
- Workers claim ready tasks with `SELECT … FOR UPDATE SKIP LOCKED`.
- `FOR UPDATE SKIP LOCKED` is Postgres's job-queue primitive. It is two instructions, and the skipping is done by the query that's running, not by anyone else:
  1. **`FOR UPDATE`**: "I'm going to change the rows I'm selecting, so lock them for me." Until my transaction ends, nobody else can lock or change them.
  2. **`SKIP LOCKED`**: "While I'm looking, if a row is already locked by someone else, don't wait for it. Leave it out of my results."

  Two workers asking at the same moment, with tasks A, B and C ready:

  ```
  worker 1: SELECT … LIMIT 1 FOR UPDATE SKIP LOCKED  → gets A (A now locked)
  worker 2: SELECT … LIMIT 1 FOR UPDATE SKIP LOCKED  → A is locked → skip → gets B
  ```

  - Without `SKIP LOCKED`: worker 2 waits for worker 1's transaction to finish before moving on, so workers queue up behind each other.
  - Without `FOR UPDATE`: nothing is locked, so both workers can read A as `ready` and both claim it. The task runs twice.
  - Together: every worker gets a different task, and nobody waits.
- Status flow: `ready → leased → submitted → succeeded | failed | cancelled`. A retry goes back to `ready`.

Task columns:

| Column | Example |
|---|---|
| `id` | `0192f7a1-…` (UUIDv7) |
| `session_id` | `3f2b9c1e-…` |
| `task_key` | `discover_host:host-4` (unique per session) |
| `type`, `role` | `discover_host`, `discovery` |
| `input` (jsonb) | `{"host": "host-4"}` |
| `scope` | `["host:host-4"]` (what the context builder selects facts for) |
| `parent_task_id` | the task whose result created this one (`NULL` for the first tasks) |
| `status` | `ready` |
| `attempt`, `max_attempts` | `1`, `3` |
| `leased_by`, `lease_expires_at` | `discovery-2`, `2026-10-03 14:02:41+00` |
| `not_before` | backoff: not claimable before this time |
| `result` (jsonb) | the worker's submitted output, before the coordinator accepts it |

**`task_key` makes task creation idempotent.** Two documents can both mention
`host-4`, and a resumed coordinator may replay a decision it already made.
A unique index on `(session_id, task_key)` with `ON CONFLICT DO NOTHING` means
the same task is never created twice. It is also the stable name used to seed
faults (see Determinism).

**Format:** a readable string, `<type>:<what it's about>`, built from the
task's input:

| Task | `task_key` |
|---|---|
| discover a host | `discover_host:host-4` |
| compare a service | `compare_service:cache@host-4` |
| verify a drift | `verify_drift:cache@host-4#1`, then `#2` for the tie-break |
| write the report | `write_report` |

- **`task_key` is not the `id`.** The `id` (a UUID) is new for every row and every run, so it can't seed faults or catch a duplicate. `task_key` describes *what* the task is, so it's the same every time. The `id` is for joins and references.
- **Retry vs. new round.** A *retry* is the same row with `attempt + 1`, same key. A deliberate *new round* of the same work (a tie-break verify, a host re-tried after its cooldown) gets a `#n` suffix, so it is a new row with a new key.

```sql
-- claim one ready task: a single statement, so a single transaction
UPDATE tasks SET status = 'leased', leased_by = :worker,
       lease_expires_at = now() + interval '30 seconds'
 WHERE id = (
   SELECT id FROM tasks
    WHERE session_id = :sid AND status = 'ready' AND role = :role
      AND (not_before IS NULL OR not_before <= now())   -- respect backoff
    ORDER BY created_at, id
    LIMIT 1
    FOR UPDATE SKIP LOCKED)       -- lock it; skip rows others have locked
RETURNING id, task_key, attempt, input;
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

- **Worker dies:** the heartbeats stop and the lease expires. On every loop, the coordinator sweeps expired leases back to `ready` with `attempt + 1`. Tasks already at `max_attempts` are set to `failed` by a second statement and go through the replan rules.

  ```sql
  UPDATE tasks SET status = 'ready', attempt = attempt + 1, leased_by = NULL
   WHERE session_id = :sid AND status = 'leased' AND lease_expires_at < now()
     AND attempt < max_attempts;
  ```
- **Worker was only slow:** its next heartbeat or result submission matches 0 rows, because the `attempt` changed. The worker drops its result. This stops a late "zombie" worker from overwriting the retry's work.

The heartbeat loop lives **inside the worker**: started when it claims a task,
cancelled when it submits. `attempt` acts as a *fencing token*: a number that
goes up every time the task is handed out, so an old holder can always tell
it has been replaced.

```
t=0   worker A claims task T          attempt=1, lease until t=30
t=5   A's tool call hangs; heartbeats stop
t=30  lease expired → coordinator: task T ready, attempt=2
t=31  worker B claims task T          attempt=2
t=45  A wakes up, submits "attempt=1" → matches 0 rows → A discards its result
t=50  B submits "attempt=2"           → accepted
```

`leased_by` alone isn't enough: the same worker could re-claim the task later
under a newer attempt.

**Cancellation needs no extra code.** When the goal is met, the coordinator
sets every leftover task to `cancelled`, including ones a worker is running
right now. That worker's next heartbeat or submit has the same
`AND status = 'leased'` condition, so it matches 0 rows, and the worker stops
and discards its result.

### Who writes what

Workers write `events` (every model call and tool call, tagged with
`task_id` and `attempt`) and their own task's `result`. They never write facts.
The split keeps "LLMs propose, code decides" true at the database level:

1. **Worker submits.** One fenced `UPDATE`: `status = 'submitted'`, `result = <output>`, `WHERE id = :id AND attempt = :attempt AND status = 'leased'`. A failed attempt is submitted the same way, with `result = {"error": {"kind": "timeout", ...}}`.
2. **Coordinator accepts.** It validates `result` against the task type's schema, and checks every claimed fact against its source. A **read** must cite a tool-call event from **this** attempt, and the value must appear in that event's raw response. A **drift claim** must cite the read and registry facts it compares, and the coordinator recomputes the comparison itself. A made-up host or number fails these checks. Then, in **one transaction**: write the facts, mark the task `succeeded`, create follow-up tasks, and log the decision in `events`. An error result (or an invalid one) goes through the retry rules instead.

If the coordinator crashes halfway, the transaction rolls back and the task is
still `submitted`. On resume, it simply processes it again. There is no state
where facts exist but their follow-up tasks don't.

### Facts

Raw tool output goes into `events` in full (that's what pointers refer to).
The agent's result lists the **facts** it read, each citing the event it came
from, and the coordinator checks them (above). A fact is one small, typed
claim.

| Column | Example |
|---|---|
| `id` | `0192f7a3-…` (UUIDv7) |
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

| row | subject | key | value | status |
|---|---|---|---|---|
| A | `service:search@host-3` | `config.replicas` | `1` | `superseded` |
| B | `service:search@host-3` | `config.replicas` | `2` | `observed` ← current |

- Inserting another `superseded` row for this key is allowed.
- Inserting another `observed` row while row B is current fails with `duplicate key value violates unique constraint "facts_one_current"`.
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

A **step** is one model call or one tool call. Each is logged as an event, so
the step count is a count of those events, the same from every process.
Heartbeats are not events.

## Processes, crashes and resume

`just start` runs a **supervisor** (`run.py`). It starts the mock network,
the coordinator, and the worker processes, and restarts any of them that dies.

- **Worker crash** (injected, seeded) **(later)**: the supervisor kills a worker mid-task and starts a new one. The old lease expires and the task is retried.
- **Coordinator crash (later):** the coordinator is restartable just like a worker, because its state is all in Postgres. The supervisor restarts it, and it picks up `submitted` tasks and expired leases. If it crashes 3 times within a minute, that's a bug (e.g. one result that always crashes it), not bad luck: the supervisor stops everything with an error and the session stays `running`, so `--resume` can continue after the fix.
- **One coordinator at a time (later):** a Postgres advisory lock per session: a named lock not tied to any table, held by one connection and released automatically when it closes. The key comes from the session ID, so nothing extra is stored: `pg_try_advisory_lock(hashtextextended('coordinator:' || :session_id, 0))`. A restarted coordinator can't run alongside an old one that hasn't fully died.
- **`--kill-at N`:** the supervisor watches the step count, and the whole run exits abruptly (`os._exit`) after step N: coordinator, workers, everything. This is the harshest crash: in-flight tasks are lost mid-lease.
- **`--resume`:** finds the latest unfinished session, starts everything again with that session's seed and fault rate, and continues from Postgres. No worker from the old run is alive, so all its leases are released right away (no 30 s wait); `submitted` results are processed. Nothing is replayed from memory, because nothing important lives in memory.

## Context packets

A `ContextPacket` is a Pydantic model built in memory right before one LLM
call, from a fresh database query:

1. **Pinned.** Goal, done criteria, this task's spec. Always included.
2. **Facts.** Only facts relevant to this task (e.g. this host or service).
3. **Recent.** The last few events of *this* task (e.g. the previous attempt's error).
4. **Pointers.** IDs of raw tool outputs in `events`. The payloads themselves are left out. In v1 they're for audit; a tool to fetch one on demand is **(later)**.

The four layers are listed from highest to lowest priority, and each has a
token budget. When the packet is over budget, cut from the bottom up:
**pointers** first, then the oldest **recent** events, then the least relevant
**facts**. **Pinned** is never cut.

**How cutting works.** Build the packet top-down instead of trimming it:
1. `budget = context window − tokens reserved for the model's output`.
2. Add pinned.
3. Add whole items in priority order (facts by relevance, then recent events newest first, then pointers) while the next item still fits.
4. Never cut an item in half. Record what was left out ("12 more facts omitted") in the packet and in the logged event.

Example: budget 10, pinned 4, facts 4, recent 2, pointers 1 = 11. Pinned,
facts and recent fit (10); the pointer doesn't, so it's left out. Same result
as "drop the lowest item", but there's no loop of cut-and-recount.

Token counts are estimated (~4 characters per token). That's fine with fake
models; a real model would use its tokenizer.

**If pinned alone doesn't fit**, the task is too big (splitting is **(later)**; v1 fails the task). That's a planning bug,
not a context problem. Truncating pinned would silently change what the task
is, so instead:
- the coordinator caps pinned size when it *creates* a task (e.g. ≤ 30% of the window), and splits oversized tasks (e.g. a host with 50 services → several `discover_host` tasks over batches of services);
- if a packet still overflows, the worker fails the attempt with a permanent `context_overflow` error, and the coordinator splits the task instead of retrying it.

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
- **Summaries.** Any summary (e.g. the coordinator's progress line, or a "12 more facts omitted" note) is recomputed by code from the facts. Never summarize a summary.

## Retries and replanning

Plain rules in the coordinator, applied in order.

**1. Classify every failure.**
- **Retryable:** timeout, 500, 429, empty 200, invalid model output, expired lease. The task goes back to `ready` after a backoff (`not_before = now() + 2^attempt s`).
- **Permanent:** 404 (the host or service doesn't exist). The task fails right away; retrying won't help.

**2. Retry budget.** Each task gets `max_attempts = 3`. After that it is
`failed`. Nothing downstream was ever created from it, because follow-ups only
come from accepted results.

**3. Replan after a failure.** A fixed table per task type:

| Failed task | What the coordinator does |
|---|---|
| `discover_host` | Record a `host unreachable` fact. Try one more round later (`discover_host:host-4#2`). |
| `compare_service` | Try once more later as a new round. It makes no network call, so repeated failure means bad model output. |
| `verify_drift` | Schedule another verify round later. The drift stays `inferred`, so it can't count yet. |
| `write_report` | Retry. If it keeps failing, the session fails, and the verified facts are still in the database. |

**4. Circuit breaker (per host) (later).** After 3 failures in a row against the same
host, the coordinator stops creating tasks for that host for a cooldown period
(the breaker is "open"). Then it lets one probe task through. Success closes
the breaker; failure re-opens it. This stops the run from wasting its whole
budget retrying one broken host.

**5. Stall (later).** No task succeeded in the last K steps and nothing is ready →
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
- **Stalled (later).** See rule 5 above.
- **Failed.** Step budget exhausted. The reporter still runs (it's exempt from the budget) and writes a partial report from what was verified.

## Fake LLMs

No API keys, no spend. Each agent's model is a deterministic Python function,
plugged in through Pydantic AI `FunctionModel`. The agent loop, tool calling,
and output validation are real; only the "brain" is scripted.

- **It reacts to its input.** It is a rule-based policy (e.g. "read every service of my host not yet in facts"), not a fixed tape, so it adapts when tools fail.
- **Two error sources:**
  1. **Tool errors** come from the mock world. The agent sees them and reports them.
  2. **Model errors.** The fake model sometimes emits malformed output on purpose (wrong schema, a host that doesn't exist). Validation rejects it before it reaches the ledger, and the task is retried.

## Determinism

Faults are **pseudo-random but seeded**. Different seeds give different
fault patterns.

Each fault decision is derived from `hash(seed, task_key, attempt, call_no)`,
not from one shared random generator. Tools send `task_key`, `attempt` and
the call number as HTTP headers, so the mock network can decide without
keeping any state.

```python
def roll(seed, task_key, attempt, call_no) -> float:  # 0.0 ≤ x < 1.0
    h = hashlib.sha256(f"{seed}|{task_key}|{attempt}|{call_no}".encode())
    return int.from_bytes(h.digest()[:8], "big") / 2**64


r = roll(...)
if r < fault_rate:  # e.g. 0.15; --chaos raises it
    return pick_fault(r)  # 500 / 429 / timeout / empty 200
```

- **It's a dice roll, not a rule about hosts.** `discover_host:host-4` doesn't always fail. Each call gets its own fixed roll, and roughly `fault_rate` of all calls fail.
- **Nothing about faults is stored.** The mock network computes each roll from the request headers and the seed it got at startup. The world itself (hosts, services, the drift, the decoys) is also generated from the seed, so a resumed run and the scorer see exactly the same world.
- **Use `hashlib`, not Python's `hash()`.** The built-in `hash()` of a string changes on every process start (`PYTHONHASHSEED`).
- **Faults are still visible afterwards.** The mock network writes nothing to Postgres (it plays the outside world), but the tool records every error it got as an event.
- **`task_key`, not the task's `id`.** The `id` is a fresh UUID in every run. `discover_host:host-4` is the same in every run.
- **`call_no` is the Nth tool call within one attempt** (0, 1, 2, …). One `discover_host` attempt makes several HTTP calls (get host, get each service, fetch each document). Without `call_no` they'd all share one roll and fail or succeed together.
- **A retry gets a new roll.** A new `attempt` means a new hash, so a retry can succeed where the first try failed.

What "same seed, same run" promises: the same world, the same faults for each
task attempt, and the same outcome (PASS and the same finding). It does
**not** promise an identical event log. With parallel workers, the order of
events still varies, and lease timeouts depend on wall-clock time.

## Mock world (FastAPI)

The mock network is its own FastAPI service. Tools make real HTTP calls to it,
so this is a second runtime boundary.

- **Endpoints:** get host (its services and documents), get service (replica count), fetch document. There is deliberately **no "list all hosts"**: hidden hosts can only be found through documents. The starting hosts come from the session.
- **Fault middleware** (seeded, as above): 500, 429, timeout, and an empty 200 (silent failure).
- **Timeouts.** The *client* is our tool code: the HTTP client (`httpx`) inside a worker that calls the mock network. For a timeout fault, the server deliberately waits ~3 s; the client gives up after ~1 s and records a timeout. Keeping the client's limit short means a timeout costs a worker 1 s, not 30 s, so a full run stays at around a minute.
- **Decoys.** A few healthy services are marked as decoys. Reads from `discover_host` tasks get a stale replica count for them; reads from `verify_drift` get the true one (the world reads the task type from the `task_key` header). Stale reads are never applied to the drifted service. This is a simplification: real staleness can hide drift too.
- **404 is not a fault.** It means the host or service doesn't exist, and that is a valid answer. Telling a real 404 apart from a fault is part of tool validation.

## Failure modes and mechanisms

| Problem (where long runs collapse) | Mechanism |
|---|---|
| Context corrupts as the window fills | The raw log never reaches a prompt. Each call gets a fresh, budgeted `ContextPacket`. |
| Summaries of summaries drift | Summaries are recomputed from facts by code. |
| Bad output poisons shared state | Output is schema-validated, and every fact is checked against the tool response it cites, before commit. Only `verified` facts count. |
| Tools fail silently | Each tool has a validator, so an empty 200 or malformed output is a failure. Failures are classified as retryable or permanent. |
| A worker dies mid-task | Leases with heartbeats. An expired lease requeues the task to the same role. `attempt` fencing drops late results. |
| The coordinator dies | All state is in Postgres, so `--resume` continues from it. |
| Plans drift | Follow-ups only from accepted results; fixed replan rules for exhausted retries; stall detection (later). Every plan change is logged with its reason. |
| Agents work at cross purposes | Workers touch state only via task leases and facts. One writer (the coordinator) owns the plan. |

## Limits and scaling

- **Breaks first as the horizon grows:** the coordinator. It's one loop that commits every result, so at high throughput it becomes the bottleneck. Next is polling: idle workers poll for ready tasks; `LISTEN/NOTIFY` would replace that.
- **2 → 20 workers:** claiming already scales (`SKIP LOCKED`, plus an index on `(session_id, status, role, created_at)`). The coordinator stays the single writer per session. Beyond that, split the plan: one coordinator per partition of subjects (e.g. per subnet), each with its own advisory lock.
- **Context at scale:** packets are per task and scoped by subject, so prompt size doesn't grow with the number of workers or steps. What does grow is the reporter's input; that would be summarized by code (counts per partition) and split.
- **Memory at scale:** facts are one current row per key, and the unique index serializes two writers racing on the same key. A larger system would add a version per key and per-partition fact stores.

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
