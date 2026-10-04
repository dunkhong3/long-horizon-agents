# Design

## What this is

This document explains how the system works and why it is built the way it is. The README is the short version, and [NOTES.md](../NOTES.md) is the look-back on what we cut, what comes next and how the project was built. Every claim about the code points to the file and the lines where it happens, so it can be checked.

## The goal the agents pursue

The agents audit a mock cloud deployment for replica drift and write a finding that has been verified. A few words need explaining before anything else. A host is a fake server in the mock network (`host-1`, `host-2` and so on), and hosts are what the agents explore. A service is a program running on a host (`payments`, `search`, `cache`), and its replicas are how many copies of it are running at the same time, much like pods in Kubernetes. The registry is one file, `registry.json`, on one host, which lists how many replicas each service should have, and it is the 'source of truth' the agents compare against. A drift is a service running a different number of replicas than the registry says. A document is a text file on a host (such as `runbook.md`, a team's operations notes), and documents can mention other hosts, which is the only way hidden hosts are found. A stale read is when the mock server answers with an old, out-of-date value, like a cache that hasn't refreshed, and a fresh read is simply asking the same question again with a new call. A decoy is a service that looks drifted because of a stale read but matches the registry on a fresh read. Lastly, the scorer is the code that runs at the end (`lha/scoring.py`, lines 30–52), which builds the world again from the seed, compares the report's finding with the planted drift and prints PASS or FAIL.

The small example below shows the idea.

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

The agents start knowing only `host-1`, `host-2` and `host-3`, and the example is simplified, because a real run has 20 hosts, about 35 services, 3 decoys and one dead reference, which is a document naming a host that doesn't exist (`lha/world/model.py`, lines 64–137).

The run is long for three reasons. First, the drifted service sits at the end of a chain of documents that starts at one of the deepest other hosts, 5–6 hops from the start in total, and because discovery works breadth-first the chain is only entered after most of the network has been explored (`lha/world/model.py`, lines 77–95), so the run cannot end in a handful of steps. Second, some services look drifted on the first read because of a stale response but match the registry when checked again, so a claim only counts once a second, independent read agrees with it. Lastly, faults happen everywhere, as tool calls time out, return 500 errors or return an empty 200 (a 'silent success'), workers crash, and the whole run gets killed and resumed. Altogether a run takes roughly 250–350 steps of discovery calls, analysis, verification and retries.

The goal is not 'scan the entire network', because coverage is a measure we report and not what counts as success, so a run that finds and verifies the drift early is a good run. The goal is 'find the drift', and since exactly one is planted, finding one verified drift ends the run. If the world planted several, the goal would have to become 'check every reachable service', with done meaning 'no unexplored hosts left and every service has a verdict', which gives a longer and more predictable horizon and is the natural next step.

The core of the system (the ledger, the task queue, the context packets, the coordinator and the recovery rules) knows nothing about networks, so this is not the only task it can run. The domain lives in four pieces that can be swapped, which are the mock world, the tools, each agent's fake model and the goal check, and a research-brief goal for example would swap those four and keep the core.

## Scope and versions

What is in this repo is version 1 (v1), which is the core built end to end before anything else, because the core alone shows both areas this project goes deep on, state and context management and failure recovery. Items marked '(later)' in this document belong to the next version and are not built. The v1 core covers the mock network with a seeded world, the chain of documents, the decoys and the fault middleware, then the tables with task claiming, leases and heartbeats fenced by `attempt` and a worker submit followed by a coordinator commit in one transaction, then the `ContextPacket` builder with a token budget, then three agents with fake models (including seeded malformed output), four task types and verification by two agreeing reads, then retries with a backoff and `max_attempts` followed by a capped number of new rounds, then the goal check, the reporter, the scorer and `--chaos`, and lastly `--kill-at` and `--resume`. v1 also restarts a worker process that dies (`lha/run.py`, lines 75–81) and ends a run cleanly with a partial report when no work is left (`lha/coordinator.py`, lines 417–458), so a run cannot hang.

The later items are a circuit breaker per host, stall detection that re-opens work that was put off, injected worker crashes, automatic restarts of the coordinator together with a coordinator advisory lock, splitting oversized tasks (v1 only fails them with `context_overflow`), a tool to fetch a raw output by its pointer, the 'find all drifts' goal, and `depends_on` for tasks planned ahead of their inputs (such as an up-front plan of several steps). A search of `lha/` finds no code for any of them.

## The coordinator, in plain words

The coordinator is an ordinary loop and not an AI (`lha/coordinator.py`, lines 89–110). It is easiest to think of it as a project manager with a to-do board, which is the `tasks` table, and a notebook of findings, which is the `facts` table, and the loop below is what it does over and over.

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

Workers never decide what happens next. They pick a task off the board, do it and hand back a result, and the coordinator decides what that result means.

## Roles and task types

**The coordinator is plain code that owns the plan.** It checks and commits worker output, creates follow-up tasks, retries, replans and decides when the goal is met, and its own output is tasks and logged decisions (`lha/coordinator.py`).

**Discovery is the 'breadth' role.** Given a host name, it reads the host's services and documents and records what exists, which services run how many replicas and which other hosts the documents mention (`lha/agents/discovery.py`, lines 19–52).

**Analysis is the 'depth' role.** Given a service and its registry entry, it compares the discovered replica count with the registry and raises a drift hypothesis, and in a verify task it reads the service again with a fresh call, which moves an `inferred` drift towards `verified` or `refuted` (`lha/agents/analysis.py`, lines 19–56).

**The reporter runs once, at the end.** It writes the finding from verified facts only and cites their fact IDs, and its output becomes `finding.md` (`lha/agents/reporter.py`, lines 15–53).

Several worker processes run side by side, two for discovery, one for analysis and one for the reporter (`lha/run.py`, line 37). Workers never talk to each other and share findings only through the fact table, and each task's prompt is built from the latest facts at the moment a worker claims it, so nobody misses anyone else's findings.

Every task has a type, and each type belongs to exactly one role (`lha/schemas/tasks.py`, lines 17–22), so a worker only claims tasks of its own role (`WHERE role = :role`), and each type has its own input and output schema (`lha/schemas/tasks.py`, lines 27–104). There are four types. `discover_host` belongs to discovery, takes a host name and returns the services with their replica counts, the documents and the hosts they mention. `compare_service` belongs to analysis, takes a service's read fact and its registry entry, makes no network call, and returns either a match or an `inferred` drift fact. `verify_drift` also belongs to analysis, takes an `inferred` drift fact and returns the replica count from a fresh read. `write_report` belongs to the reporter, takes all verified facts and returns `finding.md`.

Tasks are only created once their inputs exist. The coordinator creates a follow-up task from an accepted result, so a task never has to wait for another one, and `compare_service` is only created once both the service's read and the registry entry exist as facts. Until the registry is found no compare work is created, and when the registry fact arrives the coordinator creates a compare for every service already read (`lha/coordinator.py`, lines 211–228). Each task records `parent_task_id`, which is the task whose result created it, and that gives the plan's tree of tasks for auditing.

## Verification ('checked twice')

A drift goes through three stages (`lha/coordinator.py`, lines 230–293). First, discovery's read of the service is read number one, a `compare_service` task compares it with the registry, and a mismatch becomes an `inferred` drift fact, which does not count yet. Second, the coordinator creates a verify task that any analysis worker can pick up, which reads the service again with a fresh call and reports the number it saw without deciding anything. Lastly, the coordinator decides, and two reads must agree, so if the fresh read agrees with the first the drift becomes `verified`, and if they disagree neither read is trusted and a third read breaks the tie (2 out of 3). If the majority matches the registry, the drift becomes `refuted`, which means it was a decoy.

The second read is not automatically the truth, because it could be stale or broken too, and what counts is agreement between independent reads. A read that matches the registry is accepted after one read, and so is the registry itself, and stale reads in the mock world only ever make a service look drifted and never hide a real drift (`lha/world/app.py`, lines 65–68), which is a simplification we list as a cut.

The verify read also updates the service's read fact, superseding the stale one. That does not trigger a second compare, because `compare_service:search@host-3` already exists, so creating it again does nothing (see `task_key` below). Verify rounds are capped at 3 per drift (`lha/config.py`, line 12), and a drift still undecided after that stays `inferred` and never counts. Only verified facts can meet the goal or appear in the report, which is what stops one bad read (a stale cache, a silently broken tool, a malformed model output) from ending up as the answer.

## Storage

Every table's primary key is a UUIDv7, generated in Python in a few lines (`lha/ids.py`, lines 13–25), because Python 3.11 and Postgres 16 have no built-in v7. A v7 UUID starts with a timestamp, so sorting by `id` is sorting by creation time while the ids stay globally unique. `created_at` uses `clock_timestamp()` and not `now()` (`lha/db/models.py`, lines 24–26), because `now()` returns the same time for every row in one transaction and the coordinator creates several tasks per transaction.

There are four tables, and every table apart from `sessions` has a `session_id`. The `sessions` table has one row per run, with the goal, the starting hosts, the seed, the number of hosts, the fault rate, the step budget, the status (`running`, `succeeded` or `failed`) and the accepted report (`lha/db/models.py`, lines 28–42). The `events` table is an append-only log of everything that happens, meaning context packets, model calls, tool calls with their raw responses, errors, fact changes and coordinator decisions, with the columns `id`, `session_id`, `task_id`, `attempt`, `actor`, `kind`, `payload` (jsonb) and `created_at` (`lha/db/models.py`, lines 44–59). Workers and the coordinator both write to it, and it is never put into a prompt as a whole. The `tasks` table is the work queue and the plan (`lha/db/models.py`, lines 61–84), and the `facts` table holds the structured findings with where they came from and their status (`lha/db/models.py`, lines 86–109), written only by the coordinator and kept to one current row per `(subject, key)`.

## Task queue and leases

Tasks are added during the run as results come in, and a new task is created as `ready`, because its inputs already exist (`lha/db/crud.py`, lines 75–107). Workers claim ready tasks with `SELECT … FOR UPDATE SKIP LOCKED`, which is Postgres's building block for job queues. It is really two instructions, and the skipping is done by the query that is running and not by anyone else. `FOR UPDATE` says 'I am going to change the rows I am selecting, so lock them for me', and until that transaction ends nobody else can lock or change them. `SKIP LOCKED` says 'while I am looking, if a row is already locked by someone else, don't wait for it and leave it out of my results'. When two workers ask at the same moment with tasks A, B and C ready, it goes like this.

```
worker 1: SELECT … LIMIT 1 FOR UPDATE SKIP LOCKED  → gets A (A now locked)
worker 2: SELECT … LIMIT 1 FOR UPDATE SKIP LOCKED  → A is locked → skip → gets B
```

Without `SKIP LOCKED`, worker 2 would wait for worker 1's transaction to finish before moving on, so workers would queue up behind each other, and without `FOR UPDATE` nothing would be locked, so both workers could read A as `ready` and both claim it, and the task would run twice. Together, every worker gets a different task and nobody waits. A task moves from `ready` to `leased` to `submitted` and then to `succeeded`, `failed` or `cancelled`, and a retry goes back to `ready`. The real claim query is one statement, so one transaction (`lha/db/crud.py`, lines 112–125).

```sql
UPDATE tasks SET status = 'leased', leased_by = :worker,
       lease_expires_at = now() + make_interval(secs => :lease)
 WHERE id = (
   SELECT id FROM tasks
    WHERE session_id = :sid AND status = 'ready' AND role = :role
      AND (not_before IS NULL OR not_before <= now())   -- respect backoff
    ORDER BY created_at, id
    LIMIT 1
    FOR UPDATE SKIP LOCKED)
RETURNING id, task_key, type, attempt, input, scope
```

The columns of a task are its `id` (a UUIDv7 such as `0192f7a1-…`), its `session_id` (such as `3f2b9c1e-…`), its `task_key` (such as `discover_host:host-4`, unique per session), its `type` and `role` (such as `discover_host` and `discovery`), its `input` in jsonb (such as `{"host": "host-4"}`), its `scope`, which is what the context builder selects facts for (such as `["host:host-4", "*@host-4"]`), its `parent_task_id` (`NULL` for the first tasks), its `status` (such as `ready`), its `attempt` and `max_attempts` (such as `1` and `3`), its `leased_by` and `lease_expires_at` (such as `discovery-2` and `2026-10-03 14:02:41+00`), its `not_before`, which is the backoff time before which it cannot be claimed, and its `result` in jsonb, which is the worker's submitted output before the coordinator accepts it (`lha/db/models.py`, lines 61–84).

The `task_key` makes creating a task idempotent, meaning doing it twice has the same effect as doing it once. Two documents can both mention `host-4`, and a resumed coordinator may repeat a decision it already made, but a unique constraint on `(session_id, task_key)` together with `ON CONFLICT DO NOTHING` means the same task is never created twice (`lha/db/models.py`, line 82; `lha/db/crud.py`, line 104). It is also the stable name used to seed faults (see 'Determinism'). The key is a readable string of the form `<type>:<what it's about>`, built from the task's input (`lha/schemas/tasks.py`, lines 117–136), so discovering a host is `discover_host:host-4`, comparing a service is `compare_service:cache@host-4`, verifying a drift is `verify_drift:cache@host-4#1` and then `#2` for the tie-break, and writing the report is `write_report`. The `task_key` is not the `id`, because the `id` is new for every row and every run, so it cannot seed faults or catch a duplicate, while the `task_key` describes what the task is and is the same every time, and the `id` is for joins and references. A retry is the same row with `attempt + 1` and the same key, while a deliberate new round of the same work (a tie-break verify, or a host tried again after its cooldown) gets a `#n` suffix, so it is a new row with a new key.

There is no explicit 'unlock', because a row lock lasts until the transaction ends, and what keeps the task claimed after that is the lease, meaning the status `leased` plus `lease_expires_at`. While a worker runs a task, a background asyncio task inside the worker extends the lease every 10 s, which is shorter than the 30 s lease so one late beat isn't fatal (`lha/config.py`, lines 4–5; `lha/agents/worker.py`, lines 101–108). In SQL terms the heartbeat is the update below (`lha/db/crud.py`, lines 133–150).

```sql
UPDATE tasks SET lease_expires_at = now() + interval '30 seconds'
 WHERE id = :task_id AND leased_by = :worker AND attempt = :attempt
   AND status = 'leased';
```

If a worker dies, its heartbeats stop and the lease expires, and on every loop the coordinator selects the expired leases and sends each task through the same retry rule as any other failure, so it goes back to `ready` with `attempt + 1`, or to `failed` and the replan rules once it has used up `max_attempts` (`lha/coordinator.py`, lines 397–413). If a worker was only slow, its next heartbeat or result submission matches 0 rows because the `attempt` changed, so the worker drops its result, which stops a late 'zombie' worker from overwriting the retry's work. The heartbeat loop is started when the worker claims a task and cancelled when it submits, and `attempt` acts as a fencing token, which is a number that goes up every time the task is handed out, so an old holder can always tell it has been replaced.

```
t=0   worker A claims task T          attempt=1, lease until t=30
t=5   A's tool call hangs; heartbeats stop
t=30  lease expired → coordinator: task T ready, attempt=2
t=31  worker B claims task T          attempt=2
t=45  A wakes up, submits "attempt=1" → matches 0 rows → A discards its result
t=50  B submits "attempt=2"           → accepted
```

`leased_by` alone is not enough, because the same worker could claim the task again later under a newer attempt. Cancelling needs no extra code either. When the goal is met, the coordinator sets every leftover task to `cancelled`, including ones a worker is running right now, and that worker's next heartbeat or submit has the same `AND status = 'leased'` condition, so it matches 0 rows and the worker stops and throws its result away.

## Who writes what

Workers write `events` (every model call and tool call, tagged with `task_id` and `attempt`) and their own task's `result`, and they never write facts, which keeps 'LLMs propose, code decides' true at the database level. A worker submits with one fenced update that sets `status = 'submitted'` and `result = <output>` where `id`, `attempt` and `status = 'leased'` still match (`lha/db/crud.py`, lines 153–168), and a failed attempt is submitted the same way with `result = {"error": {"kind": "timeout", ...}}` (`lha/agents/worker.py`, lines 110–145).

The coordinator then checks `result` against the task type's schema and checks every claimed fact against its source (`lha/coordinator.py`, lines 123–185). A read must cite a tool-call event from this same attempt, and the value must appear in that event's raw response (`lha/coordinator.py`, lines 320–328 and 529–534). A drift claim must cite the read fact and the registry fact it compares, and the coordinator does the comparison again itself (`lha/coordinator.py`, lines 230–257). A made-up host or number fails these checks. Only then, in one transaction, does it write the facts, mark the task `succeeded`, create the follow-up tasks and log the decision in `events`, while an error result or an invalid one goes through the retry rules instead. If the coordinator crashes halfway, the transaction rolls back and the task is still `submitted`, so on resume it is simply processed again, and there is never a state where facts exist but their follow-up tasks don't.

## Facts

Raw tool output goes into `events` in full, and that is what pointers refer to. The agent's result lists the facts it read, each citing the event it came from, and the coordinator checks them as above. A fact is one small, typed claim with an `id` (a UUIDv7 such as `0192f7a3-…`), a `session_id` (such as `3f2b9c1e-…`), a `subject` (such as `service:cache@host-4`), a `key` (such as `config.replicas`), a `value` in jsonb (such as `1`), a `status` (`observed`, `inferred`, `verified`, `refuted` or `superseded`), a `source_task_id` and `source_event_id` saying which task and which tool call produced it, an `evidence` list in jsonb holding the reads behind a drift fact, and a `created_at` time (such as `2026-10-03 14:02:11+00`).

The fact names used in this domain are `host:<h>` (with the keys `exists` and `unreachable`), `service:<s>@<h>` (with `config.replicas`, `drift.replicas` and `verdict`), `doc:<name>@<h>` (with `mentions`) and `registry:<s>` (with `replicas`) (`lha/schemas/facts.py`). Each service has two main kinds of fact. A read has `key = config.replicas`, a value such as `1` and the status `observed`, and a drift claim has `key = drift.replicas`, a value such as `{"expected": 3, "actual": 1}` and a status that moves from `inferred` to `verified` or `refuted`.

Facts are never deleted. A fact's `status` can change, for example from `inferred` to `verified` or to `superseded`, and every change is also written to `events` (`lha/db/crud.py`, lines 193–250), so the full history is always there, with `events` being the strictly append-only table. The current facts are the ones with `status <> 'superseded'`, and `refuted` facts stay visible, because 'we checked this and it was wrong' is useful knowledge.

The same fact means the same `(session_id, subject, key)`, and a partial unique index lets the database itself guarantee at most one current fact per key (`lha/db/models.py`, lines 99–108).

```sql
CREATE UNIQUE INDEX facts_one_current
    ON facts (session_id, subject, key)
 WHERE status <> 'superseded';
```

The index is a rule and not storage, so it doesn't keep, sort or return rows, and all it does is make Postgres refuse any write that would leave two current rows for the same key. Reading the current fact therefore needs no `ORDER BY created_at DESC LIMIT 1`, and the query below returns at most one row, with the index making it fast (`lha/db/crud.py`, lines 178–186).

```sql
SELECT * FROM facts
 WHERE session_id = :sid AND subject = :subject AND key = :key
   AND status <> 'superseded';
```

As an example, discovery's first read of `search` hit a stale answer of 1, and a later fresh read says 2, which leaves two rows.

```
row  subject                key              value  status
A    service:search@host-3  config.replicas  1      superseded
B    service:search@host-3  config.replicas  2      observed   ← current
```

Inserting another `superseded` row for this key is allowed, but inserting another `observed` row while row B is current fails with the Postgres error `duplicate key value violates unique constraint "facts_one_current"`, so replacing a fact is one transaction that marks the old row `superseded` and then inserts the new one. When the coordinator writes a fact, the same value does nothing, so retries change nothing, a different value marks the old fact `superseded` and inserts the new one, and a new value that contradicts a `verified` fact is never written silently, because `upsert_fact` raises `FactConflict` (`lha/db/crud.py`, lines 211–214) and the coordinator treats the whole result as rejected and retries the task (`lha/coordinator.py`, lines 156–162).

When the coordinator creates a task it sets its scope, which is the list of subjects the task is about, such as `host:host-3` and everything on that host (`lha/schemas/tasks.py`, lines 139–151). The context builder selects the current facts whose subject is in scope, plus the registry entries for those services, with a plain SQL filter and no embeddings or similarity search (`lha/context.py`, lines 97–132).

The tables grow, and that is fine, because it is storage and not prompt, and Postgres handles millions of rows. Facts grow with the size of the world and not with the number of steps, because a newer fact with the same `(subject, key)` supersedes the old one. Retention and archiving are out of scope for this project.

A step is one model call or one tool call. Each one is logged as an event, so the step count is a count of those events and is the same from every process (`lha/db/crud.py`, lines 24–25 and 55–57), while heartbeats are not events.

## Processes, crashes and resume

`just start` runs a supervisor (`lha/run.py`), which starts the mock network and the worker processes as separate operating system processes, runs the coordinator loop itself, and restarts any worker process that dies (`lha/run.py`, lines 47–112).

Several crash cases are designed. Injected, seeded worker crashes, where the supervisor kills a worker in the middle of a task and starts a new one so the old lease expires and the task is retried, are '(later)'. A crash of the coordinator is also '(later)', but the coordinator can be restarted just like a worker because its state is all in Postgres, and the plan is for the supervisor to restart it so it picks up `submitted` tasks and expired leases, and if it crashes 3 times within a minute, to treat that as a bug (such as one result that always crashes it) rather than bad luck, stop everything with an error and leave the session `running` so `--resume` can continue after the fix. Keeping to one coordinator at a time is '(later)' too, with a Postgres advisory lock per session, which is a named lock not tied to any table that one connection holds and that is released automatically when the connection closes, with a key taken from the session ID so nothing extra is stored, `pg_try_advisory_lock(hashtextextended('coordinator:' || :session_id, 0))`, so that a restarted coordinator cannot run alongside an old one that hasn't fully died.

`--kill-at N` is built. The coordinator loop checks the step count on every loop and stops after step N (`lha/coordinator.py`, line 104), and the supervisor then kills every process with SIGKILL and exits abruptly with `os._exit` (`lha/run.py`, lines 234–238), which is the harshest crash, because tasks in flight are lost in the middle of their lease. `--resume` is built too. It takes a session ID, or finds the latest unfinished session when none is given (`lha/run.py`, lines 148–156 and 206–216), starts everything again with that session's seed and fault rate, and carries on from Postgres. No worker from the old run is alive, so all of its leases are released straight away instead of waiting 30 s (`lha/run.py`, lines 159–172), and `submitted` results are processed. Nothing is replayed from memory, because nothing important lives in memory.

## Context packets

A `ContextPacket` is a Pydantic model built in memory right before one model call, from a fresh database query (`lha/schemas/context.py`; `lha/context.py`, lines 81–94), and it has four layers. The pinned layer holds the goal, what counts as done and this task's spec, and it is always included. The facts layer holds only the facts relevant to this task, such as this host or this service. The recent layer holds the last few events of this task, up to 5 of the coordinator's decisions about it (which carry the reason the previous attempt failed) and any lost leases (`lha/config.py`, line 28; `lha/context.py`, lines 135–152). The pointers layer holds up to 20 IDs of raw tool outputs from earlier attempts in `events` but leaves the outputs themselves out (`lha/context.py`, lines 155–168), and in v1 they are for audit only, as a tool to fetch one on demand is '(later)'.

The four layers are in order of priority from highest to lowest. The window is 2000 tokens with 400 kept back for the model's answer (`lha/config.py`, lines 26–27), and when the packet would be over that budget it is cut from the bottom up, so pointers go first, then the oldest recent events, then the oldest facts, and pinned is never cut. In practice the packet is built from the top down instead of being trimmed (`lha/context.py`, lines 42–78). The budget is the context window minus the tokens kept back for the answer, pinned goes in first, and then whole items go in by priority (facts newest first, then recent events newest first, then pointers) for as long as the next item still fits, with nothing ever cut in half and whatever was left out recorded in the packet's `omitted` counts, which are logged with the packet. As an example, with a budget of 10 and pinned taking 4, facts 4, recent 2 and pointers 1, the total would be 11, so pinned, facts and recent fit (10) and the pointer is left out, which gives the same result as 'drop the lowest item' but without a loop of cutting and recounting. Token counts are estimated at about 4 characters per token (`lha/context.py`, lines 35–39), which is fine for fake models, while a real model would use its own tokeniser.

If the pinned layer alone doesn't fit, the task is too big, which is a planning bug and not a context problem, and cutting pinned would silently change what the task is. In v1 the worker fails the attempt with a permanent `context_overflow` error (`lha/context.py`, lines 52–54; `lha/agents/worker.py`, lines 110–141). The plan for later is for the coordinator to cap the pinned size when it creates a task (for example at 30% of the window) and to split oversized tasks, such as a host with 50 services becoming several `discover_host` tasks over batches of services, and to split a task that still overflows instead of retrying it.

A packet is used for one call only and then thrown away. A copy is logged to `events` (`lha/agents/worker.py`, lines 116–119), so we can always see exactly what an agent saw, but the next call never starts from it and builds a fresh packet instead, because other workers have added facts since then, because the next call is usually a different task with a different scope, and because if the old packet held a bad or stale fact, rebuilding from the database picks up the correction while reusing the packet would carry the error forward. On a retry, the new packet's recent layer includes the reason the previous attempt failed, so the prompt grows with the task and not with the run's history.

Compaction, meaning shrinking what the model sees, happens on the prompt and never on the log, and it happens in three places. When raw tool output is written it is turned into facts, when a packet is read it goes through a relevance query and the budget, and any summary (such as the coordinator's progress line, or the count of omitted facts) is worked out again by code from the facts (`lha/coordinator.py`, lines 489–526), so we never summarise a summary.

## Retries and replanning

The coordinator follows plain rules, applied in order.

**Tools retry first.** A short-lived tool error (a timeout, a 500, a 429, an empty 200 or a malformed body) is retried up to 2 more times inside the attempt, each try with a new `call_no` and so a new fault roll, and every try is logged (`lha/tools.py`, lines 69–89; `lha/config.py`, line 17). Only if it keeps failing does the attempt fail and reach the coordinator.

**Every failure is sorted into retryable or permanent.** Timeouts, 500s, 429s, empty 200s, invalid or rejected model output and expired leases are retryable, so the task goes back to `ready` after a backoff of `not_before = now() + 0.25 × 2^attempt s` (`lha/coordinator.py`, lines 343–363; `lha/config.py`, line 9). A 404, meaning the host or service doesn't exist, is permanent, so the task fails straight away because retrying won't help.

**Each task gets `max_attempts = 3`.** After that it is `failed` (`lha/config.py`, line 8), and nothing downstream was ever created from it, because follow-ups only come from accepted results.

**A task that failed for good is replanned by a fixed rule per task type** (`lha/coordinator.py`, lines 365–395). A failed `discover_host` records a `host unreachable` fact and tries one more round later (`discover_host:host-4#2`), and a 404 records that the host doesn't exist and drops it. A failed `compare_service` tries once more later as a new round, and since it makes no network call, repeated failure means bad model output. A failed `verify_drift` schedules another verify round later, and the drift stays `inferred` so it can't count yet. A failed `write_report` is retried, and if it keeps failing the session fails while the verified facts stay in the database. New rounds wait 1 second before they can be claimed (`lha/config.py`, line 13), and every replan decision is written to `events` with its reason.

**A circuit breaker per host is '(later)'.** After 3 failures in a row against the same host, the coordinator would stop creating tasks for that host for a cooldown period (the breaker is 'open'), then let one probe task through, closing the breaker on success and opening it again on failure, which would stop the run wasting its whole budget retrying one broken host.

**Stall detection is '(later)'.** If no task succeeded in the last K steps and nothing is ready, the coordinator would re-open what was put off (hosts whose cooldown ended, drifts still `inferred`), and if there were nothing to re-open the session would fail and the reporter would write a partial report.

## Session lifecycle

The coordinator checks the goal on every loop and not after each model call, and the check is plain code over the facts that asks whether there is a `verified` drift fact backed by a registry fact (`lha/coordinator.py`, lines 460–468). While the session is running, the coordinator keeps adding tasks as new facts unlock them, such as a new host leading to a read of its services. When the goal is met, the coordinator creates the reporter task and cancels the leftover tasks, and the reporter runs only now and reads only verified facts. The session has succeeded once the report is written and accepted (`lha/coordinator.py`, lines 295–316). A stalled session is '(later)', as above. The session fails when the step budget (3000 by default) or the time limit (600 seconds by default) runs out, or when no work is left without the goal being met (`lha/coordinator.py`, lines 417–458; `lha/config.py`, line 32; `lha/run.py`, lines 248–261), and in all of these cases the reporter still runs, because it is exempt from the budget, and writes a partial report from what was verified.

## Fake models

There are no API keys and no spend, because each agent's model is a deterministic Python function plugged in through Pydantic AI's `FunctionModel` (`lha/agents/base.py`, lines 82–111). The agent loop, the tool calling and the output validation are real, and only the 'brain' is scripted. The brain reacts to its input, because it is a rule-based policy (such as 'read every service of my host that isn't in the facts yet') and not a fixed tape, so it adapts when tools fail.

Errors come from two sources. Tool errors come from the mock world, and the agent sees them and reports them. Model errors are made on purpose, where a seeded 5% of final answers are broken (`lha/config.py`, line 23), half of them malformed, meaning a field is missing, which Pydantic AI's output validation rejects, and half fabricated, meaning a valid schema with the wrong content (a replica count off by one, or a host no document names), which only the coordinator's source check catches (`lha/agents/base.py`, lines 96–103). Either way the task is retried and nothing bad reaches the facts.

## Determinism

Faults are pseudo-random but seeded, meaning they look random but are fixed by the seed, so different seeds give different fault patterns. Each fault decision comes from `hash(seed, task_key, attempt, call_no)`, which is a hash of the seed, the `task_key`, the attempt and the call number, and not from one shared random number generator, and the tools send `task_key`, `attempt` and the call number as HTTP headers (`lha/tools.py`, lines 91–97), so the mock network can decide without keeping any state. The roll itself is a few lines, shown here without its docstrings and comments (`lha/faults.py`, lines 19–36).

```python
def roll(seed: int, *parts: object) -> float:
    text = "|".join(str(p) for p in (seed, *parts))
    digest = hashlib.sha256(text.encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def pick_fault(r: float, fault_rate: float) -> str | None:
    if r >= fault_rate:
        return None
    index = int(r / fault_rate * len(FAULT_KINDS))
    return FAULT_KINDS[min(index, len(FAULT_KINDS) - 1)]
```

The roll is a number from 0.0 up to but not including 1.0 (0.0 ≤ x < 1.0), and a call fails when the roll is below the fault rate. It is a dice roll and not a rule about hosts, so `discover_host:host-4` doesn't always fail, because each call gets its own fixed roll and roughly `fault_rate` of all calls fail (0.15 by default, raised by `--chaos`, `lha/config.py`, line 22). Nothing about faults is stored, because the mock network works out each roll from the request headers and the seed it got at startup, and the world itself (the hosts, the services, the drift and the decoys) is also generated from the seed, so a resumed run and the scorer see exactly the same world. The code uses `hashlib` and not Python's `hash()`, because the built-in `hash()` of a string changes on every process start (`PYTHONHASHSEED`). Faults are still visible afterwards, because although the mock network writes nothing to Postgres (it plays the outside world), the tool records every error it got as an event. The roll uses the `task_key` and not the task's `id`, because the `id` is a fresh UUID in every run while `discover_host:host-4` is the same in every run. The `call_no` is the number of the tool call within one attempt (0, 1, 2 and so on), because one `discover_host` attempt makes several HTTP calls (get the host, get each service, fetch each document), and without `call_no` they would all share one roll and fail or succeed together. A retry gets a new roll, because a new `attempt` means a new hash, so a retry can succeed where the first try failed.

What 'same seed, same run' promises is the same world, the same faults for each task attempt and the same outcome (PASS and the same finding). It does not promise an identical event log, because with workers running side by side the order of events still varies, and lease timeouts depend on the wall clock.

## The mock network

The mock network is its own FastAPI service (`lha/world/app.py`), and the tools make real HTTP calls to it, so it is a second boundary between processes. It has three endpoints, get host (its services and documents), get service (its replica count) and fetch document (`lha/world/app.py`, lines 48–76), and there is deliberately no 'list all hosts', so hidden hosts can only be found through documents, while the starting hosts come from the session. The fault middleware, seeded as above, returns 500s, 429s, timeouts and empty 200s, which are the silent failures (`lha/world/app.py`, lines 26–41).

For timeouts, the client is our tool code, meaning the HTTP client (`httpx`) inside a worker that calls the mock network. For a timeout fault the server deliberately waits about 3 s and the client gives up after about 1 s and records a timeout (`lha/config.py`, lines 16 and 19), and keeping the client's limit short means a timeout costs a worker 1 s and not 30 s, so a full run takes about 10 seconds. For decoys, a few healthy services are marked as decoys, and reads from `discover_host` tasks get a stale replica count for them while reads from `verify_drift` get the true one, because the world reads the task type from the `task_key` header (`lha/world/app.py`, lines 65–68). Stale reads are never applied to the drifted service, which is a simplification, because real staleness can hide a drift too. Lastly, a 404 is not a fault, because it means the host or service doesn't exist and that is a valid answer, and telling a real 404 apart from a fault is part of what the tools check (`lha/tools.py`, lines 38–45).

## How the failure modes are handled

**Context going bad as the window fills is handled by never letting the raw log reach a prompt.** Each call gets a fresh `ContextPacket` within a budget (`lha/context.py`, lines 42–94).

**Summaries of summaries drifting is handled by working every summary out again from the facts by code.** The progress line is rebuilt from the database each time (`lha/coordinator.py`, lines 489–526).

**Bad output poisoning the shared state is handled by checking every fact before commit.** Output is checked against its schema, every fact is checked against the tool response it cites, and only `verified` facts count (`lha/coordinator.py`, lines 123–185).

**Tools failing silently is handled by a check on every response.** An empty 200 or a malformed body counts as a failure, and failures are sorted into retryable or permanent (`lha/tools.py`, lines 91–120).

**A worker dying in the middle of a task is handled by leases with heartbeats.** An expired lease puts the task back in the queue for the same role, and the `attempt` fencing drops late results (`lha/db/crud.py`, lines 133–168; `lha/coordinator.py`, lines 397–413).

**The coordinator dying is handled by keeping all state in Postgres.** `--resume` carries on from it (`lha/run.py`, lines 201–245).

**Plans drifting is handled by creating follow-ups only from accepted results.** Retries that run out follow fixed replan rules, stall detection is '(later)', and every plan change is logged with its reason (`lha/coordinator.py`, lines 365–395 and 476–479).

**Agents working at cross purposes is handled by a single writer.** Workers only touch state through task leases and their own results, and the coordinator alone owns the plan and the facts.

## Limits and scaling

As the horizon grows, the coordinator breaks first, because it is one loop that commits every result, so at high throughput it becomes the bottleneck, and the next thing to break is polling, because idle workers poll for ready tasks (`lha/agents/worker.py`, lines 52–63), which `LISTEN/NOTIFY` would replace. Going from 2 to 20 workers, claiming already scales thanks to `SKIP LOCKED` and the index on `(session_id, status, role, created_at)` (`lha/db/models.py`, line 83), and the coordinator stays the single writer per session, so beyond that the plan would have to be split, with one coordinator per partition of subjects (for example per subnet), each with its own advisory lock. For context at scale, packets are per task and scoped by subject, so the prompt size doesn't grow with the number of workers or steps, and what does grow is the reporter's input, which would be summarised by code (counts per partition) and split. For memory at scale, facts are one current row per key and the unique index lines up two writers racing on the same key one after the other, and a larger system would add a version per key and a fact store per partition. None of this is measured, as everything here runs on one machine against one Postgres.

## Layout

```
lha/
  config.py        every tunable number in one place
  ids.py           UUIDv7
  faults.py        seeded dice rolls
  db/
    __init__.py    engine + schema creation
    models.py      SQLAlchemy tables (internal: how rows are stored)
    crud.py        claim_task, heartbeat, submit_result, upsert_fact, ...
  schemas/         Pydantic models (external contracts between processes)
    tasks.py       one input + output schema per task type, task keys, scopes
    facts.py       fact names and statuses
    context.py     ContextPacket
  agents/
    base.py        AgentContext + the scripted FunctionModel
    worker.py      worker process: claim, heartbeat, run, submit
    discovery.py   policy, tools and fabricated-output variant per agent
    analysis.py
    reporter.py
  world/           FastAPI mock network: generator, app, entry point
  tools.py         HTTP tools: validation, retries, one event per call
  context.py       builds a ContextPacket from the database
  coordinator.py   validation, source checks, retries, replanning, goal check
  scoring.py       PASS/FAIL against the world regenerated from the seed
  run.py           supervisor + CLI
tests/             unit tests, Postgres tests, full end-to-end runs
```

Database rows in `db/models.py` are internal, while the Pydantic schemas in `schemas/` are the contract a worker's output must meet before the coordinator commits it, and keeping them apart keeps that checking boundary clear. Each agent has one file, where everything about it (its policy, its tools, its output schema and its fabricated variant) lives together, so adding an agent means adding one file.
