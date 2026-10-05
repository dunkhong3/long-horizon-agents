# Design

## What this is

This document explains how the system works and why it is built the way it is. The README is the short version, and [NOTES.md](../NOTES.md) is the look-back on the decisions, what is still not built and how the project was made. Every claim about the code points to the file and the lines where it happens, so it can be checked.

## The goal the agents pursue

The agents audit a mock cloud deployment for replica drift and write a finding that has been verified. A few words need explaining before anything else. A host is a fake server in the mock network (`host-1`, `host-2` and so on), and hosts are what the agents explore. A service is a program running on a host (`payments`, `search`, `cache`), and its replicas are how many copies of it are running at the same time, much like pods in Kubernetes. The registry is one file, `registry.json`, on one host, which lists how many replicas each service should have, and it is the 'source of truth' the agents compare against. A drift is a service running a different number of replicas than the registry says. A document is a text file on a host (such as `runbook.md`, a team's operations notes), and documents can mention other hosts, which is the only way hidden hosts are found. A stale read is when the mock server answers with an old, out-of-date value, like a cache that hasn't refreshed, and a fresh read is simply asking the same question again with a new call. A decoy is a service that looks drifted because of a stale read but matches the registry on a fresh read. Lastly, the scorer is the code that runs at the end (`lha/scoring.py`, lines 30–63), which builds the world again from the seed and compares the report's findings with the planted drifts, and the supervisor then prints PASS or FAIL (`lha/run.py`, lines 293–318).

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

The agents start knowing only `host-1`, `host-2` and `host-3`, and the example is simplified, because a real run has 20 hosts, between about 65 and 90 services, 3 decoys and one dead reference, which is a document naming a host that doesn't exist (`lha/world/model.py`, lines 88–172). Two more hosts make trouble on purpose (`lha/world/model.py`, lines 121–127). The wide host runs 40 services, which is too many to read in one attempt's context window, so its discovery has to be split into batches, and the outage host is down for every task of the first round, so its circuit breaker opens and a later round has to bring it back. The outage host is always a leaf, meaning a host whose documents name no other host, so no part of the network is only reachable through it. Documents are served in pages of 40 lines (`lha/world/model.py`, lines 33 and 175–187), because in a big world `registry.json` lists hundreds of services and would not fit in any context window in one piece, and it has one entry per line, so every page can be read on its own.

There are two goals, chosen with `--goal` (`lha/run.py`, lines 45–59). The default goal, 'one', plants exactly one drift and is met as soon as that drift is verified, because coverage is a measure we report and not what counts as success, so a run that finds and verifies the drift early is a good run. The 'find all drifts' goal, 'all', plants three drifts (`lha/config.py`, line 49) and is met only when no host is left unexplored and every service has a verdict, meaning a clean compare, a verified drift or a refuted one (`lha/coordinator.py`, lines 786–842). That gives a longer and more predictable horizon, because the run cannot end until it has read everything.

The run is long for three reasons. First, the deepest planted drift sits at the end of a chain of documents that starts at one of the deepest other hosts, 5–6 hops from the start in total, and because discovery works breadth-first the chain is only entered after most of the network has been explored (`lha/world/model.py`, lines 101–119), so the run cannot end in a handful of steps. Second, some services look drifted on the first read because of a stale response but match the registry when checked again, so a claim only counts once a second, independent read agrees with it. Lastly, faults happen everywhere, as tool calls time out, return 500 errors or return an empty 200 (a 'silent success'), the outage host is down, model output is sometimes malformed or made up, workers and the coordinator can be crashed on purpose, and the whole run can be killed in the middle and resumed. Altogether a run takes roughly 250–500 steps at the default fault rate, about 470–530 for the 'find all drifts' goal, and up to about 660 at a 45% fault rate, which is what we measured over runs across many seeds and fault rates, and a run takes 10–15 seconds without injected crashes.

This is not the only task the system could run, because the general parts (the tables, the task queue with its leases and fencing, the context packet builder and the retry rules) know nothing about networks. The domain lives in the mock world, the tools, the agents, the task schemas, and the coordinator's accept handlers and goal check, and a research-brief goal for example would replace those and keep the general parts.

## Scope

The `v1` tag in the repository marks the first core that was built end to end, which is the mock network with its seeded world and faults, the tables with task claiming, leases and fencing, the `ContextPacket` builder, three agents with fake models, verification by two agreeing reads, retries and replanning, the goal check, the reporter, the scorer, and `--kill-at` with `--resume`. Since then the system has gained a benchmark against a naive agent, a circuit breaker per host, stall detection that re-opens work that was put off, the coordinator as its own process with automatic restarts and an advisory lock, injected crashes of workers and of the coordinator, splitting of tasks too big for one attempt, a tool that fetches an earlier raw output by its pointer, the 'find all drifts' goal, documents served in pages, `LISTEN/NOTIFY` instead of polling, a plan split between several coordinators, and a benchmark of all of this at scale. Everything in this document describes the code as it is now.

Some things are designed but not built, and they are named where they come up. A search of `lha/` finds no code for `depends_on` between tasks planned ahead of their inputs, for running the processes on more than one machine, or for a real model, and every model in this repository is a fake one.

## The coordinator, in plain words

The coordinator is an ordinary loop and not an AI (`lha/coordinator.py`, lines 124–155). It is easiest to think of it as a project manager with a to-do board, which is the `tasks` table, and a notebook of findings, which is the `facts` table, and the loop below is what it does over and over.

```
loop:
  1. look at tasks workers just finished
       - output invalid?          → reject it, put the task back (retry)
       - too big for one attempt? → split it into batches
       - output valid?            → save its facts, mark the task done
  2. plan follow-ups from new facts
       - new host found           → "discover host X"
       - service read             → "compare service Y with the registry"
       - mismatch found           → "verify service Y" (read it again)
  3. handle trouble
       - task failed too often    → give up on it, try another route
       - host failing again and again → open its circuit breaker
       - worker went silent       → put its task back on the board
       - nothing moved for a while → re-open work that was put off
  4. check the goal
       - met → create the reporter task, cancel all leftover tasks
  5. stop when the report is written (a partial one if the run ran out)
```

Workers never decide what happens next. They pick a task off the board, do it and hand back a result, and the coordinator decides what that result means.

## Roles and task types

**The coordinator is plain code that owns the plan.** It checks and commits worker output, creates follow-up tasks, retries, splits, replans and decides when the goal is met, and its own output is tasks and logged decisions (`lha/coordinator.py`).

**Discovery is the 'breadth' role.** Given a host name, it reads the host's services and documents and reports what exists, which services run how many replicas and which other hosts the documents mention, and the coordinator records that as facts (`lha/agents/discovery.py`, lines 18–72). A batch of a split discovery reads only its own services, and only the first batch reads the documents.

**Analysis is the 'depth' role.** Given a service and its registry entry, it compares the discovered replica count with the registry and raises a drift hypothesis, and in a verify task it reads the service again with a fresh call and reports the number, which the coordinator uses to move an `inferred` drift towards `verified` or `refuted` (`lha/agents/analysis.py`, lines 19–56).

**The reporter runs once, at the end.** It writes the finding from verified facts only and lists every verified drift with the ID of its fact, and the supervisor saves its summary as `finding.md` (`lha/agents/reporter.py`, lines 17–51; `lha/run.py`, lines 293–318).

Several worker processes run side by side, by default two for discovery, two for analysis and one for the reporter (`lha/run.py`, line 40), and `--discovery` and `--analysis` set how many (`lha/run.py`, lines 374–393). Workers never talk to each other and share findings only through the fact table, and each task's prompt is built from the latest facts at the moment a worker claims it, so nobody misses anyone else's findings.

Every task has a type, and each type belongs to exactly one role (`lha/schemas/tasks.py`, lines 18–23), so a worker only claims tasks of its own role (`WHERE role = :role`), and each type has its own input and output schema (`lha/schemas/tasks.py`, lines 28–130). There are four types. `discover_host` belongs to discovery, takes a host name (and for a batch, its list of services and its part number) and returns the services with their replica counts, the documents and the hosts they mention. `compare_service` belongs to analysis, takes a service and its host, works from the service's read fact and registry entry in its context packet, makes no network call, and returns either a match or a drift claim that the coordinator records as an `inferred` drift fact. `verify_drift` also belongs to analysis, takes the service and host of an `inferred` drift and returns the replica count from a fresh read. `write_report` belongs to the reporter, takes only a flag saying whether the report is partial, works from the verified drift facts in its context packet, and returns a list of findings with a summary.

Tasks are only created once their inputs exist. The coordinator creates a follow-up task from an accepted result, so a task never has to wait for another one, and `compare_service` is only created once both the service's read and the registry entry exist as facts. Until the registry is found no compare work is created, and when the registry fact arrives the coordinator creates a compare for every service already read (`lha/coordinator.py`, lines 314–330). Each task records `parent_task_id`, which is the task whose result created it (`NULL` for the starting tasks, the report task and work re-opened after a stall), and that gives the plan's tree of tasks for auditing.

## Verification ('checked twice')

A drift goes through three stages (`lha/coordinator.py`, lines 352–415). First, discovery's read of the service is read number one, a `compare_service` task compares it with the registry, and a mismatch becomes an `inferred` drift fact, which does not count yet. Second, the coordinator creates a verify task that any analysis worker can pick up, which reads the service again with a fresh call and reports the number it saw without deciding anything. Lastly, the coordinator decides, and two reads must agree, so if the fresh read agrees with the first the drift becomes `verified`, and if they disagree neither read is trusted and a third read breaks the tie (2 out of 3). If the majority matches the registry, the drift becomes `refuted`, which means it was a decoy.

The second read is not automatically the truth, because it could be stale or broken too, and what counts is agreement between independent reads. A read that matches the registry is accepted after one read, and so is the registry itself, and stale reads in the mock world only ever make a service look drifted and never hide a real drift (`lha/world/app.py`, lines 70–73), which is a simplification of the world and not of the checks.

The verify read also updates the service's read fact, superseding the stale one. That does not trigger a second compare, because `compare_service:search@host-3` already exists, so creating it again does nothing (see `task_key` below). Verify rounds are capped at 3 per drift (`lha/config.py`, line 12), and a drift still undecided after that stays `inferred` and never counts, unless stall detection gives it one more round later. Only verified facts can meet the goal or appear in the report, which is what stops one bad read (a stale cache, a silently broken tool, a malformed model output) from ending up as the answer.

## Storage

Every table's primary key is a UUIDv7, generated in Python in a few lines (`lha/ids.py`, lines 13–25), because Python 3.11 and Postgres 16 have no built-in v7. A v7 UUID starts with a timestamp, so sorting by `id` is sorting by creation time while the ids stay globally unique. `created_at` uses `clock_timestamp()` and not `now()` (`lha/db/models.py`, lines 24–26), because `now()` returns the same time for every row in one transaction and the coordinator creates several tasks per transaction.

There are four tables, and every table apart from `sessions` has a `session_id`. The `sessions` table has one row per run, with the goal and its kind, the starting hosts, the seed, the number of hosts and of planted drifts, the fault rate, the crash rate, the number of coordinators, how processes wait for work (`notify` or `poll`), the step budget, the status (`running`, `succeeded` or `failed`) and the accepted report (`lha/db/models.py`, lines 28–47). The `events` table is an append-only log of everything that happens, meaning context packets, model calls, tool calls with their raw responses, errors, injected crashes, fact changes and coordinator decisions, with the columns `id`, `session_id`, `task_id`, `attempt`, `actor`, `kind`, `payload` (jsonb) and `created_at` (`lha/db/models.py`, lines 49–64). Workers, the coordinator and the supervisor (which logs the start and any resume of a session and every restart of the coordinator, `lha/run.py`, lines 161–164, 258 and 289) all write to it, and it is never put into a prompt as a whole. The `tasks` table is the work queue and the plan (`lha/db/models.py`, lines 66–92), and the `facts` table holds the structured findings with where they came from and their status (`lha/db/models.py`, lines 94–117), written only by the coordinator and kept to one current row per `(subject, key)`.

The tables are created with `CREATE TABLE IF NOT EXISTS` when a run starts, together with a trigger on `tasks` that is described under 'Waking up' below (`lha/db/__init__.py`, lines 11–48), and there are no migrations, so a database made by an older version of the code has to be dropped and made again (`just db-down && just db-up`).

## Task queue and leases

Tasks are added during the run as results come in, and a new task is created as `ready`, because its inputs already exist (`lha/db/crud.py`, lines 75–109). Workers claim ready tasks with `SELECT … FOR UPDATE SKIP LOCKED`, which is Postgres's building block for job queues. It is really two instructions, and the skipping is done by the query that is running and not by anyone else. `FOR UPDATE` says 'I am going to change the rows I am selecting, so lock them for me', and until that transaction ends nobody else can lock or change them. `SKIP LOCKED` says 'while I am looking, if a row is already locked by someone else, don't wait for it and leave it out of my results'. When two workers ask at the same moment with tasks A, B and C ready, it goes like this.

```
worker 1: SELECT … LIMIT 1 FOR UPDATE SKIP LOCKED  → gets A (A now locked)
worker 2: SELECT … LIMIT 1 FOR UPDATE SKIP LOCKED  → A is locked → skip → gets B
```

Without `SKIP LOCKED`, worker 2 would wait for worker 1's transaction to finish before moving on, so workers would queue up behind each other, and without `FOR UPDATE` nothing would be locked, so both workers could read A as `ready` and both claim it, and the task would run twice. Together, every worker gets a different task and nobody waits. A task moves from `ready` to `leased` to `submitted` and then to `succeeded`, `failed`, `split` or `cancelled`, and a retry goes back to `ready`. The real claim query is one statement, so one transaction (`lha/db/crud.py`, lines 114–127).

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
RETURNING id, task_key, parent_task_id, type, attempt, input, scope
```

The columns of a task are its `id` (a UUIDv7 such as `0192f7a1-…`), its `session_id` (such as `3f2b9c1e-…`), its `task_key` (such as `discover_host:host-4`, unique per session), its `type` and `role` (such as `discover_host` and `discovery`), its `input` in jsonb (such as `{"host": "host-4"}`), its `scope`, which is what the context builder selects facts for (such as `["host:host-4", "*@host-4"]`), its `parent_task_id`, its `status` (such as `ready`), its `attempt` and `max_attempts` (such as `1` and `3`), its `leased_by` and `lease_expires_at` (such as `discovery-2` and `2026-10-03 14:02:41+00`), its `not_before`, which is the time before which it cannot be claimed (a backoff, a new round's delay or an open circuit breaker), its `partition`, which says which coordinator decides its results (see 'Processes, crashes and resume'), its `result` in jsonb, which is the worker's submitted output before the coordinator accepts it, and its `submitted_at` time, which measures how long a result waits for its coordinator (`lha/db/models.py`, lines 66–92).

The `task_key` makes creating a task idempotent, meaning doing it twice has the same effect as doing it once. Two documents can both mention `host-4`, and a restarted coordinator may repeat a decision it already made, but a unique constraint on `(session_id, task_key)` together with `ON CONFLICT DO NOTHING` means the same task is never created twice (`lha/db/models.py`, line 89; `lha/db/crud.py`, line 106). It is also the stable name used to seed faults (see 'Determinism'). The key is a readable string of the form `<type>:<what it's about>`, built from the task's input (`lha/schemas/tasks.py`, lines 135–157), so discovering a host is `discover_host:host-4`, the second of three batches of a split discovery is `discover_host:host-7/2of3`, comparing a service is `compare_service:cache@host-4`, verifying a drift is `verify_drift:cache@host-4#1` and then `#2` for the tie-break, and writing the report is `write_report`. The `task_key` is not the `id`, because the `id` is new for every row and every run, so it cannot seed faults or catch a duplicate, while the `task_key` describes what the task is and is the same every time, and the `id` is for joins and references. A retry is the same row with `attempt + 1` and the same key, while a deliberate new round of the same work (a tie-break verify, or a host tried again in a second round) gets a `#n` suffix, so it is a new row with a new key.

There is no explicit 'unlock', because a row lock lasts until the transaction ends, and what keeps the task claimed after that is the lease, meaning the status `leased` plus `lease_expires_at`. While a worker runs a task, a background asyncio task inside the worker extends the lease every 3 s, which is shorter than the 10 s lease so one late beat isn't fatal (`lha/config.py`, lines 4–5; `lha/agents/worker.py`, lines 145–152). In SQL terms the heartbeat is the update below (`lha/db/crud.py`, lines 149–166).

```sql
UPDATE tasks SET lease_expires_at = now() + interval '10 seconds'
 WHERE id = :task_id AND leased_by = :worker AND attempt = :attempt
   AND status = 'leased';
```

If a worker dies, its heartbeats stop and the lease expires, and on every loop the coordinator selects the expired leases and sends each task through the same retry rule as any other failure, so it goes back to `ready` with `attempt + 1`, or to `failed` and the replan rules once it has used up `max_attempts` (`lha/coordinator.py`, lines 621–638). When the supervisor sees a worker process exit, it knows the worker is dead, so it sets that worker's lease to expire at once instead of waiting up to 10 s (`lha/run.py`, lines 171–190), and the task still goes through the coordinator's usual rule. If a worker was only slow, its next heartbeat or result submission matches 0 rows because the `attempt` changed, so the worker drops its result, which stops a late 'zombie' worker from overwriting the retry's work. The heartbeat loop is started when the worker claims a task and cancelled when it submits, and `attempt` acts as a fencing token, which is a number that goes up every time the task is handed out, so an old holder can always tell it has been replaced.

```
t=0   worker A claims task T          attempt=1, lease until t=10
t=2   A's tool call hangs; heartbeats stop
t=10  lease expired → coordinator: task T ready, attempt=2
t=11  worker B claims task T          attempt=2
t=15  A wakes up, submits "attempt=1" → matches 0 rows → A discards its result
t=16  B submits "attempt=2"           → accepted
```

`leased_by` alone is not enough, because the same worker could claim the task again later under a newer attempt. Cancelling needs no extra code either. When the goal is met, the coordinator sets every leftover task to `cancelled`, including ones a worker is running right now, and that worker's next heartbeat or submit has the same `AND status = 'leased'` condition, so it matches 0 rows and the worker stops and throws its result away.

### Waking up

Processes don't poll for work. A trigger on `tasks` sends a Postgres notification, which is a short message on a named channel that every connection listening on that channel receives, when a task becomes ready, on the channel `lha_ready` with the session and the role, and when a result is submitted, on the channel `lha_submitted` with the session and the partition (`lha/db/__init__.py`, lines 11–29). A notification sent inside a transaction is only delivered when that transaction commits, so a process that is woken always finds the row it was told about. Each worker and each coordinator holds one extra connection that listens for its own role or partition (`lha/db/notify.py`, lines 23–54). A worker that finds no work waits for a notification, or until the next backed-off task becomes due, because nothing announces the end of a backoff, and never longer than 1 s (`lha/agents/worker.py`, lines 80–88; `lha/db/crud.py`, lines 135–146), and a coordinator waits for a submitted result or 0.5 s, so it still sweeps expired leases and checks the goal (`lha/coordinator.py`, lines 124–155; `lha/config.py`, lines 46–47). `--poll` turns this off, so every process looks for work every 50 ms as it did before, which the scale benchmark uses for comparison.

## Who writes what

Workers write `events` (every model call and tool call, tagged with `task_id` and `attempt`) and their own task's `result`, and they never write facts, which keeps 'LLMs propose, code decides' true at the database level. A worker submits with one fenced update that sets `status = 'submitted'` and `result = <output>` where `id`, `attempt` and `status = 'leased'` still match (`lha/db/crud.py`, lines 169–184), and a failed attempt is submitted the same way with `result = {"error": {"kind": "timeout", ...}}` (`lha/agents/worker.py`, lines 154–203).

The coordinator then checks `result` against the task type's schema and checks every claimed fact against its source (`lha/coordinator.py`, lines 235–291). A read must cite a tool-call event from this same attempt, and the value must appear in that event's raw response (`lha/coordinator.py`, lines 445–467 and 1024–1029). A discovery must also report every service the host listed and every page of every document, or for a batch exactly the services, documents or page it was given, so an attempt cannot quietly leave something out (`lha/coordinator.py`, lines 1013–1021). A drift claim must cite the read fact and the registry fact it compares, and the coordinator does the comparison again itself (`lha/coordinator.py`, lines 352–379), and a report must list exactly the verified drifts, each matching its fact (`lha/coordinator.py`, lines 417–441). A made-up host or number fails these checks. Only then, in one transaction, does it write the facts, mark the task `succeeded`, create the follow-up tasks and log the decision in `events`, while an error result or an invalid one goes through the retry rules instead. If the coordinator crashes halfway, the transaction rolls back and the task is still `submitted`, so after a restart it is simply processed again, and there is never a state where facts exist but their follow-up tasks don't.

## Facts

Raw tool output goes into `events` in full, and that is what pointers refer to. The agent's result lists the facts it read, each citing the event it came from, and the coordinator checks them as above. A fact is one small, typed claim with an `id` (a UUIDv7 such as `0192f7a3-…`), a `session_id` (such as `3f2b9c1e-…`), a `subject` (such as `service:cache@host-4`), a `key` (such as `config.replicas`), a `value` in jsonb (such as `1`), a `status` (`observed`, `inferred`, `verified`, `refuted` or `superseded`), a `source_task_id` and `source_event_id` saying which task and which tool call produced it, an `evidence` list in jsonb holding the reads behind a drift fact, and a `created_at` time (such as `2026-10-03 14:02:11+00`).

The fact names used in this domain are `host:<h>` (with the keys `exists`, `listing`, `unreachable` and `breaker`), `service:<s>@<h>` (with `config.replicas`, `drift.replicas` and `verdict`), `doc:<name>@<h>` (with `mentions` and `pages`, and `doc:<name>#<page>@<h>` with `mentions` for each later page) and `registry:<s>` (with `replicas`) (`lha/schemas/facts.py`). A host's `listing` is the services and documents it listed when it was read, which is how the 'find all drifts' goal knows whether everything on a host has been read, and its `breaker` is the state of its circuit breaker. Each service has two main kinds of fact. A read has `key = config.replicas`, a value such as `1` and the status `observed`, and a drift claim has `key = drift.replicas`, a value such as `{"expected": 3, "actual": 1}` and a status that moves from `inferred` to `verified` or `refuted`.

Facts are never deleted. A fact's `status` can change, for example from `inferred` to `verified` or to `superseded`, and every change is also written to `events` (`lha/db/crud.py`, lines 224–334), so the full history is always there, with `events` being the strictly append-only table. The current facts are the ones with `status <> 'superseded'`, and `refuted` facts stay visible, because 'we checked this and it was wrong' is useful knowledge.

The same fact means the same `(session_id, subject, key)`, and a partial unique index lets the database itself guarantee at most one current fact per key (`lha/db/models.py`, lines 107–116).

```sql
CREATE UNIQUE INDEX facts_one_current
    ON facts (session_id, subject, key)
 WHERE status <> 'superseded';
```

The index is a rule and not storage, so it doesn't keep, sort or return rows, and all it does is make Postgres refuse any write that would leave two current rows for the same key. Reading the current fact therefore needs no `ORDER BY created_at DESC LIMIT 1`, and the query below returns at most one row, with the index making it fast (`lha/db/crud.py`, lines 209–217).

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

Inserting another `superseded` row for this key is allowed, but inserting another `observed` row while row B is current fails with the Postgres error `duplicate key value violates unique constraint "facts_one_current"`, so replacing a fact is one transaction that marks the old row `superseded` and then inserts the new one. When the coordinator writes a fact, the same value does nothing, so retries change nothing, a different value marks the old fact `superseded` and inserts the new one, and a new value that contradicts a `verified` fact is never written silently, because `upsert_fact` raises `FactConflict` (`lha/db/crud.py`, lines 242–245) and the coordinator treats the whole result as rejected and retries the task (`lha/coordinator.py`, lines 248–257).

When the coordinator creates a task it sets its scope, which is the list of subjects the task is about, such as `host:host-3` and everything on that host, or for a batch of a split discovery only its own services (`lha/schemas/tasks.py`, lines 160–176). The context builder selects the current facts whose subject is in scope, with a plain SQL filter and no embeddings or similarity search, and a compare or verify task's scope already names the service's registry entry, while for the reporter it also adds the registry entry and the latest read behind each verified drift (`lha/context.py`, lines 112–147).

The tables grow, and that is fine, because it is storage and not prompt, and Postgres handles millions of rows. Facts grow with the size of the world and not with the number of steps, because a newer fact with the same `(subject, key)` supersedes the old one. Retention and archiving are out of scope for this project.

A step is one model call or one tool call. Each one is logged as an event, so the step count is a count of those events and is the same from every process (`lha/db/crud.py`, lines 24–25 and 55–57), while heartbeats are not events.

## Processes, crashes and resume

`just start` runs a supervisor (`lha/run.py`), which starts the mock network, the coordinators and the worker processes as separate operating system processes and then watches them (`lha/run.py`, lines 92–214). Every 50 ms it starts again any worker that died, starts a coordinator again if it crashed, checks `--kill-at`, and prints a progress line every 2 s (`lha/run.py`, lines 130–152). The coordinator is its own program (`lha/coordinator.py`, lines 1032–1044), and it ends with exit code 0 once the session is over, which is how the supervisor knows the run has finished.

The plan can be split between several coordinators with `--coordinators`, and each one is a partition, meaning it decides the results of only the tasks marked with its number (`lha/coordinator.py`, lines 173–184 and 621–638). A task's partition is a stable hash of its host (`lha/schemas/tasks.py`, lines 179–190), so every fact about a host, its services and its documents is written by one coordinator, which keeps the rule of one writer per fact. Partition 0 is the leader, which also checks the goal, the budget and stalls, re-opens work and creates the report, while the others only process results and sweep their own expired leases (`lha/coordinator.py`, lines 722–729). Only one thing crosses partitions, which is the registry, because a read creates its compare only if the service's registry fact exists, and the registry creates compares for every read that exists, so a read and the registry committed by two coordinators at the same moment could each miss the other. Every discovery therefore takes a transaction-level advisory lock in shared mode, and the one that found registry entries takes it exclusively, so whichever commits second sees what the first committed (`lha/coordinator.py`, lines 338–350).

Before it does anything, each coordinator takes a Postgres advisory lock for its session and partition (`lha/coordinator.py`, lines 110–171). An advisory lock is a named lock that is not tied to any table, which one connection holds and which is released automatically when that connection closes, so a coordinator that dies, however it dies, lets go of it. The key is taken from the session ID and the partition, so nothing extra is stored, as in `pg_try_advisory_lock(hashtextextended('coordinator:' || :session_id || ':' || :partition, 0))`. A restarted coordinator that starts while an old one has not fully died waits for the lock, for up to 30 s (`lha/config.py`, line 54), so two coordinators never work on the same partition at the same time.

A coordinator that crashes is started again, and because its state is all in Postgres the new one simply picks up the `submitted` results and the expired leases. If one crashes 3 times in a row without a single decision being committed in between (`lha/config.py`, line 53), the supervisor treats that as a bug, such as one result that always crashes it, and not as bad luck, so it stops everything with exit code 2 and leaves the session `running` for `--resume` after the fix (`lha/run.py`, lines 154–169).

Crashes can be injected on purpose with `--crashes`, which is the share of task attempts that crash a process, and every injected crash is seeded like the other faults. A worker crashes after it has done the work and before it submits, which is the most wasteful moment, and half of these crashes make it exit holding its lease, while the other half make it hang, staying alive but silent for longer than its lease, after which it tries to submit and is fenced out (`lha/agents/worker.py`, lines 104–143). The coordinator crashes after it has written a result's facts and follow-ups and before it commits, so the transaction rolls back and the next coordinator does that result again (`lha/coordinator.py`, lines 188–233). A result crashes a coordinator at most once, which is recorded in `events` in a transaction of its own, because an injected crash stands for bad luck and must not look like a crash loop.

`--kill-at N` makes the supervisor kill every process with SIGKILL once the step count reaches N and then exit abruptly with `os._exit` (`lha/run.py`, lines 353–360), which is the harshest crash, because tasks in flight are lost in the middle of their lease. `--resume` takes a session ID, or finds the latest unfinished session when none is given (`lha/run.py`, lines 266–274 and 326–337), starts everything again with that session's seed and settings, and carries on from Postgres. No worker from the old run is alive, so all of its leases are released straight away (`lha/run.py`, lines 277–290), and `submitted` results are processed. Nothing is replayed from memory, because nothing important lives in memory.

## Context packets

A `ContextPacket` is a Pydantic model built in memory at the start of each attempt at a task, from a fresh database query (`lha/schemas/context.py`; `lha/context.py`, lines 96–109), and it is the prompt for every model call in that attempt, where the later calls see the same packet plus the results of the attempt's own tool calls (`lha/agents/base.py`, lines 153–172). It has four layers. The pinned layer holds the goal, what counts as done and this task's spec, and it is always included. The facts layer holds only the facts relevant to this task, such as this host or this service. The recent layer holds the last few events of this task, up to 5 in total, made of the coordinator's decisions about it (which carry the reason the previous attempt failed) and any lost leases (`lha/config.py`, line 39; `lha/context.py`, lines 150–167). The pointers layer says where raw outputs this task may reuse are, as the event ID, the tool and the path of each, but leaves the outputs themselves out (`lha/context.py`, lines 170–198).

A task may reuse the successful outputs of its own earlier attempts, and a batch of a split discovery may also reuse the outputs of the attempt that was too big, which had already read part of what the batch is about to read (`lha/db/crud.py`, lines 187–199), and a batch is only pointed at reads of its own services, documents and pages and holds only its own services' facts, so its pointers fit in its budget (`lha/context.py`, lines 170–215; `lha/schemas/tasks.py`, lines 160–176). The discovery agent fetches such an output with the `fetch_pointer` tool instead of calling the network again (`lha/agents/discovery.py`, lines 18–72), and the tool reads it from Postgres, so it has no faults, and logs the copy as a tool call of its own, so a fact can cite it (`lha/tools.py`, lines 80–94). The coordinator does not take the copy on trust, because before a copy counts as a source it checks that the original event exists, belongs to this task or its split parent, succeeded, and has exactly the same response (`lha/coordinator.py`, lines 445–486). A verify task never uses pointers, because its whole point is a fresh read.

The four layers are in order of priority from highest to lowest. The window is 2000 tokens, with 400 kept back for the model's answer and 800 for the attempt's own tool calls and results, which leaves 800 for the packet (`lha/config.py`, lines 36–38), and when the packet would be over that budget it is cut from the bottom up, so pointers go first, then the oldest recent events, then the oldest facts, and pinned is never cut. In practice the packet is built from the top down instead of being trimmed (`lha/context.py`, lines 57–93). The budget is the context window minus the tokens kept back, pinned goes in first, and then whole items go in by priority (facts newest first, then recent events newest first, then pointers) for as long as the next item still fits, with nothing ever cut in half and whatever was left out recorded in the packet's `omitted` counts, which are logged with the packet. As an example, with a budget of 10 and pinned taking 4, facts 4, recent 2 and pointers 1, the total would be 11, so pinned, facts and recent fit (10) and the pointer is left out, which gives the same result as 'drop the lowest item' but without a loop of cutting and recounting. Token counts are estimated at about 4 characters per token (`lha/context.py`, lines 50–54), which is fine for fake models, while a real model would use its own tokeniser.

A task can still be too big in two ways. If the pinned layer alone doesn't fit the packet's budget, the worker fails the attempt with a permanent `context_overflow` error (`lha/context.py`, lines 67–69), because cutting pinned would silently change what the task is. More often the attempt's own tool results fill the window, as when discovery reads the wide host's 40 services one after another, and the fake model checks the size of everything it is shown before every call and raises the same error once it is over 1600 tokens (`lha/agents/base.py`, lines 121–125). The coordinator then splits the discovery into batches (`lha/coordinator.py`, lines 526–581 and 1000–1010), with 8 services in each batch of services and one batch for each document (`lha/config.py`, line 23), taking what the host lists from the failed attempt's own `get_host` response in `events` and not from anything the worker claimed, marks the original task `split`, and the batches run as tasks of their own. A document still too big for one attempt, such as the registry of a big world, is split again into one batch per page, where the number of pages comes from the failed attempt's own response for the first page. A batch of services or a single page that still doesn't fit is not split again and fails for good.

A packet is used for one attempt only and then thrown away. A copy is logged to `events` (`lha/agents/worker.py`, line 181), so we can always see exactly what an agent saw, but the next attempt never starts from it and builds a fresh packet instead, because other workers have added facts since then, because the next attempt is usually a different task with a different scope, and because if the old packet held a bad or stale fact, rebuilding from the database picks up the correction while reusing the packet would carry the error forward. On a retry, the new packet's recent layer includes the reason the previous attempt failed, so the prompt grows with the task and not with the run's history.

Compaction, meaning shrinking what the model sees, happens on the prompt and never on the log, and it happens in three places. When raw tool output is written it is turned into facts, when a packet is read it goes through a relevance query and the budget, and any summary (such as the progress line, or the count of omitted facts) is worked out again by code from the facts (`lha/coordinator.py`, lines 958–997), so we never summarise a summary.

## Retries and replanning

The coordinator follows plain rules, applied in order.

**Tools retry first.** A short-lived tool error (a timeout, a 500 or 503, a 429, an empty 200 or a malformed body) is retried up to 2 more times inside the attempt, each try with a new `call_no` and so a new fault roll, and every try is logged (`lha/tools.py`, lines 96–116; `lha/config.py`, line 27). Only if it keeps failing does the attempt fail and reach the coordinator.

**Every failure is sorted into retryable or permanent.** Timeouts, 500s, 429s, empty 200s, invalid or rejected model output and expired leases are retryable, so the task goes back to `ready` after a backoff of `not_before = now() + 0.25 × 2^attempt s` (`lha/coordinator.py`, lines 501–524; `lha/config.py`, line 9). A 404, meaning the host or service doesn't exist, and a `context_overflow` are permanent, so the task fails straight away because retrying won't help (`lha/coordinator.py`, line 77), except that a discovery that overflows is split first, as above.

**Each task gets `max_attempts = 3`.** After that it is `failed` (`lha/config.py`, line 8), and nothing downstream was ever created from it, because follow-ups only come from accepted results.

**A task that failed for good is replanned by a fixed rule per task type** (`lha/coordinator.py`, lines 583–619). A failed `discover_host` records a `host unreachable` fact and tries one more round later (`discover_host:host-4#2`), and a 404 records that the host doesn't exist and drops it. A failed `compare_service` tries once more later as a new round, and since it makes no network call, repeated failure means bad model output. A failed `verify_drift` schedules another verify round later, up to the cap of 3 rounds, and the drift stays `inferred` so it can't count yet. A failed `write_report` is retried, and if it keeps failing the session fails while the verified facts stay in the database. A task too big even after splitting is given up. New rounds wait 1 second before they can be claimed (`lha/config.py`, line 13), and every replan decision is written to `events` with its reason.

**Each host has a circuit breaker.** A circuit breaker is a rule that stops sending work to something that keeps failing, so the run doesn't waste its budget retrying one broken host. The coordinator counts failed network attempts in a row against each host, from discovery and verify tasks failing with a network error, and after 3 of them it opens the host's breaker for 3 seconds (`lha/config.py`, lines 16–17; `lha/coordinator.py`, lines 667–691). While the breaker is open, the task that tripped it, or its next round, waits until the cooldown ends and then goes first as the probe, which is the one task that tests whether the host is back, and every other task for the host, including new ones, waits another cooldown on top (`lha/coordinator.py`, lines 642–665). The next successful task for the host closes the breaker and releases everything it held (`lha/coordinator.py`, lines 693–718). The outage host shows the breaker at work in most runs, because its first-round discovery fails three attempts in a row, the breaker opens, and the second round, which goes as the probe, finds it back, unless the goal is met before the third attempt comes round.

**Stall detection re-opens work that was put off.** When no work is left and the goal is not met, or when no result has been accepted for 200 steps (`lha/config.py`, line 20; `lha/coordinator.py`, lines 848–864), the coordinator re-opens what was put off before it gives up (`lha/coordinator.py`, lines 866–925). That is one more round for hosts given up as unreachable, for drifts still `inferred` and for services that never got a verdict, plus batches for whatever a host listed but nobody has read, as long as nothing is already working on them. A re-opened task has a round past the usual cap, so its `task_key` is new, and a second re-open of the same thing does nothing because its key already exists, so the run can't loop. If there is nothing to re-open, the session ends and the reporter writes a partial report.

## Session lifecycle

The leader coordinator checks the goal at most every half second and not after each model call, because the check reads every current fact, and the check is plain code over the facts (`lha/config.py`, line 48; `lha/coordinator.py`, lines 774–842). For the 'one' goal it asks whether there is a `verified` drift fact backed by a registry fact, and for the 'all' goal it asks whether the registry has been found, every host any document mentions has been read or found not to exist, everything each host listed has been read, and every service read has a verdict. While the session is running, the coordinator keeps adding tasks as new facts unlock them, such as a new host leading to a read of its services. When the goal is met, the coordinator creates the reporter task and cancels the leftover tasks, and the reporter runs only now and reads only verified facts. The session has succeeded once the report is written and accepted with at least one finding (`lha/coordinator.py`, lines 417–441). The session fails when the step budget (3000 by default) or the time limit (600 seconds by default) runs out, or when the run stalls with nothing to re-open (`lha/coordinator.py`, lines 722–772; `lha/config.py`, line 44; `lha/run.py`, lines 374–393), and in all of these cases the reporter still runs, because it is exempt from the budget, and writes a partial report from what was verified.

## Fake models

There are no API keys and no spend, because each agent's model is a deterministic Python function plugged in through Pydantic AI's `FunctionModel` (`lha/agents/base.py`, lines 110–145). The agent loop, the tool calling and the output validation are real, and only the 'brain' is scripted. The brain reacts to its input, because it is a rule-based policy and not a fixed tape, so for example the discovery policy reads every service and document of its host (or of its batch) that it hasn't read yet in this attempt, one call at a time, and fetches by pointer whatever an earlier attempt already read (`lha/agents/discovery.py`, lines 18–72), and the compare policy works from the facts in its packet (`lha/agents/analysis.py`, lines 19–38). It does not try to work around a failing tool, because a tool that keeps failing ends the attempt and the coordinator's retry rules take over. A real model could sit behind the same `Agent` interface, and the code does not include one.

Errors come from two sources. Tool errors come from the mock world, and the model never sees them, because the tool raises an error and the worker submits that error as the attempt's result (`lha/agents/worker.py`, lines 154–203). Model errors are made on purpose, where a seeded 5% of final answers are broken (`lha/config.py`, line 33), half of them malformed, meaning a field is missing, which Pydantic AI's output validation rejects, and half fabricated, meaning a valid schema with the wrong content (a replica count off by one, or a host no document names), which only the coordinator's source check catches (`lha/agents/base.py`, lines 128–135). Either way the task is retried and nothing bad reaches the facts.

## Determinism

Faults are pseudo-random but seeded, meaning they look random but are fixed by the seed, so different seeds give different fault patterns. Each fault decision comes from `hash(seed, task_key, attempt, call_no)`, which is a hash of the seed, the `task_key`, the attempt and the call number, and not from one shared random number generator, and the tools send `task_key`, `attempt` and the call number as HTTP headers (`lha/tools.py`, lines 118–147), so the mock network can decide without keeping any state. Injected crashes are rolled in the same way, from the seed, the `task_key` and the attempt (`lha/agents/worker.py`, lines 135–143; `lha/coordinator.py`, line 209). The roll itself is a few lines, shown here without its docstrings and comments (`lha/faults.py`, lines 19–36).

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

The roll is a number from 0.0 up to but not including 1.0 (0.0 ≤ x < 1.0), and a call fails when the roll is below the fault rate. It is a dice roll and not a rule about hosts, so `discover_host:host-4` doesn't always fail, because each call gets its own fixed roll and roughly `fault_rate` of all calls fail (0.15 by default, set with `--chaos`, `lha/config.py`, line 32). Nothing about faults is stored, because the mock network works out each roll from the request headers and the seed it got at startup, and the world itself (the hosts, the services, the drifts, the decoys, the wide host and the outage host) is also generated from the seed, so a resumed run and the scorer see exactly the same world. The code uses `hashlib` and not Python's `hash()`, because the built-in `hash()` of a string changes on every process start (`PYTHONHASHSEED`). Faults are still visible afterwards, because although the mock network writes nothing to Postgres (it plays the outside world), the tool records every error it got as an event. The roll uses the `task_key` and not the task's `id`, because the `id` is a fresh UUID in every run while `discover_host:host-4` is the same in every run. The `call_no` is the number of the tool call within one attempt (0, 1, 2 and so on), because one `discover_host` attempt makes several HTTP calls (get the host, get each service, fetch each document), and without `call_no` they would all share one roll and fail or succeed together. A retry gets a new roll, because a new `attempt` means a new hash, so a retry can succeed where the first try failed.

What 'same seed, same run' promises is the same world, the same faults for each task attempt and the same outcome (PASS and the same findings). It does not promise an identical event log, because with workers running side by side the order of events still varies, and lease timeouts depend on the wall clock.

## The mock network

The mock network is its own FastAPI service (`lha/world/app.py`), and the tools make real HTTP calls to it, so it is a second boundary between processes. It has three endpoints, get host (its services and documents), get service (its replica count) and fetch document, which returns one page and the number of pages (`lha/world/app.py`, lines 52–87), and there is deliberately no 'list all hosts', so hidden hosts can only be found through documents, while the starting hosts come from the session. The fault middleware, seeded as above, returns 500s, 429s, timeouts and empty 200s, where the empty 200s are the silent failures, and it answers 503 for the outage host to every task of the first round, meaning every task key without a `#n` round suffix (`lha/world/app.py`, lines 27–46).

For timeouts, the client is our tool code, meaning the HTTP client (`httpx`) inside a worker that calls the mock network. For a timeout fault the server deliberately waits about 3 s and the client gives up after about 1 s and records a timeout (`lha/config.py`, lines 26 and 29), and keeping the client's limit short means a timeout costs a worker 1 s, so a full run takes about 10–15 seconds. For decoys, a few healthy services are marked as decoys, and reads from `discover_host` tasks get a stale replica count for them while reads from `verify_drift` get the true one, because the world reads the task type from the `task_key` header (`lha/world/app.py`, lines 70–73). Stale reads are never applied to a drifted service, which is a simplification, because real staleness can hide a drift too. Lastly, a 404 is not a fault, because it means the host or service doesn't exist and that is a valid answer, and telling a real 404 apart from a fault is part of what the tools check (`lha/tools.py`, lines 41–48 and 131–132).

## How the failure modes are handled

**Context going bad as the window fills is handled by never letting the raw log reach a prompt.** Each attempt gets a fresh `ContextPacket` within a budget (`lha/context.py`, lines 57–109), and a task whose own work doesn't fit is split (`lha/coordinator.py`, lines 526–581).

**Summaries of summaries drifting is handled by working every summary out again from the facts by code.** The progress line is rebuilt from the database each time (`lha/coordinator.py`, lines 958–997).

**Bad output poisoning the shared state is handled by checking every fact before commit.** Output is checked against its schema, every fact is checked against the tool response it cites, a copy fetched by pointer is checked against its original, and only `verified` facts count (`lha/coordinator.py`, lines 235–291 and 445–486).

**Tools failing silently is handled by a check on every response.** An empty 200 or a malformed body counts as a failure, and failures are sorted into retryable or permanent (`lha/tools.py`, lines 118–147).

**A host that keeps failing is handled by its circuit breaker.** Work for it is held back for a cooldown and one probe goes first (`lha/coordinator.py`, lines 642–718).

**A worker dying or hanging in the middle of a task is handled by leases with heartbeats.** An expired lease puts the task back in the queue for the same role, and the `attempt` fencing drops late results (`lha/db/crud.py`, lines 149–184; `lha/coordinator.py`, lines 621–638).

**The coordinator dying is handled by keeping all state in Postgres.** The supervisor starts it again, an advisory lock keeps it to one per session, and a crash loop stops the run instead of spinning (`lha/run.py`, lines 130–169), while `--resume` carries on after the whole run was killed (`lha/run.py`, lines 321–371).

**Plans drifting is handled by creating follow-ups only from accepted results.** Retries that run out follow fixed replan rules, stall detection re-opens what was put off, and every plan change is logged with its reason (`lha/coordinator.py`, lines 583–619, 866–925 and 929–941).

**Agents working at cross purposes is handled by a single writer.** Workers only touch state through task leases and their own results, and the coordinator alone owns the plan and the facts.

## The benchmark

To show that the design matters, we compare the system with a naive baseline, which is the usual way an agent is built, meaning one agent whose prompt is its whole history (`lha/baseline.py`). It is one Pydantic AI agent run, where every tool call and every result stays in the run's message history and the whole history is sent to the model again on every call, so the prompt grows with the run. It audits the same mock network with the same tools, the same seeded faults and the same seeded model errors, and its fake model follows the obvious plan, which is to visit every host it knows of, read everything, compare each read with the registry and report the first mismatch (`lha/baseline.py`, lines 186–231). It trusts what it reads, keeps all of its state in the prompt and has no coordinator checking its work, so it needs no Postgres and a crash loses everything. It also has no second round, so to it the outage host is down for good, which costs it nothing in the 'one' goal, because the outage host is a leaf and never on the way to the drift.

We run the baseline in three variants. The plain 'naive' variant believes the first mismatch it sees. The 'naive + re-read' variant reads a mismatch once more before believing it, and its reads use the same task keys as the system's, so a re-check gets a fresh answer in the same way a verify task does and the faults are rolled in the same way (`lha/baseline.py`, lines 272–323). The third variant also cuts the history down to a 2000-token window, which is the same window the system's attempts get, by keeping the goal and dropping the oldest messages first, which is what a chat loop does when it runs out of room (`lha/baseline.py`, lines 89–108).

`just bench` runs the system and all three variants on the same worlds, 5 seeds each at 20 and 60 hosts with the default fault rate and the 'one' goal, and every run gets its own processes (`lha/bench.py`). The size of a prompt is estimated in the same way for both, by counting every part the model is shown in a call (`lha/agents/base.py`, lines 90–107), and a repeated read is a successful network read of something that had already been read successfully, apart from deliberate re-checks (`lha/scoring.py`, lines 150–164). The table below is what one run of the benchmark printed, which took about 6 minutes, where 'wrong' is a report naming the wrong service and 'none' is a run that ended without naming any, and the step and token columns are averages per run except for the largest prompt.

```
| hosts | variant                            | pass | wrong | none | steps | prompt mean | prompt max | prompt total | repeated reads |
|-------|------------------------------------|------|-------|------|-------|-------------|------------|--------------|----------------|
| 20    | system                             | 5/5  | 0     | 0    | 445   | 425         | 1,595      | 111,892      | 2              |
| 20    | naive                              | 0/5  | 5     | 0    | 80    | 1,242       | 2,741      | 51,053       | 0              |
| 20    | naive + re-read                    | 5/5  | 0     | 0    | 260   | 3,096       | 5,993      | 366,250      | 0              |
| 20    | naive + re-read, 2000-token window | 1/5  | 0     | 4    | 2,517 | 1,935       | 2,000      | 2,242,402    | 1,025          |
| 60    | system                             | 5/5  | 0     | 0    | 930   | 326         | 1,598      | 180,223      | 0              |
| 60    | naive                              | 0/5  | 5     | 0    | 191   | 2,903       | 6,647      | 284,856      | 0              |
| 60    | naive + re-read                    | 5/5  | 0     | 0    | 597   | 7,272       | 13,623     | 1,977,628    | 0              |
| 60    | naive + re-read, 2000-token window | 1/5  | 0     | 4    | 2,462 | 1,920       | 2,000      | 2,199,942    | 1,083          |
```

Three things stand out. First, the naive baseline gives a wrong answer in every run, because the first mismatch it reads is almost always a decoy, since the planted drift sits at the far end of the network, and that is bad output poisoning the shared state in its simplest form. Second, re-reading fixes that, and with unlimited history the baseline then finds the drift in every run, with fewer steps than the system because it has no compare tasks, no splitting and no coordinator in between, but every call sends the whole history, so at 60 hosts the average prompt is about 7,300 tokens against about 330 for the system, and a whole run sends about 2 million tokens against about 180,000, which is roughly 11 times more. The system's prompts never go over its 1600-token limit however long the run is, because a task that would need more is split, while the baseline's prompt grows with every step, so its total grows with the square of the run's length. Lastly, once the history has to fit a window, the baseline loses track of what it has already done, because the reads of the first hosts and the registry fall out of the window, so it visits hosts again and reads the registry again, and it repeats about 1,000 reads per run and uses up its 3000-step budget without an answer in 8 runs out of 10, while the system repeats almost no reads, because a retry or a batch fetches what was already read by pointer.

The benchmark is kind to the baseline in one way, which we state plainly. A fake model reads a long prompt perfectly, while a real model gets worse at using what is in its prompt as the prompt grows, so with a real model the unlimited-history variant would also start to miss things, and the gap measured here is the smallest it would be. What the benchmark does measure exactly is the size and the cost of the prompts, the work that is repeated once the history is cut, and the wrong answers that come from trusting a single read.

## Limits and scaling

`just scale` runs one big world, 200 hosts with the 'find all drifts' goal, over and over with more and more workers and coordinators, with polling and with `LISTEN/NOTIFY` (`lha/scale.py`). Every run has the same world, the same faults and the same 2,771 steps, so the differences come from the coordination alone. A decision's latency is how long a submitted result waited before its coordinator decided it, worked out from `submitted_at` (`lha/coordinator.py`, lines 929–941), and an empty claim is a worker asking for work and finding none, which every worker counts and logs when it exits (`lha/agents/worker.py`, lines 56–78). The table below is what one run printed on a machine with 4 CPU cores.

```
| workers | coordinators | wake-ups | pass | steps | elapsed | steps/s | decision p50 | decision p95 | empty claims per worker-second |
|---------|--------------|----------|------|-------|---------|---------|--------------|--------------|--------------------------------|
| 2+2     | 1            | notify   | yes  | 2,771 | 41.3 s  | 67      | 15 ms        | 41 ms        | 2.7                            |
| 8+8     | 1            | notify   | yes  | 2,771 | 21.0 s  | 132     | 310 ms       | 580 ms       | 5.4                            |
| 8+8     | 1            | poll     | yes  | 2,771 | 19.8 s  | 140     | 296 ms       | 613 ms       | 7.9                            |
| 8+8     | 4            | notify   | yes  | 2,771 | 21.5 s  | 129     | 50 ms        | 194 ms       | 4.9                            |
| 16+16   | 1            | notify   | yes  | 2,771 | 29.0 s  | 96      | 2382 ms      | 3747 ms      | 4.5                            |
| 16+16   | 4            | notify   | yes  | 2,771 | 25.8 s  | 107     | 827 ms       | 2636 ms      | 2.9                            |
```

Four things stand out. First, going from 2+2 to 8+8 workers halves the time a run takes, from about 41 s to about 21 s, while the single coordinator's decisions start to queue up, from 15 ms to about 300 ms at the median. Second, splitting the plan between 4 coordinators brings that back down to about 50 ms with the same total time, which shows that the coordinator is the first thing to saturate and that partitions are the way past it. Third, at 16+16 workers the run gets slower again and decisions wait for seconds, because 33 worker processes and the coordinators share 4 CPU cores and all of them wait for the CPU, and partitions still help (a median of about 0.8 s against 2.4 s), but here the machine is the limit and not the design. Lastly, `LISTEN/NOTIFY` cuts the empty claims, from about 8 to about 5 per worker per second with 8+8 workers, with no difference in total time, and the gain is small because workers are busy most of the time and because one ready task wakes every idle worker of its role while only one of them gets it, whereas an idle worker that polls asks 20 times a second.

For context at scale, packets are per task and scoped by subject, so the prompt size doesn't grow with the number of workers or steps, as the benchmark shows, and the parts of the world that do grow, such as a host with many services or a registry with hundreds of entries, are split into batches and pages. What still grows is the reporter's input when there are many drifts, which would be summarised by code (counts per partition) and split. For memory at scale, facts are one current row per key, written by the one coordinator that owns the host, and the unique index lines up two writers racing on the same key one after the other. Everything here runs on one machine against one Postgres, and the next limits are the number of connections, since every process holds a few (the docker compose Postgres allows 300), and Postgres itself.

## Layout

```
lha/
  config.py        every tunable number in one place
  ids.py           UUIDv7
  faults.py        seeded dice rolls
  db/
    __init__.py    engine, schema creation, the notify trigger
    models.py      SQLAlchemy tables (internal: how rows are stored)
    crud.py        claim_task, heartbeat, submit_result, upsert_fact, ...
    notify.py      LISTEN/NOTIFY wake-ups
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
  tools.py         HTTP tools: validation, retries, one event per call, pointers
  context.py       builds a ContextPacket from the database
  coordinator.py   coordinator process: checks, retries, splits, breaker, stalls, goal
  scoring.py       PASS/FAIL against the world regenerated from the seed
  run.py           supervisor + CLI
  baseline.py      the naive full-history agent we compare against
  bench.py         runs the system and the baseline on the same worlds
  scale.py         one big world with more and more workers and coordinators
tests/             unit tests, Postgres tests, full end-to-end runs
```

Database rows in `db/models.py` are internal, while the Pydantic schemas in `schemas/` are the contract a worker's output must meet before the coordinator commits it, and keeping them apart keeps that checking boundary clear. Each agent has one file, where its policy, its tools and its fabricated variant live together, while its input and output schemas live in `schemas/tasks.py`, so adding an agent means adding one file there and one set of schemas.
