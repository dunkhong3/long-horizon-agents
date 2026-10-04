# Notes

## What this is

This repo is version 1 (v1) of the system, which is the core described in the 'Scope and versions' section of [docs/design.md](docs/design.md), and everything marked '(later)' in that document is the next version and is not built yet. The system is a set of agents that audit a mock cloud deployment over a few hundred steps, where the hard part is not any single agent but keeping the whole run coherent while things keep going wrong on purpose. Two words come up a lot below. A decoy is a healthy service whose first read returns an old, out-of-date replica count (`lha/world/app.py`, lines 65–68), so it looks broken on first sight but turns out fine when it is read again. An RNG is a random number generator, the usual tool for random choices, and this repo avoids a shared one on purpose (see below).

## What we went deep on

**First, state and context management.** Long agent runs usually break because the context turns into a pile of history, where old errors, stale reads and earlier guesses all compete for attention. In this repo the model never sees history, because each attempt at a task gets a `ContextPacket` built fresh from Postgres (`lha/context.py`, lines 81–94) with the goal, this task, the facts in its scope and the reason its last attempt failed, packed from the top down within a token budget (`lha/context.py`, lines 42–78), and within the attempt the model sees only that packet and the results of its own tool calls (`lha/agents/base.py`, lines 119–138). Raw tool output is turned into small, typed facts when it is written, so the prompt grows with the task and not with the run.

**Second, failure detection and recovery.** Faults are injected everywhere on purpose and reproducibly, meaning they are seeded so the same seed gives the same faults. They include HTTP 500 and 429 responses, timeouts, empty 'successful' responses, stale reads, malformed and fabricated model output, and, with `--kill-at`, a hard crash of every process (`lha/world/app.py`, lines 26–41; `lha/agents/base.py`, lines 82–111; `lha/run.py`, lines 234–238). Each kind of fault has a specific mechanism that catches it, and every run is scored against the planted answer (`lha/scoring.py`, lines 30–52).

## Decisions we are most confident about

**The coordinator is plain code and not an LLM.** LLMs propose and code decides, and the coordinator is the only writer of facts and of the plan (`lha/coordinator.py`), so there is one source of truth and no agent can talk the system into a wrong state.

**We never trust a claim that code can check.** Every fact a worker reports cites the tool-call event it came from, and the coordinator checks the value against that event's raw response before it accepts anything (`lha/coordinator.py`, lines 167–185 and 529–534), so fabricated output is rejected and not believed.

**Two independent reads must agree before a drift counts.** The newest read is not automatically right, and the decoys show why, because their first read is stale and only a majority of reads settles it (`lha/coordinator.py`, lines 259–293).

**Postgres is the only shared state, and three small tools inside it do most of the work.** These are `FOR UPDATE SKIP LOCKED` for the task queue (`lha/db/crud.py`, lines 112–125), an `attempt` number used as a fencing token for leases, which lets a slow worker find out it has been replaced (`lha/db/crud.py`, lines 133–168), and a partial unique index for 'one current fact per key' (`lha/db/models.py`, lines 99–108).

**Each accepted result is one transaction, covering the facts, the follow-up tasks and the decision log.** A crash cannot leave half a decision behind (`lha/coordinator.py`, lines 123–165), which is why `--resume` is simple, because nothing important lives in memory.

**Follow-up tasks are only created from accepted results.** Apart from the starting tasks and the report task, every task comes from an accepted result, so failed work cannot flow downstream, because nothing downstream is ever created from it (`lha/coordinator.py`, lines 211–228 and 365–395).

**Faults are seeded on stable names and not on ids or a shared RNG.** The roll is a hash of the seed, the `task_key`, the attempt and the call number (`lha/faults.py`, lines 19–27), so a seed gives the same faults across processes and across runs, whereas a shared RNG would give different results depending on which process happened to ask first.

## What we cut and what comes next

We cut several things on purpose, all of which are designed in [docs/design.md](docs/design.md). The code does not have a circuit breaker per host or stall detection that re-opens put-off work, and v1 instead retries, runs a second round, and ends cleanly with a partial report when no work is left (`lha/coordinator.py`, lines 417–458). The code does not restart the coordinator automatically and does not take a Postgres advisory lock, although the coordinator can already be restarted by hand with `--resume`. The code does not kill workers at random in the middle of a task, although the supervisor already restarts a worker that dies (`lha/run.py`, lines 75–81), and tasks lost in the middle of a lease are recovered after `--kill-at` because `--resume` releases their leases (`lha/run.py`, lines 159–172). No test kills a single worker on its own, so the restart path itself is not exercised. The code does not split a task whose pinned context does not fit, and instead fails it with `context_overflow` (`lha/agents/worker.py`, lines 110–141). Lastly, the mock world is simplified, because a read that matches the registry is trusted after one read, stale reads only ever create false drifts and never hide real ones, and there is exactly one planted drift.

For the next version we would work in this order. First, make 'find all drifts' the goal, by planting several and defining done as 'no unexplored hosts and a verdict for every service', which gives a longer and more predictable horizon. Second, add the circuit breaker and the stall detection above. Third, swap polling for `LISTEN/NOTIFY` and then split the plan across several coordinators for scale, as described under 'Limits and scaling' in the design doc. Lastly, put a real model behind the same `Agent` interface and keep the fake one for reproducible tests.

## How we used coding tools

We built this with Claude Code, and the way of working had three parts. First, the design came before any code, in `docs/design.md`, and we went through several review passes, each of which found real gaps, such as faults keyed on ids not being reproducible, made-up facts not actually being caught, a 'list all hosts' endpoint making the hidden hosts pointless, and `depends_on` turning out to be unnecessary. Second, the code was written against that document, with `CLAUDE.md` holding the working rules and the architecture rules so that every session followed the same design. Lastly, we ran the system a lot, with full runs across many seeds and fault rates plus crash-and-resume runs, and one early run found the drift after only 8 of 20 hosts because one worker raced down the chain of documents while another was stuck on timeouts, which we fixed by hanging the chain below the deepest hosts (`lha/world/model.py`, lines 77–95).
