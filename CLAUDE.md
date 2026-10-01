# CLAUDE.md

A personal side project: a multi-agent system that stays coherent over a
long horizon. Architecture and judgment over polish. A small system that
works end to end beats a big one that doesn't.

Read first: `README.md` (overview, how to run) and `docs/design.md` (all
design decisions: goal, roles, tables, context packets, faults).

## Working preferences

- Keep responses short and concise.
- Push directly to `main`. No feature branches or PRs unless asked.
- Build the v1 core end to end first (see "Scope" in `docs/design.md`).
- Keep scope small. Go deep on
  (1) state and context management and (2) failure detection and recovery.
  Keep everything else simple.
- **No real LLM calls, ever.** No API keys, no spend. Every agent uses a
  deterministic fake model (Pydantic AI `FunctionModel`).
- Long-form docs go in `docs/`. `README.md` and `NOTES.md` stay at the
  root.
- Write everything (code, docs, commit messages) as a personal side
  project. Don't mention companies or who it was built for.

## Stack

Python 3.11, uv, just, Pydantic + Pydantic AI (agents only, **not**
pydantic-graph: coordination is our own code, explicit and testable), FastAPI
(the mock network service), SQLAlchemy async + asyncpg, Postgres 16, asyncio,
pytest, ruff.

## Commands

```bash
just install   # uv sync + install git hooks
just check     # ruff format --check + ruff check
just fmt       # auto-fix
just test      # pytest (needs Postgres)
just start     # run the system
```

Run `just fmt && just check && just test` before every commit.

### Postgres in the cloud container

There is no Docker daemon here. Use the system Postgres:

```bash
service postgresql start
su postgres -c "psql -c \"CREATE USER lha WITH PASSWORD 'lha' SUPERUSER;\" -c \"CREATE DATABASE lha OWNER lha;\""
```

Default `DATABASE_URL`: `postgresql+asyncpg://lha:lha@localhost:5432/lha`.
Elsewhere, use `just db-up` (docker compose).

## Commit messages

- [Conventional Commits](https://www.conventionalcommits.org/):
  `<type>(<optional scope>): <summary>`. Types: feat, fix, docs, chore,
  refactor, test, perf, build, ci, style, revert.
- Subject: imperative mood, lower case, no trailing period.
- Blank line, then a body explaining *why*.
- **Every line ≤ 72 chars** (vim's gitcommit `textwidth`, so `gqip` /
  `ggVGgq` wraps to the same width). URLs and trailers are exempt.
- Enforced by `.githooks/commit-msg` (installed by `just install`).

## Architecture rules (don't drift from these)

- The **coordinator is plain code, not an LLM**. LLMs propose; code decides
  what gets accepted.
- **Postgres is the only shared state.** Tables: `sessions`, `events`
  (append-only, everything), `tasks` (created only from accepted results,
  leased with `FOR UPDATE SKIP LOCKED`), `facts` (provenance + status).
- **Never feed the raw log to a model.** Each call gets a fresh
  `ContextPacket` (pinned → relevant facts → recent → pointers), within a
  token budget. Summaries are recomputed from facts, never from summaries.
- **Failed work never flows downstream.** Retry with the same role, then
  replan.
- **Workers never write facts.** They submit a result (fenced by
  `attempt`); the coordinator validates it and commits facts + follow-up
  tasks in one transaction.
- **Faults are seeded:** `hash(seed, task_key, attempt, call_no)`, so the
  same seed gives the same faults and outcome (not the same event order).
- Session ends when the goal is met (leftover tasks → `cancelled`) or the
  budget runs out.
