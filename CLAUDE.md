# CLAUDE.md

A multi-agent system that stays coherent over a long horizon.
Architecture and judgment over polish. A clear partial system beats a
rushed full one.

Read first: `README.md` (overview, how to run) and `docs/design.md` (all
design decisions: goal, roles, tables, context packets, faults).

## Working preferences

- Keep responses short and concise.
- Push directly to `main`. No feature branches or PRs unless asked.
- Keep scope small. Go deep on
  (1) state and context management and (2) failure detection and recovery.
  Keep everything else simple.
- **No real LLM calls, ever.** No API keys, no spend. Every agent uses a
  deterministic fake model (Pydantic AI `FunctionModel`).
- Long-form docs go in `docs/`. `README.md` and `NOTES.md` stay at the
  root.
- Keep the repo generic: never mention any company, interview, or
  take-home in code, docs, or commit messages. Don't mention any time
  limit for building it either.

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
Reviewers use `just db-up` (docker compose).

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
  (append-only, everything), `tasks` (DAG via `depends_on`, leased with
  `FOR UPDATE SKIP LOCKED`), `facts` (provenance + status).
- **Never feed the raw log to a model.** Each call gets a fresh
  `ContextPacket` (pinned → relevant facts → recent → pointers), within a
  token budget. Summaries are recomputed from facts, never from summaries.
- **Failed work never flows downstream.** Retry with the same role, then
  replan. Worker output is schema-validated before commit.
- **Faults are seeded:** `hash(seed, task_id, attempt)`, so the same seed
  gives the same run.
- Session ends when the goal is met (leftover tasks → `cancelled`) or the
  budget runs out.
