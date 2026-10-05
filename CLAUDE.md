# CLAUDE.md

## What this is

This is a personal side project, a system of several agents that stays coherent over a long run, where architecture and judgement matter more than polish, and a small system that works end to end beats a big one that doesn't. Read `README.md` first for the overview and how to run it, and then `docs/design.md`, which holds every design decision (the goal, the roles, the tables, the context packets and the faults).

## Working preferences

Keep responses short and concise, and push directly to `main`, with no feature branches or pull requests unless asked. Build the v1 core end to end first (see 'Scope' in `docs/design.md`), and keep the scope small, going deep on state and context management and on failure detection and recovery, and keeping everything else simple. There are never any real LLM calls, meaning no API keys and no spend, because every agent uses a deterministic fake model through Pydantic AI's `FunctionModel`. Long-form docs go in `docs/`, while `README.md` and `NOTES.md` stay at the root. Write everything (code, docs and commit messages) as a personal side project, and don't mention companies or who it was built for.

## Writing style

Every doc, note and code comment follows the owner's writing style. It is narrative and explanatory, as if explaining to a colleague new to the subject, with long flowing sentences joined by 'which', 'but', 'so' and 'because', plain everyday words, and 'we' and 'our' for the team's view. Every technical term is explained on first use in each document, in the form 'X, which is Y'. There are no em dashes and no spaced en dashes, a short dash is only used inside number ranges, there are at most two or three colons per document outside code, there are no question marks outside quotes, no rhetorical questions, no punchy fragments, no idioms and no emojis, and prose is preferred over bullet points and tables. Double quotes are only for someone's exact words, and our own labels go in single quotes. Every claim about the code points to its file and line range, and anything not in the code is stated plainly as 'the code does not ...', backed by an actual search.

## Stack

The stack is Python 3.11, uv, just, Pydantic and Pydantic AI (for the agents only, and not pydantic-graph, because the coordination is our own code, explicit and testable), FastAPI (for the mock network service), SQLAlchemy async with asyncpg, Alembic (for migrations), Postgres 16, asyncio, pytest and ruff.

## Commands

```bash
just install   # uv sync + install git hooks
just check     # ruff format --check + ruff check
just fmt       # auto-fix
just test      # pytest (needs Postgres)
just start     # run the system (--domain audit or research)
just up        # Postgres and one full run, both in docker compose
just dashboard # a live view of every run, on port 8000
just migrate   # upgrade the schema; `just migrate revision "msg"` writes a new one
just demo      # full run + crash at step 120 + resume
just bench     # the system against a naive full-history baseline
just scale     # one big world with more workers and coordinators
```

Run `just fmt && just check && just test` before every commit. In the cloud container there is no Docker daemon, so use the system Postgres, which may need starting again after the container restarts.

```bash
service postgresql start
su postgres -c "psql -c \"CREATE USER lha WITH PASSWORD 'lha' SUPERUSER;\" -c \"CREATE DATABASE lha OWNER lha;\""
```

The default `DATABASE_URL` is `postgresql+asyncpg://lha:lha@localhost:5433/lha`, matching `just db-up`, which starts Postgres with docker compose on port 5433. The system Postgres in the cloud container listens on 5432, so the container has a git-ignored `.env` with `DATABASE_URL=postgresql+asyncpg://lha:lha@localhost:5432/lha`, which `just` loads automatically.

## Commit messages

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/), in the form `<type>(<optional scope>): <summary>`, with the types feat, fix, docs, chore, refactor, test, perf, build, ci, style and revert. The subject is in the imperative mood, in lower case and without a trailing full stop, followed by a blank line and a body explaining why. Every line is at most 72 characters, which is vim's `textwidth` for git commits, so `gqip` or `ggVGgq` wraps to the same width, and URLs and trailers are the only exceptions. This is enforced by `.githooks/commit-msg`, which `just install` sets up. Commits are authored by the owner (`dunkhong3 <wddpzh@gmail.com>`) with a `Co-Authored-By` trailer for Claude, and they never include a link to a chat session.

## Architecture rules

These rules should not drift. First, the coordinator is plain code and not an LLM, so LLMs propose and code decides what gets accepted. Second, Postgres is the only shared state, with the tables `sessions`, `events` (append-only, everything), `tasks` (follow-ups created only from accepted results, and leased with `FOR UPDATE SKIP LOCKED`) and `facts` (with where each fact came from and its status). Third, the raw log is never fed to a model, and each attempt at a task gets a fresh `ContextPacket` (pinned, then relevant facts, then recent events, then pointers) within a token budget, with summaries worked out again from the facts and never from other summaries. Fourth, failed work never flows downstream, so it is retried with the same role and then replanned. Fifth, workers never write facts, because they submit a result fenced by `attempt`, and the coordinator checks it and commits the facts and the follow-up tasks in one transaction. Sixth, faults are seeded with `hash(seed, task_key, attempt, call_no)`, so the same seed gives the same faults and the same outcome, though not the same order of events. Lastly, a session ends when the goal is met (with leftover tasks set to `cancelled`), or when the step budget or time limit runs out, or when the run stalls (no work left, or no accepted result for a while) and there is nothing left to re-open, in which case a partial report is written.
