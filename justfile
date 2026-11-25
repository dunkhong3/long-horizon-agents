set dotenv-load

# list targets
default:
    @just --list

# install dependencies and git hooks
install:
    uv sync
    git config core.hooksPath .githooks

# start Postgres with Docker, on port 5433
db-up:
    docker compose up -d --wait db

# Postgres and one full run, both in Docker
up:
    docker compose up --build --exit-code-from lha

# a run on two containers, where the second joins the first one's run
up-join:
    docker compose --profile join build home helper
    docker compose --profile join up --detach helper
    docker compose --profile join run --rm --use-aliases home
    docker compose --profile join logs helper

# bring the database to the newest schema (every run also does this)
migrate *args:
    uv run python -m lha.db.migrate {{args}}

# stop Postgres and delete its data
db-down:
    docker compose down -v

# formatting + lint check (no changes)
check:
    uv run ruff format --check .
    uv run ruff check .

# auto-fix formatting and lint
fmt:
    uv run ruff format .
    uv run ruff check --fix .

# run tests (needs Postgres)
test *args:
    uv run pytest -q {{args}}

# run the system
start *args:
    uv run python -m lha.run {{args}}

# research a question on the real web, which needs a real model (ANTHROPIC_API_KEY)
research question *args:
    uv run python -m lha.run --question {{quote(question)}} {{args}}

# a live view of the runs in Postgres, on http://localhost:8000
dashboard *args:
    uv run python -m lha.dashboard {{args}}

# a full run, then a crash at step 120 and a resume
demo:
    uv run python -m lha.run --seed 42
    -uv run python -m lha.run --seed 7 --kill-at 120
    uv run python -m lha.run --resume

# the system against the naive full-history baseline
bench *args:
    uv run python -m lha.bench {{args}}

# one big world with more and more workers and coordinators
scale *args:
    uv run python -m lha.scale {{args}}
