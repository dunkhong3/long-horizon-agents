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
    docker compose up -d --wait

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

# a full run, then a crash at step 120 and a resume
demo:
    uv run python -m lha.run --seed 42
    -uv run python -m lha.run --seed 7 --kill-at 120
    uv run python -m lha.run --resume

# the system against the naive full-history baseline
bench *args:
    uv run python -m lha.bench {{args}}
