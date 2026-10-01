set dotenv-load

# list targets
default:
    @just --list

# install dependencies and git hooks
install:
    uv sync
    git config core.hooksPath .githooks

# start Postgres (no Docker: `service postgresql start`)
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
