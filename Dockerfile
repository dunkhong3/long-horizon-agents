# The system in one image, for `docker compose up`, which starts Postgres
# and then one full run. The image has uv and the locked dependencies, and
# the code is copied last so a code change doesn't reinstall them.
FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /bin/uv
WORKDIR /app
# git, for the code domain's workspaces
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 PYTHONUNBUFFERED=1
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --extra models --no-install-project
COPY lha ./lha
RUN uv sync --locked --no-dev --extra models
ENTRYPOINT ["uv", "run", "--no-sync", "python", "-m", "lha.run"]
CMD ["--seed", "42"]
