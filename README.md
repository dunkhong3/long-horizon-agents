# long-horizon-agents

[![ci](https://github.com/dunkhong3/long-horizon-agents/actions/workflows/ci.yml/badge.svg)](https://github.com/dunkhong3/long-horizon-agents/actions/workflows/ci.yml)

## What this is

This project is a small system where several agents, each running as its own process, work towards one goal over hundreds of dependent steps without losing track of what they are doing, and the only thing they share is a Postgres database. Each attempt at a task gets a small, fresh context instead of the whole history, faults and crashes are injected on purpose, and the system has to find its way back from all of them. The full design is in [docs/design.md](docs/design.md), and the decisions, what is still not built and how it was made are in [NOTES.md](NOTES.md).

## The task

The agents audit a mock cloud deployment, which is a set of fake servers (hosts) where each host runs some services, and each service runs a number of replicas, meaning copies of itself running at the same time. A file called the registry says how many replicas each service should have, a service that runs a different number has drifted, and the agents have to find the drift and write a report backed by verified evidence. The small example below shows the idea.

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
```

The agents start knowing only `host-1`, `host-2` and `host-3`, and the example is simplified. A real run has 20 hosts, between about 65 and 90 services, 3 decoys (healthy services whose first read is stale, so they look broken until they are read again) and a dead reference, which is a document that names a host that doesn't exist. One host runs 40 services, too many to read in one attempt's context, so its work has to be split, and another is down for the first round of work, so its circuit breaker opens and a later round has to bring it back. The default goal plants one drift 5–6 document hops away from the start (`lha/domains/audit/world.py`, lines 101–119), and `--goal all` plants three and only ends once every host has been explored and every service has a verdict. A run takes roughly 250–500 steps at the default fault rate, about 470–530 when finding all drifts, and up to about 660 at a 45% fault rate, and timeouts, 500 errors, empty responses and made-up model output all happen along the way, plus crashes of workers and of the coordinator with `--crashes`, and a crash of every process with `--kill-at`. Every run is scored against the planted answer as PASS or FAIL (`lha/core/scoring.py`, lines 31–49).

The audit is one domain, and the core that keeps the run coherent knows nothing about it. A second domain, verified research, runs on the same core and answers a question by planning what to find, searching, reading pages and checking every quote against the page it came from, where a value only counts once two or more independent sites agree on it and outnumber every other value, and a word-for-word copy on another site doesn't count twice. On the fake model it researches a mock web whose answers are known, so every run is scored, and on a real model `just research "your question"` researches the real web with the provider's own search and writes a report in which every answer lists the pages and sentences behind it ('Verified research' in [docs/design.md](docs/design.md); `lha/domains/research/`). A third domain, code work, makes a check such as a test suite pass on a git repository, one failing item at a time, on a branch of its own, where every commit is a fix whose item passed its check with exactly that fix before it was committed, and the tests are left alone. On the fake model it fixes a mock repository with planted bugs, one of them in a helper that other modules use, and on a real model `lha code <repo> --check "uv run pytest -q {item}"` works on a real repository and leaves a branch to review ('Code work on a repository' in [docs/design.md](docs/design.md); `lha/domains/code/`). A fourth domain, batch extraction, fills in a table of fields from every document in a folder, where every value is quoted from its document and checked against the page it came from, and a field a document doesn't state is left empty. On the fake model it reads mock invoices with known values, some of them long or missing a date, and on a real model `lha extract <folder> --fields "invoice number; total"` writes the table as CSV and JSON ('Batch extraction' in [docs/design.md](docs/design.md); `lha/domains/extract/`). A domain like that can also be one TOML file instead of Python, with stages that each read every document and ask for fields of their own, where a later stage waits on the earlier ones and sees what they found, and with documents of its own whose values are known, so it is scored on the fake model like the others, while `--docs` runs it on a real folder with a real model ('Domains from configuration' in [docs/design.md](docs/design.md); `lha/domains/config/`).

## How it is built

```mermaid
flowchart TB
    S["Supervisor<br/>starts every process, restarts the ones that die"]
    C["Coordinator processes, one per partition of the plan<br/>plain code that checks, retries, splits and replans"]
    DB[("Postgres<br/>sessions · events · tasks · facts")]
    W["Agent worker processes on the fake model or a real one<br/>discovery · analysis · reporter"]
    M["Mock network<br/>FastAPI with seeded faults"]
    S --> C
    S --> W
    C <-->|"decisions, facts, follow-up tasks"| DB
    W <-->|"claim a task, submit a result"| DB
    W -->|"tool calls"| M
```

Every process wakes up on Postgres `LISTEN/NOTIFY` instead of polling, and the only thing they share is the database.

The two things this project goes deep on are state and context management, and failure detection and recovery. For context, every attempt at a task gets a fresh `ContextPacket`, which is a small bundle with the goal, the task and only the facts that matter for it, built from the database within a token budget (`lha/core/context.py`, lines 61–101), and within that attempt the model sees only the packet and the results of its own tool calls, so the raw log never goes into a prompt, and a task whose own work doesn't fit is split into batches. For recovery, tool failures are never silent (`lha/core/tools.py`, lines 130–159), the tasks of crashed workers are handed out again, a host that keeps failing has its work held back by a circuit breaker, a coordinator that crashes is started again, work that was put off is re-opened when the run stalls, claims are verified before they count, and failed work is retried or rerouted and never passed downstream. Workers never write facts themselves, because the coordinator checks every claimed fact against the raw tool response it cites (`lha/domains/audit/rules.py`, lines 44–74), so a made-up number or host is rejected. The whole run can be killed at any point and `--resume` carries on from the database. No API key is needed, because without one the agents run on a deterministic fake model through Pydantic AI's `FunctionModel`, and setting `ANTHROPIC_API_KEY` switches them to a real model with a money budget, while tests and CI always stay on the fake one ('Models' in [docs/design.md](docs/design.md); `lha/core/agent.py`, lines 45–55).

We also measured the design against a naive agent whose prompt is its whole history, on the same worlds with the same faults (`lha/baseline.py`, `lha/bench.py`). At 60 hosts the system's prompts average about 440 tokens and never go over its 1600-token limit, while the naive agent's average about 7,300 and grow with every step, so a run sends about 8 times more tokens, and the naive agent names a decoy in every run unless it re-reads a mismatch, and even then, once its history has to fit a 2000-token window, it loses track of its work and runs out of steps in 8 runs out of 10. The numbers and their limits are in 'The benchmark' in [docs/design.md](docs/design.md).

At scale, a world of 200 hosts finishes the 'find all drifts' goal in about 28 s with 8+8 workers against about 43 s with 2+2, and splitting the plan between 4 coordinators cuts the time a result waits for its decision from about 730 ms to about 70 ms at the median (`lha/scale.py`, and 'Limits and scaling' in the design doc).

## How to run it

It needs Python 3.11 or newer, [uv](https://docs.astral.sh/uv/), [just](https://github.com/casey/just), and Postgres 16. `just db-up` starts Postgres in Docker on port 5433, not the usual 5432, so it doesn't clash with another Postgres already running on the machine, and the default `DATABASE_URL` is `postgresql+asyncpg://lha:lha@localhost:5433/lha` to match (`docker-compose.yml`; `lha/db/__init__.py`, line 5). To use a Postgres you already have instead, create a user and a database in it and point `DATABASE_URL` at them, for example in a `.env` file, which `just` loads automatically. Every run brings the database to the newest schema with Alembic migrations first, so a database from an older version is upgraded in place (`lha/db/migrate.py`). With only Docker installed, `docker compose up --build` builds the image, starts Postgres and runs one full run, and `just up` does the same. Many workers need many connections, so for `just scale` an existing Postgres needs `max_connections` of about 300, which the docker compose one already has.

```bash
psql -c "CREATE USER lha WITH PASSWORD 'lha';" -c "CREATE DATABASE lha OWNER lha;"
echo 'DATABASE_URL=postgresql+asyncpg://lha:lha@localhost:5432/lha' > .env
```

```bash
just up                               # Postgres and one full run, both in Docker
just up-join                          # the same run on two containers, where the second joins it
just install && just db-up
just test                             # unit tests + full runs + crashes + resume
just start --seed 42                  # full run, prints PASS/FAIL (~10 s)
just start --seed 42 --chaos 0.3      # more faults
just start --seed 42 --goal all       # find every drift, not just one
just start --seed 42 --crashes 0.05   # crash workers and the coordinator on purpose
just start --seed 7 --kill-at 120     # crash every process after step 120...
just start --resume                   # ...and resume from Postgres
just demo                             # a full run, then a crash at step 120 and a resume
just bench                            # the system against a naive full-history agent (~7 min)
just scale                            # one big world with more workers and coordinators (~3 min)
just dashboard                        # a live view of every run, on http://localhost:8000
just start --domain research --goal all  # the second domain, verified research on a mock web
just research "your question"         # verified research on the real web, which needs a real model
just start --domain code --goal all   # the third domain, code work on a mock repository
just start --domain extract --goal all  # the fourth domain, batch extraction from mock invoices
just start --domain tickets           # a domain from configuration (lha/domains/config/examples/tickets.toml)
just start --seed 7 --hosts 200 --goal all --step-budget 50000 --coordinators 4 --workers 8,8
```

Without `just`, the same run is `uv sync && uv run python -m lha.run --seed 42`, but then `.env` is not loaded, so `DATABASE_URL` has to be exported in the shell if it isn't the default. A step is one model call or one tool call, and `--seed` fixes the network, the faults and the injected crashes, so the same seed gives the same faults and the same result. Each run writes `runs/<session>/finding.md` and `score.json`. To watch a run as it happens, start `just dashboard` in a second terminal (or `docker compose up dashboard`) and open `http://localhost:8000`, which shows the progress line, the steps over time, the tasks of each type by status, the facts by status, any open circuit breakers, the coordinator's latest decisions with their reasons, and the report once it is written (`lha/dashboard.py`).

Everything above runs on the fake model and costs nothing. To use a real model, install the provider packages with `uv sync --extra models` and set `ANTHROPIC_API_KEY`, and every new run then uses Claude Sonnet under a money budget of 5 dollars, which `--budget` changes, while `--model fake` keeps the fake one and `--model` takes any other Pydantic AI model name. Research on the real web searches with the model provider's own web search and reads pages with httpx, and on a machine that can reach the model's API but not the open web, `--fetch provider` (or `LHA_FETCH=provider`) has the model provider's own web fetch read the pages instead, for a small cost per page, although the provider refuses that to a sandbox with restricted network access, such as a cloud container, where research on the real web can't run at all. With docker compose, setting the key in the shell before `docker compose up` is enough, because the image already has the provider packages, and `docker compose run --rm lha --question "your question"` researches a question on the real web with nothing but Docker installed. Tests, CI and the benchmarks always use the fake model.

### Handing it a long task

`uv run lha` is a small command over the same runs, which starts them, follows them and prints what they found, and it reads `.env` the way `just` does ('The lha command and the MCP server' in [docs/design.md](docs/design.md); `lha/cli.py`). With a provider key set, `lha research "your question" --detach` starts research on the real web in the background and prints its session id, so the terminal can be closed while it runs, `lha status` shows how the latest run is doing and what it has spent, and `lha report` prints its report once it is done. A run whose supervisor stopped, for example because the machine restarted, keeps all its state in Postgres, and `lha resume` goes on from where it was.

```bash
uv run lha research "your question" --detach   # research in the background (a real model)
uv run lha code . --check "uv run pytest -q {item}" --detach   # make the tests pass on a branch
uv run lha extract ~/invoices --fields "invoice number; total" --detach   # a table of fields from a folder
uv run lha status                               # how the latest run is doing, and what it has spent
uv run lha report                               # what it found, with the pages and sentences behind it
uv run lha list                                 # the latest runs
uv run lha resume                               # continue the latest unfinished run
```

### A domain of your own

A domain that reads documents in stages and fills in a table can be written as one TOML file, and `lha/domains/config/examples/tickets.toml` is one to start from, which sorts support tickets and then routes each one to a team. Each `[[stage]]` names its fields, each field can have `one_of` options or a `pattern`, a stage can wait on earlier ones with `needs` or `after`, and `[[document]]` entries with the values they `expect` make a mock world for the fake model. `uv run lha domains` lists the domains it finds, which are the built-in ones, every file in the folders of `LHA_DOMAIN_PATH` and the examples, and `--domain-file` runs a file from anywhere ('Domains from configuration' in [docs/design.md](docs/design.md)).

```bash
uv run lha domains                                     # what it can run
just start --domain-file my.toml                       # a domain of your own, on its own documents
uv run lha start --domain-file my.toml --docs ~/inbox --detach   # on a real folder, with a real model
```

### More than one machine

Workers share nothing but Postgres, so other machines that reach the same Postgres can lend their workers to a run ('Workers on more than one machine' in [docs/design.md](docs/design.md); `lha/run.py`, lines 628–688). The machine that starts the run stays its home, with the coordinators, the mock world and any files the run works on, and it has to start the run with `--listen 0.0.0.0` so the mock world can be reached from the others, while every other machine runs `lha join`, which starts workers there until the run ends. A machine is named by `LHA_MACHINE`, or else by its hostname, and the names have to differ, and when the hostname isn't how the other machines reach the home, `LHA_ADVERTISE_HOST` says how they do. Code work and extraction need the files of the home machine for every task but the report, so only the audit and research runs take other machines, and `lha status` names the machines of a run.

```bash
just start --seed 7 --hosts 200 --goal all --listen 0.0.0.0   # on the home machine
uv run lha join --workers 8,8                                   # on each other machine, for the latest run
```

An assistant such as Claude Code can hand long tasks to the system by itself, through an MCP server, which is a process that offers tools to an assistant over the Model Context Protocol (`lha/mcp_server.py`). Once it is added with the command below, run in this folder, every Claude Code session can start research, code work or an extraction, check on it and read the report, while the key and the database come from this folder's `.env`.

```bash
uv sync --extra mcp --extra models
claude mcp add --scope user lha -- uv run --directory "$PWD" --extra mcp --extra models lha mcp
```

## Sample output

```
[supervisor] session <id>: audit, seed 42, size 20, fault rate 0.15, goal 'one', crash rate 0.0, 1 coordinator(s), workers 2+2, LISTEN/NOTIFY, model fake
[supervisor] step  245 | tasks: 42 done, 3 active, 1 failed | hosts found: 14 | drift claims: 0 inferred, 0 verified, 1 refuted
[supervisor] step  390 | tasks: 73 done, 20 active, 2 failed | hosts found: 15 | drift claims: 2 inferred, 0 verified, 1 refuted
[coordinator] goal met: a verified drift backed by the registry; cancelled 1 leftover task(s); writing report

# Finding: replica drift

- **oncall** on **host-12** runs **2** replica(s); the registry expects **5** (verified drift fact `<id>`).

  ✓ session succeeded
  ✓ found the drifted service (oncall)
  ✓ on the right host (host-12)
  ✓ right counts (expected 5, actual 2)
  ✓ every cited fact is verified

steps=469 model_calls=278 tool_calls=191 faults_injected=34 model_errors_injected=9 outputs_rejected=3 retries=11 replans=2 splits=1 breaker_opened=1 reopened=0 crashes_injected=0 pointer_fetches=35 leases_lost=0 repeated_reads=0 hosts_checked=19/20 decoys_refuted=3/3 drifts_planted=1 elapsed=11.1s
verdict=PASS  (session <id>, files in runs/<id>)
```
