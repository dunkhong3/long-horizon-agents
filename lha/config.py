"""Tunable constants, in one place so the numbers in docs/design.md are easy to find."""

# Leases and heartbeats (see 'Task queue and leases' in docs/design.md).
LEASE_SECONDS = 10
HEARTBEAT_SECONDS = 3

# Retries, where a retry is the same task row with attempt + 1.
MAX_ATTEMPTS = 3
BACKOFF_BASE_SECONDS = 0.25  # retry n waits BACKOFF_BASE_SECONDS * 2**n
# New rounds are new task rows (the task_key gets a '#n' suffix).
MAX_ROUNDS = 2  # discover_host and compare_service
MAX_VERIFY_ROUNDS = 3  # verify_drift
NEW_ROUND_DELAY_SECONDS = 1.0  # 'try again later, after other work'

# The circuit breaker per resource (a host in the audit, a source in the research brief).
BREAKER_THRESHOLD = 3  # failed network attempts in a row against one resource
BREAKER_COOLDOWN_SECONDS = 3.0

# Stalls, where no result was accepted for this many steps.
STALL_STEPS = 200

# Splitting a discovery that doesn't fit in one attempt's context.
SPLIT_BATCH = 8  # services per batch

# Tools talking to the mock network.
TOOL_TIMEOUT_SECONDS = 1.0  # the client gives up after this
TOOL_RETRIES = 2  # extra tries for a transient error, inside one attempt
TOOL_RETRY_DELAY_SECONDS = 0.05
TIMEOUT_FAULT_SECONDS = 3.0  # how long the server stalls on a timeout fault

# Fault injection.
DEFAULT_FAULT_RATE = 0.15  # share of HTTP calls that fail
MODEL_ERROR_RATE = 0.05  # share of final model outputs that are corrupted

# Context packets. Every model call has a window, which is shared between
# the model's answer, the attempt's own tool calls and results, and the
# packet, so the packet gets what is left, 800 of the fake model's 2000.
CONTEXT_WINDOW_TOKENS = 2000  # the fake model's window
REAL_CONTEXT_WINDOW_TOKENS = 32000  # a real model's window, unless --window says otherwise
OUTPUT_RESERVE_SHARE = 0.2  # room for the model's answer
WORK_RESERVE_SHARE = 0.4  # room for the attempt's own tool calls and results
RECENT_EVENTS = 5
MAX_POINTERS = 40

# Real models, which are opt-in (see --model).
DEFAULT_REAL_MODEL = "anthropic:claude-sonnet-5-5"  # what 'auto' picks when ANTHROPIC_API_KEY is set
DEFAULT_BUDGET_USD = 5.0  # what a run on a real model may spend, unless --budget says otherwise
# US dollars per million input and output tokens for a model Pydantic AI's
# price list doesn't know, set high on purpose so the budget still holds.
FALLBACK_PRICE_PER_MILLION = (15.0, 75.0)

# Runs.
DEFAULT_HOSTS = 20
DEFAULT_STEP_BUDGET = 3000
POLL_SECONDS = 0.05  # how often to look for work when polling (--poll)
IDLE_MAX_SECONDS = 1.0  # the longest a worker waits for a notification before looking anyway
COORDINATOR_IDLE_SECONDS = 0.5  # the same for a coordinator, which also sweeps leases
GOAL_CHECK_SECONDS = 0.5  # how often the leader checks the goal, the budget and stalls
ALL_DRIFTS = 3  # drifts planted for the 'find all drifts' goal

# Crashes, where the supervisor gives up on a coordinator that crashes this
# many times in a row without committing anything in between.
CRASH_LOOP_LIMIT = 3
LOCK_WAIT_SECONDS = 30.0  # how long a coordinator waits for an old one to let go
