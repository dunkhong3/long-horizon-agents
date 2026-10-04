"""Tunable constants, in one place so the numbers in docs/design.md are easy to find."""

# Leases and heartbeats (see 'Heartbeat' in docs/design.md).
LEASE_SECONDS = 30
HEARTBEAT_SECONDS = 10

# Retries, where a retry is the same task row with attempt + 1.
MAX_ATTEMPTS = 3
BACKOFF_BASE_SECONDS = 0.25  # retry n waits BACKOFF_BASE_SECONDS * 2**n
# New rounds are new task rows (the task_key gets a '#n' suffix).
MAX_ROUNDS = 2  # discover_host and compare_service
MAX_VERIFY_ROUNDS = 3  # verify_drift
NEW_ROUND_DELAY_SECONDS = 1.0  # 'try again later, after other work'

# Tools talking to the mock network.
TOOL_TIMEOUT_SECONDS = 1.0  # the client gives up after this
TOOL_RETRIES = 2  # extra tries for a transient error, inside one attempt
TOOL_RETRY_DELAY_SECONDS = 0.05
TIMEOUT_FAULT_SECONDS = 3.0  # how long the server stalls on a timeout fault

# Fault injection.
DEFAULT_FAULT_RATE = 0.15  # share of HTTP calls that fail
MODEL_ERROR_RATE = 0.05  # share of final model outputs that are corrupted

# Context packets.
CONTEXT_WINDOW_TOKENS = 2000
OUTPUT_RESERVE_TOKENS = 400
RECENT_EVENTS = 5

# Runs.
DEFAULT_HOSTS = 20
DEFAULT_STEP_BUDGET = 3000
POLL_SECONDS = 0.05
