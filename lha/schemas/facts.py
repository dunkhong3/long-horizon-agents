"""How facts are named.

A fact is one small, typed claim: (subject, key) -> value. The same
(subject, key) is "the same fact"; only one row for it is current at a time.
"""

# Fact keys.
EXISTS = "exists"  # host:<h> -> true / false
UNREACHABLE = "unreachable"  # host:<h> -> true after repeated failures
REPLICAS = "config.replicas"  # service:<s>@<h> -> int (a read)
MENTIONS = "mentions"  # doc:<name>@<h> -> [hosts]
EXPECTED = "replicas"  # registry:<s> -> int
DRIFT = "drift.replicas"  # service:<s>@<h> -> {"expected", "actual"} (a claim)
VERDICT = "verdict"  # service:<s>@<h> -> "match" after a clean compare

# Fact statuses.
OBSERVED = "observed"
INFERRED = "inferred"
VERIFIED = "verified"
REFUTED = "refuted"
SUPERSEDED = "superseded"


def host_subject(host: str) -> str:
    return f"host:{host}"


def service_subject(service: str, host: str) -> str:
    return f"service:{service}@{host}"


def doc_subject(name: str, host: str) -> str:
    return f"doc:{name}@{host}"


def registry_subject(service: str) -> str:
    return f"registry:{service}"
