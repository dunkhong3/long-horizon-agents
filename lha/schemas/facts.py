"""How facts are named.

A fact is one small, typed claim of the form (subject, key) -> value. The
same (subject, key) counts as 'the same fact', and only one row for it is
current at a time.
"""

# Fact keys.
EXISTS = "exists"  # host:<h> -> true / false
LISTING = "listing"  # host:<h> -> {"services": [...], "documents": [...]}, as the host listed them
UNREACHABLE = "unreachable"  # host:<h> -> true after repeated failures
REPLICAS = "config.replicas"  # service:<s>@<h> -> int (a read)
MENTIONS = "mentions"  # doc:<name>@<h> -> [hosts]
EXPECTED = "replicas"  # registry:<s> -> int
DRIFT = "drift.replicas"  # service:<s>@<h> -> {"expected", "actual"} (a claim)
VERDICT = "verdict"  # service:<s>@<h> -> "match" after a clean compare
BREAKER = "breaker"  # host:<h> -> {"state", "failures", "open_until"}

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


def split_service_subject(subject: str) -> tuple[str, str]:
    """'service:cache@host-4' -> ('cache', 'host-4')"""
    service, host = subject.removeprefix("service:").split("@")
    return service, host
