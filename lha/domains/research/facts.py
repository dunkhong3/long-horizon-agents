"""How the research brief's facts are named.

A fact is one small, typed claim of the form (subject, key) -> value, and
only one row for a (subject, key) is current at a time. The statuses are the
same in every domain.
"""

READ = "read"  # source:<s> -> true once read, false if it doesn't exist
UNREACHABLE = "unreachable"  # source:<s> -> true after repeated failures
CITES = "cites"  # source:<s> -> [sources]
YEAR = "year"  # claim:<project>@<source> -> int (what the source says),
#               answer:<project> -> int (verified when sources agree)


def source_subject(source: str) -> str:
    return f"source:{source}"


def claim_subject(project: str, source: str) -> str:
    return f"claim:{project}@{source}"


def answer_subject(project: str) -> str:
    return f"answer:{project}"
