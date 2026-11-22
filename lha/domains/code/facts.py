"""How the code facts are named.

A fact is one small, typed claim of the form (subject, key) -> value, and
only one row for a (subject, key) is current at a time.

    branch:work      head        {"commit", "fixes"}, the session branch as the coordinator left it
    item:<item>      status      {"status": "fixed" or "passing", "commit", "round"}, verified
    item:<item>      unfixable   true once fixing it was given up
    check:full       result      {"commit", "passed", "round", "failing"}, verified once it passes

An item is what the check takes in place of {item}, such as one test file,
and the whole check is the template with {item} left out.
"""

BRANCH = "branch:work"
HEAD = "head"
STATUS = "status"
UNFIXABLE = "unfixable"
CHECK = "check:full"
RESULT = "result"


def item_subject(item: str) -> str:
    return f"item:{item}"
