"""How the facts of a domain loaded from configuration are named.

A fact is one small, typed claim of the form (subject, key) -> value, and
only one row for a (subject, key) is current at a time. An item is one
stage of one document, and a cell is one field of one document, which
belongs to the one stage that asks for it.

    item:<doc>|<stage>     read        {"pages", "done": [pages read]}, once a page of it is read
    item:<doc>|<stage>     unreadable  true once reading it for that stage was given up
    item:<doc>|<stage>     skipped     true when a stage it needs was given up for that document
    cell:<doc>|<field>     value       {"value", "quote", "page"}, verified against its page
    cell:<doc>|<field>     missing     true once every page is read and none states it
    cell:<doc>|<field>     conflict    [values], when two pages state different values
    config:progress        counts      {"docs", "items", "done", "unreadable", "skipped", "values",
                                        "missing", "conflicts"}

A value only counts once its quote has been found in the page it was read
from, and once every page of its document is read for its stage.
"""

READ = "read"
UNREADABLE = "unreadable"
SKIPPED = "skipped"
VALUE = "value"
MISSING = "missing"
CONFLICT = "conflict"
PROGRESS = "config:progress"
COUNTS = "counts"


def item_subject(doc: str, stage: str) -> str:
    return f"item:{doc}|{stage}"


def cell_subject(doc: str, field: str) -> str:
    return f"cell:{doc}|{field}"
