"""How the extraction facts are named.

A fact is one small, typed claim of the form (subject, key) -> value, and
only one row for a (subject, key) is current at a time.

    doc:<doc>              read         {"pages", "done": [pages read]}, once a page of it is read
    doc:<doc>              unreadable   true once reading it was given up
    cell:<doc>|<field>     value        {"value", "quote", "page"}, verified against its page
    cell:<doc>|<field>     missing      true once every page is read and none states it
    cell:<doc>|<field>     conflict     [values], when two pages state different values
    extract:progress       counts       {"docs", "done", "values", "missing", "conflicts", "unreadable"}

A cell is one field of one document, and a value only counts once its quote
has been found in the page it was read from.
"""

READ = "read"
UNREADABLE = "unreadable"
VALUE = "value"
MISSING = "missing"
CONFLICT = "conflict"
PROGRESS = "extract:progress"
COUNTS = "counts"


def doc_subject(doc: str) -> str:
    return f"doc:{doc}"


def cell_subject(doc: str, field: str) -> str:
    return f"cell:{doc}|{field}"
