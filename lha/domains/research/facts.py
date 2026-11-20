"""How the research facts are named.

A fact is one small, typed claim of the form (subject, key) -> value, and
only one row for a (subject, key) is current at a time. The statuses are the
same in every domain.

    plan:research          cells        {"subjects": [...], "attributes": [...]}
    page:<url>             read         {"site", "source", "pages"} once read, or false if it doesn't exist
    page:<url>             unreachable  true after repeated failures
    page:<url>             links        [urls] the page links to
    search:<query>         results      [urls]
    claim:<cell>@<url>     value        {"value", "site", "source"}, what the page states
    answer:<cell>          value        the value, verified once independent sources agree

A cell is `<subject>|<attribute>`, such as `Atlas|license`, and a page's
source is the site it is on, or for a word-for-word copy of a page on
another site, that page's site, so a copy never counts as a second source.
"""

PLAN = "plan:research"
CELLS = "cells"
READ = "read"
UNREACHABLE = "unreachable"
LINKS = "links"
RESULTS = "results"
VALUE = "value"


def cell_id(subject: str, attribute: str) -> str:
    return f"{subject}|{attribute}"


def split_cell(cell: str) -> tuple[str, str]:
    subject, attribute = cell.split("|", 1)
    return subject, attribute


def page_subject(url: str) -> str:
    return f"page:{url}"


def search_subject(query: str) -> str:
    return f"search:{' '.join(query.lower().split())}"


def claim_subject(subject: str, attribute: str, url: str) -> str:
    return f"claim:{cell_id(subject, attribute)}@{url}"


def answer_subject(subject: str, attribute: str) -> str:
    return f"answer:{cell_id(subject, attribute)}"


def same_value(a: str, b: str) -> bool:
    """Whether two stated values are the same, ignoring case, spacing and
    punctuation around them ('Apache-2.0' and 'apache-2.0.' are, '2014' and
    '2015' aren't)."""
    return normal(a) == normal(b)


def normal(value: str) -> str:
    return " ".join(value.casefold().strip(" .,;:'\"()[]").split())
