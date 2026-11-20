"""The research task types, with their inputs, outputs, keys and scopes.

A question is answered as a table of cells, where a cell is one attribute of
one subject, such as the license of Atlas. The planner names the subjects
and the attributes, searches and page reads find claims about the cells, and
a cell gets an answer once enough independent sources agree on its value.
"""

from urllib.parse import quote

from pydantic import BaseModel, Field

from lha.core.schemas import VERIFIED
from lha.domains.research.facts import PLAN, VALUE, answer_subject, cell_id, page_subject

# --- inputs ---------------------------------------------------------------------------


class PlanInput(BaseModel):
    question: str
    round: int = 1


class SearchInput(BaseModel):
    query: str
    round: int = 1


class ReadInput(BaseModel):
    url: str
    page: int | None = None  # one page of a long document, or None for all of it
    round: int = 1


class ReconcileInput(BaseModel):
    subject: str
    attribute: str
    sources: int  # how many independent sources had a claim when this was created
    round: int = 1


class ReportInput(BaseModel):
    partial: bool = False  # True when the run ended without meeting the goal


# --- outputs ----------------------------------------------------------------------------


class Attribute(BaseModel):
    name: str
    form: str = ""  # what a value looks like, such as 'a year' or 'an SPDX license id'


class PlanOutput(BaseModel):
    subjects: list[str]
    attributes: list[Attribute]
    queries: list[str]  # what to search for first


class Hit(BaseModel):
    url: str
    title: str = ""


class SearchOutput(BaseModel):
    query: str
    results: list[Hit]  # the results worth reading, each from a search the attempt made


class Claim(BaseModel):
    subject: str
    attribute: str
    value: str
    quote: str  # the sentence it comes from, word for word
    event_id: str  # the fetch_page call the quote was read from


class ReadOutput(BaseModel):
    url: str
    claims: list[Claim]  # what the page states about the planned cells
    links: list[str] = Field(default_factory=list)  # pages it links to that look worth reading


class ReconcileOutput(BaseModel):
    subject: str
    attribute: str
    value: str | None  # None when the sources don't settle it yet
    claim_fact_ids: list[str]  # the agreeing claims


class Answer(BaseModel):
    subject: str
    attribute: str
    value: str
    fact_id: str  # the verified answer fact


class ReportOutput(BaseModel):
    findings: list[Answer]  # one per verified answer, possibly none
    summary: str  # markdown, written from the verified answers only


# --- keys and scopes --------------------------------------------------------------------


def task_key(task_type: str, inp: BaseModel) -> str:
    """`plan_research`, `search_web:atlas launch year`, `read_page:<url>` (with
    `/p2` for one page of it), `reconcile_cell:Atlas|license/3` (after 3
    sources) and `write_report`, with '#n' for a new round."""
    if isinstance(inp, PlanInput):
        base = "plan_research"
    elif isinstance(inp, SearchInput):
        base = f"search_web:{' '.join(inp.query.lower().split())}"
    elif isinstance(inp, ReadInput):
        base = f"read_page:{inp.url}" + ("" if inp.page is None else f"/p{inp.page}")
    elif isinstance(inp, ReconcileInput):
        base = f"reconcile_cell:{cell_id(inp.subject, inp.attribute)}/{inp.sources}"
    elif isinstance(inp, ReportInput):
        return "write_report"
    else:
        raise TypeError(f"unknown input {inp!r}")
    return base if inp.round == 1 else f"{base}#{inp.round}"


def task_scope(inp: BaseModel) -> list[str]:
    if isinstance(inp, PlanInput):
        return []
    if isinstance(inp, (SearchInput, ReadInput)):
        return [PLAN] + ([page_subject(inp.url)] if isinstance(inp, ReadInput) else [])
    if isinstance(inp, ReconcileInput):
        cell = cell_id(inp.subject, inp.attribute)
        return [f"claim:{cell}@*", answer_subject(inp.subject, inp.attribute)]
    if isinstance(inp, ReportInput):
        return [PLAN, f"key:{VALUE}={VERIFIED}"]
    raise TypeError(f"unknown input {inp!r}")


def page_path(url: str, page: int) -> str:
    """The mock web's path for one page of a document."""
    return f"/page?url={quote(url, safe='')}&page={page}"
