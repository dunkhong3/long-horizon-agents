"""The research brief's task types, with their inputs, outputs, keys and scopes."""

from pydantic import BaseModel

from lha.core.schemas import VERIFIED
from lha.domains.research.facts import YEAR, answer_subject, source_subject

# --- inputs ---------------------------------------------------------------------------


class ReadInput(BaseModel):
    source: str
    round: int = 1


class ReconcileInput(BaseModel):
    project: str
    claims: int  # how many sources had made a claim when this was created
    round: int = 1


class BriefInput(BaseModel):
    partial: bool = False  # True when the run ended without meeting the goal


# --- outputs ----------------------------------------------------------------------------


class Claim(BaseModel):
    project: str
    year: int
    quote: str  # the sentence it comes from, word for word


class ReadOutput(BaseModel):
    source: str
    event_id: str  # the get_source call it was read from
    claims: list[Claim]  # every launch year the source states
    cites: list[str]  # every source it cites


class ReconcileOutput(BaseModel):
    project: str
    year: int | None  # None when the sources don't settle it yet
    claim_fact_ids: list[str]  # the agreeing claims


class Answer(BaseModel):
    project: str
    year: int
    fact_id: str  # the verified answer fact


class BriefOutput(BaseModel):
    findings: list[Answer]
    summary: str  # markdown


# --- keys and scopes --------------------------------------------------------------------


def task_key(task_type: str, inp: BaseModel) -> str:
    """`read_source:src-4`, `reconcile_project:Atlas/3` (after 3 claims) and `write_brief`,
    with '#n' for a new round."""
    if isinstance(inp, ReadInput):
        base = f"read_source:{inp.source}"
    elif isinstance(inp, ReconcileInput):
        base = f"reconcile_project:{inp.project}/{inp.claims}"
    elif isinstance(inp, BriefInput):
        return "write_brief"
    else:
        raise TypeError(f"unknown input {inp!r}")
    return base if inp.round == 1 else f"{base}#{inp.round}"


def task_scope(inp: BaseModel) -> list[str]:
    if isinstance(inp, ReadInput):
        return [source_subject(inp.source)]
    if isinstance(inp, ReconcileInput):
        return [f"claim:{inp.project}@*", answer_subject(inp.project)]
    if isinstance(inp, BriefInput):
        return [f"key:{YEAR}={VERIFIED}"]
    raise TypeError(f"unknown input {inp!r}")
