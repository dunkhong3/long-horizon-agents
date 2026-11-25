"""The task types of a domain loaded from configuration, with their inputs, outputs, keys and scopes.

Every stage is a task type of its own, named after the stage, with one task
per document, which reads the document, or one page of a long one, and
reports the value of each of the stage's fields with the sentence it comes
from, copied word for word, or that the document doesn't state it. The
writer writes a short summary at the end, and the table itself is written
by code, as in batch extraction, whose answer shape the stages share.
"""

from pydantic import BaseModel

from lha.domains.config.facts import PROGRESS, cell_subject, item_subject
from lha.domains.extract.tasks import FieldValue

# --- inputs ---------------------------------------------------------------------------


class StageInput(BaseModel):
    stage: str
    doc: str
    fields: list[str]
    page: int | None = None  # one page of a long document, or None for all of it
    round: int = 1


class ReportInput(BaseModel):
    partial: bool = False  # True when the run ended without meeting the goal


# --- outputs ----------------------------------------------------------------------------


class StageOutput(BaseModel):
    doc: str
    values: list[FieldValue]


class ReportOutput(BaseModel):
    summary: str  # markdown, from the counts in the prompt, and the table is added by code


# --- keys and scopes --------------------------------------------------------------------


def task_key(task_type: str, inp: BaseModel) -> str:
    """`<stage>:<doc>` (with `/p2` for one page of it, and '#n' after round 1)
    and `write_report`."""
    if isinstance(inp, StageInput):
        base = f"{inp.stage}:{inp.doc}" + ("" if inp.page is None else f"/p{inp.page}")
        return base if inp.round == 1 else f"{base}#{inp.round}"
    if isinstance(inp, ReportInput):
        return "write_report"
    raise TypeError(f"unknown input {inp!r}")


def task_scope(inp: BaseModel) -> list[str]:
    """A stage sees what every stage found in its document so far."""
    if isinstance(inp, StageInput):
        return [item_subject(inp.doc, "*"), cell_subject(inp.doc, "*")]
    if isinstance(inp, ReportInput):
        return [PROGRESS]
    raise TypeError(f"unknown input {inp!r}")
