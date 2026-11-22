"""The code task types, with their inputs, outputs, keys and scopes.

A checker runs the whole check on the branch, and either it passes, which
meets the goal, or it names the failing items, each of which a coder fixes
in a task of its own. Once a round's fixes are done, the checker runs the
whole check again, which starts the next round, until it passes or the
rounds run out.
"""

from typing import Literal

from pydantic import BaseModel, Field

from lha.core.schemas import VERIFIED
from lha.domains.code.facts import BRANCH, CHECK, STATUS, item_subject

# --- inputs ---------------------------------------------------------------------------


class CheckInput(BaseModel):
    round: int = 1


class FixInput(BaseModel):
    item: str
    round: int = 1


class ReportInput(BaseModel):
    partial: bool = False  # True when the run ended without meeting the goal


# --- outputs ----------------------------------------------------------------------------


class CheckOutput(BaseModel):
    passed: bool
    items: list[str] = Field(default_factory=list)  # the failing items, word for word from the output
    check_event_id: str  # the run_check call of the whole check


class Edit(BaseModel):
    path: str
    old: str  # the text to replace, which must be in the file once, or '' for a new file
    new: str


class FixOutput(BaseModel):
    item: str
    status: Literal["fixed", "passing"]  # 'passing' when the item already passes on the branch
    edits: list[Edit] = Field(default_factory=list)
    check_event_id: str  # the run_check call that shows the item passing with exactly these edits


class Fixed(BaseModel):
    item: str
    status: str
    fact_id: str  # the verified item fact


class ReportOutput(BaseModel):
    findings: list[Fixed]  # one per verified item, possibly none
    summary: str  # markdown, written from the facts only


# --- keys and scopes --------------------------------------------------------------------


def task_key(task_type: str, inp: BaseModel) -> str:
    """`check_all#2` for round 2, `fix_item:tests/test_a.py` (with '#n' after
    round 1) and `write_report`."""
    if isinstance(inp, CheckInput):
        return "check_all" if inp.round == 1 else f"check_all#{inp.round}"
    if isinstance(inp, FixInput):
        return f"fix_item:{inp.item}" + ("" if inp.round == 1 else f"#{inp.round}")
    if isinstance(inp, ReportInput):
        return "write_report"
    raise TypeError(f"unknown input {inp!r}")


def task_scope(inp: BaseModel) -> list[str]:
    if isinstance(inp, CheckInput):
        return [BRANCH]
    if isinstance(inp, FixInput):
        return [item_subject(inp.item), BRANCH]
    if isinstance(inp, ReportInput):
        return [BRANCH, CHECK, f"key:{STATUS}={VERIFIED}"]
    raise TypeError(f"unknown input {inp!r}")
