"""The extraction task types, with their inputs, outputs, keys and scopes.

A reader reads one document, or one page of a long one, and reports the
value of each field it states, each with the sentence it comes from copied
word for word, or that the document doesn't state it. The writer writes a
short summary at the end, and the table itself is written by code.
"""

from pydantic import BaseModel

from lha.domains.extract.facts import PROGRESS, cell_subject, doc_subject

# --- inputs ---------------------------------------------------------------------------


class ExtractInput(BaseModel):
    doc: str
    fields: list[str]
    page: int | None = None  # one page of a long document, or None for all of it
    round: int = 1


class TableInput(BaseModel):
    partial: bool = False  # True when the run ended without meeting the goal


# --- outputs ----------------------------------------------------------------------------


class FieldValue(BaseModel):
    field: str
    value: str | None  # None when the document (or this page of it) doesn't state it
    quote: str = ""  # the sentence it comes from, word for word
    event_id: str = ""  # the read_document call the quote was read in


class ExtractOutput(BaseModel):
    doc: str
    values: list[FieldValue]


class TableOutput(BaseModel):
    summary: str  # markdown, from the counts in the prompt, and the table is added by code


# --- keys and scopes --------------------------------------------------------------------


def task_key(task_type: str, inp: BaseModel) -> str:
    """`extract_doc:<doc>` (with `/p2` for one page of it, and '#n' after round 1)
    and `write_table`."""
    if isinstance(inp, ExtractInput):
        base = f"extract_doc:{inp.doc}" + ("" if inp.page is None else f"/p{inp.page}")
        return base if inp.round == 1 else f"{base}#{inp.round}"
    if isinstance(inp, TableInput):
        return "write_table"
    raise TypeError(f"unknown input {inp!r}")


def task_scope(inp: BaseModel) -> list[str]:
    if isinstance(inp, ExtractInput):
        return [doc_subject(inp.doc), cell_subject(inp.doc, "*")]
    if isinstance(inp, TableInput):
        return [PROGRESS]
    raise TypeError(f"unknown input {inp!r}")
