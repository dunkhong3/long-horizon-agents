"""Batch extraction: the mock invoices, reading documents, and the coordinator's checks."""

import pytest
from sqlalchemy import select

from lha.core.coordinator import Coordinator
from lha.core.domain import Rejected
from lha.db import crud
from lha.db.models import sessions, tasks
from lha.domains.extract import EXTRACT, parse_fields, rules
from lha.domains.extract import facts as F
from lha.domains.extract.agents import PATTERNS
from lha.domains.extract.documents import PAGE_CHARS, DocumentError, list_documents, pages_of, read_text
from lha.domains.extract.tasks import ExtractInput, ExtractOutput
from lha.domains.extract.world import ASKED, generate_docs
from lha.run import parse_args, real_world_problem
from tests.conftest import add_task, new_session

# --- the mock invoices ------------------------------------------------------------------


def test_every_asked_value_is_stated_once_and_only_where_it_should_be(tmp_path):
    for seed in range(5):
        docs = generate_docs(seed, 40, "all")
        for name, text in docs.files.items():
            (tmp_path / name).write_text(text)
        assert any(v is None for v in docs.truth.values())  # some dates aren't stated
        for (name, field), value in docs.truth.items():
            lines = [
                line.strip() for p in pages_of(tmp_path, name, PAGE_CHARS["fake"]) for line in p.split("\n")
            ]
            found = [m.group("v") for line in lines for pat in PATTERNS[field] if (m := pat.fullmatch(line))]
            assert found == ([] if value is None else [value]), (name, field)
        assert all(len(pages_of(tmp_path, d, PAGE_CHARS["fake"])) > 2 for d in docs.long_docs)
    assert set(ASKED["one"]) < set(ASKED["all"])


def test_fields_are_named_once_each_with_what_their_values_look_like():
    assert parse_fields("invoice number; total: the amount to pay;; Total ; a|b") == [
        {"name": "invoice number", "form": ""},
        {"name": "total", "form": "the amount to pay"},
    ]


def test_only_text_documents_in_the_folder_are_read(tmp_path):
    (tmp_path / "a.txt").write_text("Invoice number: INV-1.")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.html").write_text("<p>Total due: 5.00 EUR.</p><script>x()</script>")
    (tmp_path / "c.pdf").write_bytes(b"%PDF")
    (tmp_path / ".hidden").mkdir()
    (tmp_path / ".hidden" / "d.txt").write_text("secret")
    assert list_documents(tmp_path) == ["a.txt", "sub/b.html"]
    assert read_text(tmp_path, "sub/b.html") == "Total due: 5.00 EUR."
    for bad in ("../x.txt", "c.pdf.txt"):
        with pytest.raises(DocumentError):
            read_text(tmp_path, bad)


# --- the coordinator's checks -------------------------------------------------------------------


async def test_a_value_counts_only_when_its_quote_is_on_its_page(engine):
    sid, coord = await _session(engine, ["a.txt"])
    text = "INVOICE\n\nInvoice number: INV-2024-0042.\nTotal due: 120.00 EUR."
    async with engine.begin() as conn:
        task, eid = await _read(conn, sid, "a.txt", {0: text})
        number = {
            "field": "invoice number",
            "value": "INV-2024-0042",
            "quote": "Invoice number: INV-2024-0042.",
        }
        total = {"field": "total", "value": "120.00 EUR", "quote": "Total due: 120.00 EUR."}
        good = [
            {**number, "event_id": eid[0]},
            {**total, "event_id": eid[0]},
            {"field": "date", "value": None},
        ]
        for i, bad, why in (
            (0, {"value": "INV-2024-0041"}, "not in its quote"),
            (0, {"quote": "Invoice number: INV-2024-0041."}, "doesn't say"),
            (1, {"event_id": "not-an-event"}, "not a read of a.txt"),
            (0, {"value": "INV-2024"}, "not in its quote"),  # part of a word
        ):
            values = [dict(v) for v in good]
            values[i].update(bad)
            with pytest.raises(Rejected, match=why):
                await rules.accept_extract(coord, conn, task, ExtractOutput(doc="a.txt", values=values))
        await rules.accept_extract(coord, conn, task, ExtractOutput(doc="a.txt", values=good))
        cell = await crud.current_fact(conn, sid, F.cell_subject("a.txt", "total"), F.VALUE)
        assert cell.status == "verified" and cell.value["value"] == "120.00 EUR"
        # Every page is read, so the date no page states is missing.
        assert await crud.current_fact(conn, sid, F.cell_subject("a.txt", "date"), F.MISSING)
        assert await rules.goal_met(coord, conn)


async def test_pages_read_apart_are_put_together_and_disagreements_kept(engine):
    sid, coord = await _session(engine, ["long.txt"])
    pages = {
        0: "Invoice number: INV-2024-0001.\nItem 1: part 100, 1 x 1.00.",
        1: "This is invoice INV-2024-0002.\nTotal due: 9.00 EUR.",
    }
    async with engine.begin() as conn:
        for page in (1, 0):  # the later page first
            task, eid = await _read(conn, sid, "long.txt", {page: pages[page]}, page=page, total=2)
            number = "INV-2024-0001" if page == 0 else "INV-2024-0002"
            quote = pages[page].split("\n")[0]
            values = [{"field": "invoice number", "value": number, "quote": quote, "event_id": eid[page]}]
            if page == 1:
                values.append(
                    {
                        "field": "total",
                        "value": "9.00 EUR",
                        "quote": "Total due: 9.00 EUR.",
                        "event_id": eid[1],
                    }
                )
            await rules.accept_extract(coord, conn, task, ExtractOutput(doc="long.txt", values=values))
            if page == 1:
                assert not await rules.goal_met(coord, conn)  # page 0 isn't read yet
        number = await crud.current_fact(conn, sid, F.cell_subject("long.txt", "invoice number"), F.VALUE)
        assert number.value["value"] == "INV-2024-0001" and number.value["page"] == 0  # the lowest page wins
        assert number.status == "verified"  # once every page is read
        conflict = await crud.current_fact(
            conn, sid, F.cell_subject("long.txt", "invoice number"), F.CONFLICT
        )
        assert conflict.value == ["INV-2024-0001", "INV-2024-0002"]
        assert await crud.current_fact(conn, sid, F.cell_subject("long.txt", "date"), F.MISSING)
        assert await rules.goal_met(coord, conn)


def test_a_folder_of_documents_needs_a_real_model_and_its_fields():
    args = parse_args(["--docs", ".", "--fields", "invoice number; total", "--model", "test"])
    assert args.domain == "extract" and real_world_problem(args) is None
    assert "real model" in real_world_problem(
        parse_args(["--docs", ".", "--fields", "total", "--model", "fake"])
    )
    assert "--fields" in real_world_problem(parse_args(["--docs", ".", "--model", "test"]))


# --- helpers ------------------------------------------------------------------------------------


async def _session(engine, docs: list[str]):
    sid = await new_session(
        engine, domain="extract", seed=1, n_hosts=len(docs), start_points=[], world="mock"
    )
    params = {"folder": "/nowhere", "docs": docs, "n_docs": len(docs), "field_list": "",
              "fields": [{"name": n, "form": ""} for n in ("invoice number", "date", "total")]}  # fmt: skip
    async with engine.begin() as conn:
        await conn.execute(sessions.update().where(sessions.c.id == sid).values(params=params))
    coord = Coordinator(engine, sid)
    await coord.load()
    return sid, coord


async def _read(conn, sid, doc, texts: dict[int, str], page=None, total=None):
    """An extract task with these pages of the document read in its attempt."""
    inp = ExtractInput(doc=doc, fields=["invoice number", "date", "total"], page=page)
    await add_task(conn, sid, "extract_doc", inp, domain=EXTRACT)
    task = await crud.claim_task(conn, sid, "reader", "w")
    eid = {}
    for p, text in texts.items():
        response = {"doc": doc, "page": p, "pages": total or len(texts), "text": text}
        payload = {
            "tool": "read_document",
            "path": f"read_document:{doc}#{p}",
            "ok": True,
            "response": response,
        }
        eid[p] = str(await crud.log_event(conn, sid, "w", "tool_call", payload, task.id, task.attempt))
    return (await conn.execute(select(tasks).where(tasks.c.id == task.id))).one(), eid
