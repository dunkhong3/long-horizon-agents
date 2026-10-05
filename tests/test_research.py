"""The research brief's library, its settle rule and its source checks."""

import pytest
from sqlalchemy import select

from lha.core.coordinator import Coordinator
from lha.core.domain import Rejected
from lha.db import crud
from lha.db.models import tasks
from lha.domains.research import RESEARCH, rules
from lha.domains.research.tasks import ReadInput, ReadOutput
from lha.domains.research.world import ASKED, START_SOURCES, cites_in, claim, claims_in, generate_library
from tests.conftest import add_task, new_session


def test_every_asked_project_has_three_true_sources_and_one_outdated():
    for seed in range(20):
        lib = generate_library(seed, 60, ASKED["all"])
        texts = "\n".join(lib.texts.values())
        for project in lib.asked:
            assert texts.count(claim(project, lib.truth[project])) == 3
            stale, wrong = lib.outdated[project]
            assert wrong != lib.truth[project] and claim(project, wrong) in lib.texts[stale]
        # The first project's answer needs the end of the chain of citations.
        first = lib.asked[0]
        deepest = max(lib.depth.values())
        assert any(
            claim(first, lib.truth[first]) in lib.texts[s] for s, d in lib.depth.items() if d == deepest
        )


def test_same_seed_same_library():
    assert generate_library(5, 60, 6).texts == generate_library(5, 60, 6).texts


def test_settle_needs_two_agreeing_sources_that_outnumber_the_rest():
    assert rules.settle([2014]) is None
    assert rules.settle([2014, 2012]) is None
    assert rules.settle([2014, 2014]) == 2014
    assert rules.settle([2014, 2012, 2014]) == 2014
    assert rules.settle([2014, 2014, 2012, 2012]) is None


async def test_a_read_must_match_its_source_word_for_word(engine):
    sid = await new_session(engine, domain="research", n_hosts=60, start_points=list(START_SOURCES))
    coord = Coordinator(engine, sid)
    await coord.load()
    lib = RESEARCH.make_world(1, 60, "one")
    text = lib.texts["src-1"]
    async with engine.begin() as conn:
        await add_task(conn, sid, "read_source", ReadInput(source="src-1"), domain=RESEARCH)
        task = await crud.claim_task(conn, sid, "reader", "w")
        event = await crud.log_event(
            conn, sid, "w", "tool_call",
            {"tool": "get_source", "path": "/sources/src-1", "ok": True,
             "response": {"source": "src-1", "text": text}}, task.id, task.attempt,
        )  # fmt: skip
        task = (await conn.execute(select(tasks).where(tasks.c.id == task.id))).one()

        claims = [{"project": p, "year": y, "quote": q} for p, y, q in claims_in(text)]
        good = {"source": "src-1", "event_id": str(event), "claims": claims, "cites": cites_in(text, "src-1")}
        with pytest.raises(Rejected):  # a citation the text doesn't have
            await rules.accept_read(
                coord, conn, task, ReadOutput(**{**good, "cites": [*good["cites"], "src-999"]}), lib
            )
        if claims:
            wrong = [dict(c) for c in claims]
            wrong[0]["year"] += 1
            with pytest.raises(Rejected):  # a year off by one
                await rules.accept_read(coord, conn, task, ReadOutput(**{**good, "claims": wrong}), lib)
        await rules.accept_read(coord, conn, task, ReadOutput(**good), lib)
        read = await crud.current_fact(conn, sid, "source:src-1", "read")
        assert read is not None and read.value is True
