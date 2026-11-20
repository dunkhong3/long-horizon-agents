"""Verified research, on its mock web and on the real web's reading path, offline."""

import httpx
import pytest
from pydantic_ai.messages import NativeToolReturnPart
from sqlalchemy import select

from lha.core.agent import native_call
from lha.core.coordinator import Coordinator
from lha.core.domain import Rejected
from lha.core.tools import NotFound
from lha.db import crud
from lha.db.models import tasks
from lha.domains.research import RESEARCH, rules
from lha.domains.research import facts as F
from lha.domains.research.tasks import ReadInput, ReadOutput, SearchInput, SearchOutput
from lha.domains.research.tools import ResearchTools, Unreadable
from lha.domains.research.web import MAX_BYTES, html_to_text, paginate
from lha.domains.research.world import (
    ASKED,
    MIRROR_SITE,
    cells_of,
    claims_in,
    findable,
    generate_web,
    links_in,
)
from lha.run import parse_args, question_problem
from tests.conftest import add_task, new_session

# --- the mock web -----------------------------------------------------------------------


def test_every_value_asked_about_is_on_three_pages_on_two_sites_and_one_outdated():
    for seed in range(15):
        web = generate_web(seed, 60, ASKED["all"])
        for cell in cells_of(web.asked):
            pages = [
                p
                for p in web.pages.values()
                if any(c[:3] == (*cell, web.truth[cell]) for c in claims_in(p.text))
            ]
            assert len(pages) >= 3 and len({p.site for p in pages}) >= 2
            url, wrong = web.outdated[cell]
            assert wrong != web.truth[cell] and (*cell, wrong) in [
                c[:3] for c in claims_in(web.pages[url].text)
            ]
        # The first project's outdated page is copied word for word by the mirror.
        ((copy, original),) = web.mirror.items()
        assert web.pages[copy].site == MIRROR_SITE and web.pages[copy].text == web.pages[original].text
        assert original == web.outdated[cells_of(web.asked)[0]][0]


def test_every_page_with_a_value_asked_about_is_reached_from_the_searches_for_its_cells():
    for seed in range(15):
        for n in ASKED.values():
            web = generate_web(seed, 60, n)
            # A run reads what the searches find and follows the links the
            # coordinator accepts, which are the first few of each page's.
            reached, todo = set(), [r["url"] for c in cells_of(web.asked) for r in web.search(" ".join(c))]
            while todo:
                url = todo.pop()
                if url in web.pages and url not in reached:
                    reached.add(url)
                    todo += links_in(web.pages[url].text)[: rules.MAX_LINKS_PER_PAGE]
            for cell in cells_of(web.asked):
                stating = [p.url for p in web.pages.values() if any(c[:2] == cell for c in claims_in(p.text))]
                assert set(stating) <= reached and web.outdated[cell][0] in reached
            assert set(web.mirror) <= reached
    # A page no search finds and no link leads to is caught.
    web = generate_web(10, 60, 1)
    cell = cells_of(web.asked)[0]
    hidden = next(
        p for p in web.pages.values() if not p.indexed and cell in [c[:2] for c in claims_in(p.text)]
    )
    for page in web.pages.values():
        page.lines = [line for line in page.lines if hidden.url not in line]
    assert not findable(web)


def test_search_finds_only_shallow_pages_and_the_same_seed_gives_the_same_web():
    web = generate_web(3, 60, 4)
    found = {r["url"] for project in web.asked for r in web.search(project)}
    assert found and all(web.pages[url].indexed for url in found)
    assert generate_web(3, 60, 4).pages.keys() == web.pages.keys()


def test_settle_counts_sources_and_not_pages():
    assert rules.settle({"2014": {"a", "b"}, "2012": {"c"}}) == "2014"
    assert rules.settle({"2014": {"a"}}) is None  # one source, however many pages it has
    assert rules.settle({"2014": {"a", "b"}, "2012": {"c", "d"}}) is None


# --- the checks on a result -------------------------------------------------------------


async def _session(engine):
    sid = await new_session(engine, domain="research", n_hosts=60, start_points=[], world="mock")
    coord = Coordinator(engine, sid)
    await coord.load()
    plan = {"subjects": ["Atlas"], "attributes": [{"name": "license", "form": ""}]}
    async with engine.begin() as conn:
        await crud.upsert_fact(conn, sid, F.PLAN, F.CELLS, plan, source_task_id=None)
    return sid, coord


async def _read(engine, sid, url, text):
    """A read task with one successful fetch of `text` logged in its attempt."""
    async with engine.begin() as conn:
        await add_task(conn, sid, "read_page", ReadInput(url=url), domain=RESEARCH)
        task = await crud.claim_task(conn, sid, "reader", "w")
        payload = {"tool": "fetch_page", "path": "/page", "ok": True,
                   "response": {"url": url, "title": "t", "page": 0, "pages": 1, "text": text}}  # fmt: skip
        event = await crud.log_event(conn, sid, "w", "tool_call", payload, task.id, task.attempt)
        return (await conn.execute(select(tasks).where(tasks.c.id == task.id))).one(), str(event)


async def test_a_read_must_quote_its_page_word_for_word(engine):
    sid, coord = await _session(engine)
    url = "https://oss-wiki.example/atlas-notes-1"
    text = (
        "# Atlas notes\n\nAtlas is released under the MIT license.\nSee also https://oss-wiki.example/more-2."
    )
    task, event = await _read(engine, sid, url, text)
    claim = {"subject": "Atlas", "attribute": "license", "value": "MIT",
             "quote": "Atlas is released under the MIT license.", "event_id": event}  # fmt: skip
    good = {"url": url, "claims": [claim], "links": ["https://oss-wiki.example/more-2"]}
    async with engine.begin() as conn:
        for bad, why in (
            ({**claim, "value": "GPL-3.0"}, "not in its quote"),
            ({**claim, "value": "MI"}, "not in its quote"),  # only part of a word
            ({**claim, "quote": "Atlas is released under the GPL-3.0 license."}, "doesn't say"),
        ):
            with pytest.raises(Rejected, match=why):
                await rules.accept_read(coord, conn, task, ReadOutput(**{**good, "claims": [bad]}))
        for link in (
            "https://x.example/y",
            "https://oss-wiki.example/more-",
        ):  # not on the page as a whole URL
            with pytest.raises(Rejected, match="doesn't link"):
                await rules.accept_read(coord, conn, task, ReadOutput(**{**good, "links": [link]}))
        await rules.accept_read(coord, conn, task, ReadOutput(**good))
        stated = await crud.current_fact(conn, sid, F.claim_subject("Atlas", "license", url), F.VALUE)
        assert stated.value == {"value": "MIT", "site": "oss-wiki.example", "source": "oss-wiki.example"}


async def test_a_copy_on_another_site_is_not_a_second_source(engine):
    sid, coord = await _session(engine)
    text = "# Atlas notes\n\nAtlas is released under the MIT license."
    for url in ("https://oss-wiki.example/atlas-notes-1", "https://mirror.example/atlas-notes-1"):
        task, event = await _read(engine, sid, url, text)
        claim = {"subject": "Atlas", "attribute": "license", "value": "MIT",
                 "quote": "Atlas is released under the MIT license.", "event_id": event}  # fmt: skip
        async with engine.begin() as conn:
            await rules.accept_read(coord, conn, task, ReadOutput(url=url, claims=[claim]))
    async with engine.begin() as conn:
        claims = await rules.claims_for(coord, conn, "Atlas", "license")
        assert {c.value["source"] for c in claims.values()} == {"oss-wiki.example"}
        reconciles = (await conn.execute(select(tasks).where(tasks.c.type == "reconcile_cell"))).all()
        assert not [t for t in reconciles if t.session_id == sid]  # one source, so nothing to settle yet


async def test_a_search_may_only_name_pages_it_found(engine):
    sid, coord = await _session(engine)
    async with engine.begin() as conn:
        await add_task(conn, sid, "search_web", SearchInput(query="atlas license"), domain=RESEARCH)
        task = await crud.claim_task(conn, sid, "searcher", "w")
        payload = {"tool": "web_search", "path": "/search", "ok": True,
                   "response": {"results": [{"url": "https://a.example/1", "title": "A"}]}}  # fmt: skip
        await crud.log_event(conn, sid, "w", "tool_call", payload, task.id, task.attempt)
        made_up = SearchOutput(query="atlas license", results=[{"url": "https://made-up.example/x"}])
        with pytest.raises(Rejected):
            await rules.accept_search(coord, conn, task, made_up)
        await rules.accept_search(
            coord, conn, task, SearchOutput(query="atlas license", results=[{"url": "https://a.example/1"}])
        )
        assert (await crud.current_fact(conn, sid, F.search_subject("atlas license"), F.RESULTS)).value == [
            "https://a.example/1"
        ]


async def test_a_stall_searches_again_for_a_cell_whose_sources_disagree(engine):
    sid, coord = await _session(engine)

    async def claim(url, value):
        site = url.split("/")[2]
        subject = F.claim_subject("Atlas", "license", url)
        value = {"value": value, "site": site, "source": site}
        await crud.upsert_fact(conn, sid, subject, F.VALUE, value, source_task_id=None)

    async def keys():
        return set((await conn.execute(select(tasks.c.task_key).where(tasks.c.session_id == sid))).scalars())

    async with engine.begin() as conn:
        await claim("https://a.example/1", "MIT")
        await claim("https://b.example/1", "GPL-3.0")
        # One source against one, which reconciling again can't settle.
        assert await rules.reopen(coord, conn) == 1 and await keys() == {"search_web:atlas#3"}
        await claim("https://c.example/1", "MIT")
        assert await rules.reopen(coord, conn) == 1
        assert await keys() == {"search_web:atlas#3", "reconcile_cell:Atlas|license/3#3"}


# --- the real web, offline --------------------------------------------------------------


def test_html_becomes_text_without_scripts_and_comes_in_pages():
    title, text = html_to_text(
        "<html><head><title> Atlas  notes </title><style>p{}</style></head><body><nav>menu</nav>"
        "<h1>Atlas</h1><p>Atlas is released under the <b>MIT</b> license, <a href='/more#top'>more</a>.</p>"
        "<script>x()</script></body></html>",
        "https://site.example/atlas",
    )
    assert title == "Atlas notes"
    assert (
        text
        == "Atlas\nAtlas is released under the MIT license, more.\nLinks on this page:\nhttps://site.example/more"
    )
    pages = paginate("\n".join(f"line {i} " + "word " * 20 for i in range(50)), size=400)
    assert len(pages) > 1 and all(len(p) <= 400 for p in pages)
    assert paginate("") == [""]


def test_a_native_search_is_logged_with_the_urls_it_found():
    part = NativeToolReturnPart(
        tool_name="web_search",
        content=[
            {
                "type": "web_search_result",
                "url": "https://a.example/1",
                "title": "A",
                "encrypted_content": "x",
            }
        ],
    )
    assert native_call(part) == {
        "tool": "web_search", "path": "native", "ok": True,
        "response": {"results": [{"url": "https://a.example/1", "title": "A"}]},
    }  # fmt: skip


def test_a_question_needs_the_research_domain_and_a_real_model():
    args = parse_args(["--question", "Who maintains Atlas?", "--model", "test"])
    assert args.domain == "research" and question_problem(args) is None
    audit = parse_args(["--domain", "audit", "--question", "Who maintains Atlas?", "--model", "test"])
    assert "mock world" in question_problem(audit)
    assert "real model" in question_problem(
        parse_args(["--question", "Who maintains Atlas?", "--model", "fake"])
    )
    assert parse_args([]).domain == "audit"
    assert args.max_seconds == 3 * 3600 and parse_args(["--model", "fake"]).max_seconds == 600


async def test_real_pages_are_read_as_text_in_pages():
    def serve(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/atlas":
            body = "<title>Atlas</title><p>" + "Atlas is fast. " * 600 + "</p>"
            return httpx.Response(200, text=body, headers={"content-type": "text/html"})
        if request.url.path == "/paper.pdf":
            return httpx.Response(200, content=b"%PDF", headers={"content-type": "application/pdf"})
        if request.url.path == "/huge.txt":
            return httpx.Response(200, content=b"word " * MAX_BYTES, headers={"content-type": "text/plain"})
        return httpx.Response(404)

    events = []

    async def log(kind, payload):
        events.append(payload)
        return "e"

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        tools = ResearchTools(http, "read_page:x", 1, log, world="web")
        first = await tools.fetch_page("https://site.example/atlas")
        assert first["title"] == "Atlas" and first["pages"] > 1 and "Atlas is fast." in first["text"]
        assert (await tools.fetch_page("https://site.example/atlas", 1))["page"] == 1
        with pytest.raises(Unreadable):
            await tools.fetch_page("https://site.example/paper.pdf")
        with pytest.raises(NotFound):
            await tools.fetch_page("https://site.example/gone")
        await tools.fetch_page("https://site.example/huge.txt")  # read only up to MAX_BYTES
        assert sum(len(p) for p in tools.documents["https://site.example/huge.txt"][1]) <= MAX_BYTES
    assert [e["ok"] for e in events] == [True, True, False, False, True]
