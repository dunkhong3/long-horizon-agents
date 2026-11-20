"""The research rules, which are what the coordinator does with each kind of result.

A plan is kept within limits, and a search may only name pages its own
searches returned. A page read must match the page word for word, because
the coordinator finds each quote in the fetched text itself and each value
inside its quote, and a link must be on the page. A cell's value only
becomes a verified answer when two or more independent sources agree on it
and outnumber every other value, where two pages on one site are one source,
and so is a page and its word-for-word copy on another site. The report must
list exactly the verified answers, and the sources under it are written by
code from the facts.
"""

import hashlib
import re
import unicodedata
from collections import Counter
from typing import TYPE_CHECKING

from sqlalchemy import func, select, text
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.config import MAX_ROUNDS
from lha.core.domain import Rejected
from lha.core.schemas import SUPERSEDED, VERIFIED
from lha.db import crud
from lha.db.models import facts, tasks
from lha.domains.research import facts as F
from lha.domains.research.tasks import (
    PlanInput,
    PlanOutput,
    ReadInput,
    ReadOutput,
    ReconcileInput,
    ReconcileOutput,
    ReportOutput,
    SearchInput,
    SearchOutput,
)
from lha.domains.research.web import site_of

if TYPE_CHECKING:
    from lha.core.coordinator import Coordinator

MAX_SUBJECTS = 20
MAX_ATTRIBUTES = 6
MAX_QUERIES = 12
MAX_LINKS_PER_PAGE = 8


# --- the plan and the searches --------------------------------------------------------


async def accept_plan(coord: "Coordinator", conn: AsyncConnection, task: Row, out: PlanOutput) -> None:
    subjects = _distinct(out.subjects)
    attributes: dict[str, str] = {}
    for a in out.attributes:
        name = " ".join(a.name.split())
        if name and name.casefold() not in {n.casefold() for n in attributes}:
            attributes[name] = " ".join(a.form.split())
    queries = _distinct(out.queries)[:MAX_QUERIES]  # extra searches are left out, which changes no cell
    if not subjects or not attributes:
        raise Rejected("a plan needs at least one subject and one attribute")
    if len(subjects) > MAX_SUBJECTS or len(attributes) > MAX_ATTRIBUTES:
        raise Rejected(f"a plan may have {MAX_SUBJECTS} subjects and {MAX_ATTRIBUTES} attributes")
    if any("|" in name for name in [*subjects, *attributes]):
        raise Rejected("names may not contain '|'")
    plan = {"subjects": subjects, "attributes": [{"name": n, "form": f} for n, f in attributes.items()]}
    await crud.upsert_fact(conn, coord.sid, F.PLAN, F.CELLS, plan, source_task_id=task.id)
    for query in queries or [f"{s} {a}" for s in subjects for a in attributes][:MAX_QUERIES]:
        await coord.create(conn, "search_web", SearchInput(query=query), task.id)


async def accept_search(coord: "Coordinator", conn: AsyncConnection, task: Row, out: SearchOutput) -> None:
    inp = SearchInput(**task.input)
    cited = await coord.cited_responses(conn, task)
    returned = {
        r["url"] for p in cited.values() if p["tool"] == "web_search" for r in p["response"]["results"]
    }
    urls: list[str] = []
    for hit in out.results:
        if hit.url not in returned:
            raise Rejected(f"{hit.url} is not among the results of this attempt's searches")
        if hit.url not in urls:
            urls.append(hit.url)
    await crud.upsert_fact(
        conn, coord.sid, F.search_subject(inp.query), F.RESULTS, urls, source_task_id=task.id
    )
    for url in urls:
        await read_later(coord, conn, url, task.id)


async def read_later(coord: "Coordinator", conn: AsyncConnection, url: str, parent) -> None:
    """A page to read, unless the session's page budget is used up. On the
    real web the session's size is how many pages it may read, while the
    mock web is finite, so every page of it may be read."""
    session = coord.session
    if session.world == "web":
        q = select(func.count()).where(
            tasks.c.session_id == coord.sid,
            tasks.c.type == "read_page",
            tasks.c.input["page"].astext.is_(None),  # whole pages, and not the parts of a split one
        )
        if (await conn.execute(q)).scalar_one() >= session.n_hosts:
            return
    await coord.create(conn, "read_page", ReadInput(url=url), parent)


# --- reading pages --------------------------------------------------------------------


async def accept_read(coord: "Coordinator", conn: AsyncConnection, task: Row, out: ReadOutput) -> None:
    inp = ReadInput(**task.input)
    if out.url != inp.url:
        raise Rejected(f"read {out.url}, the task is {inp.url}")
    cited = await coord.cited_responses(conn, task)
    pages = {
        p["response"]["page"]: (eid, p["response"])
        for eid, p in cited.items()
        if p["tool"] == "fetch_page" and p["response"]["url"] == inp.url
    }
    if not pages:
        raise Rejected("the page was never read")
    total = next(iter(pages.values()))[1]["pages"]
    wanted = {inp.page} if inp.page is not None else set(range(total))
    if missing := wanted - set(pages):
        raise Rejected(f"pages {sorted(missing)} of {inp.url} were not read")

    plan = await crud.current_fact(conn, coord.sid, F.PLAN, F.CELLS)
    names = plan.value if plan is not None else {"subjects": [], "attributes": []}
    cells = {
        (_fold(s), _fold(a["name"])): (s, a["name"]) for s in names["subjects"] for a in names["attributes"]
    }
    kept = []
    for claim in out.claims:
        source = cited.get(claim.event_id)
        if source is None or source["tool"] != "fetch_page" or source["response"]["url"] != inp.url:
            raise Rejected(f"claim cites {claim.event_id}, which is not a read of {inp.url} in this attempt")
        if not _within(claim.quote, source["response"]["text"]):
            raise Rejected(f"the page doesn't say {claim.quote[:80]!r}")
        if not _within(claim.value, claim.quote, whole_words=True):
            raise Rejected(f"the value {claim.value!r} is not in its quote")
        cell = cells.get((_fold(claim.subject), _fold(claim.attribute)))
        if cell is not None:  # values the plan didn't ask for are left out
            kept.append((cell, claim))
    on_page = {url for _, r in pages.values() for url in urls_in(r["text"])}
    links: list[str] = []
    for link in out.links:
        if link not in on_page:
            raise Rejected(f"the page doesn't link to {link}")
        if link not in links and link != inp.url:
            links.append(link)

    site = site_of(inp.url)
    source = await _source(coord, conn, task, site, pages[min(pages)][1])
    page_fact = {"site": site, "source": source, "pages": total}
    await crud.upsert_fact(
        conn, coord.sid, F.page_subject(inp.url), F.READ, page_fact, source_task_id=task.id
    )
    if links:
        await crud.upsert_fact(
            conn, coord.sid, F.page_subject(inp.url), F.LINKS, links, source_task_id=task.id
        )
    for (subject, attribute), claim in kept:
        await crud.upsert_fact(
            conn, coord.sid, F.claim_subject(subject, attribute, inp.url), F.VALUE,
            {"value": claim.value, "site": site, "source": source},
            evidence=[{"quote": claim.quote, "event_id": claim.event_id}], source_task_id=task.id,
        )  # fmt: skip
    for link in links[:MAX_LINKS_PER_PAGE]:
        await read_later(coord, conn, link, task.id)

    # A cell is reconciled again whenever it has a new source, from its second
    # one on. Pages about one cell can be read by different coordinators at
    # once, so the sources are counted under a lock per cell, otherwise two
    # reads could each miss the other.
    for subject, attribute in sorted({cell for cell, _ in kept}):
        await _lock(conn, f"cell:{coord.sid}:{F.cell_id(subject, attribute)}")
        n = len({c.value["source"] for c in (await claims_for(coord, conn, subject, attribute)).values()})
        if n >= 2 and not await crud.current_fact(
            conn, coord.sid, F.answer_subject(subject, attribute), F.VALUE
        ):
            inp_r = ReconcileInput(subject=subject, attribute=attribute, sources=n)
            await coord.create(conn, "reconcile_cell", inp_r, task.id)


async def _source(coord: "Coordinator", conn: AsyncConnection, task: Row, site: str, page: dict) -> str:
    """The source a page counts as, which is its own site, unless a page with
    exactly the same text was read first on another site, whose source it
    shares. The text's fingerprint is looked up under a lock, so a page and
    its copy read at the same moment by two coordinators can't both win."""
    digest = hashlib.sha256(f"{page['page']}:{page['text']}".encode()).hexdigest()[:16]
    await _lock(conn, f"copy:{coord.sid}:{digest}")
    first = await crud.current_fact(conn, coord.sid, f"text:{digest}", "source")
    if first is not None:
        return first.value
    await crud.upsert_fact(conn, coord.sid, f"text:{digest}", "source", site, source_task_id=task.id)
    return site


async def claims_for(
    coord: "Coordinator", conn: AsyncConnection, subject: str, attribute: str
) -> dict[str, Row]:
    """The current claims about a cell, by fact id."""
    q = select(facts).where(
        facts.c.session_id == coord.sid,
        facts.c.subject.like(f"claim:{_like(F.cell_id(subject, attribute))}@%"),
        facts.c.key == F.VALUE,
        facts.c.status != SUPERSEDED,
    )
    return {str(r.id): r for r in (await conn.execute(q)).all()}


# --- answers and the report -----------------------------------------------------------


def sources_by_value(claims: dict[str, Row]) -> dict[str, set[str]]:
    """The sources behind each value the claims give, by normalized value."""
    out: dict[str, set[str]] = {}
    for c in claims.values():
        out.setdefault(F.normal(c.value["value"]), set()).add(c.value["source"])
    return out


def settle(sources: dict[str, set[str]]) -> str | None:
    """The value two or more sources agree on and that more sources state than
    any other value, from {normalized value: sources}, or None."""
    ranked = Counter({value: len(s) for value, s in sources.items()}).most_common()
    if ranked and ranked[0][1] >= 2 and (len(ranked) == 1 or ranked[0][1] > ranked[1][1]):
        return ranked[0][0]
    return None


async def accept_reconcile(
    coord: "Coordinator", conn: AsyncConnection, task: Row, out: ReconcileOutput
) -> None:
    inp = ReconcileInput(**task.input)
    claims = await claims_for(coord, conn, inp.subject, inp.attribute)
    value = settle(sources_by_value(claims))
    # The coordinator settles it again from the facts, and the model's answer
    # has to be the same, with citations that back it.
    claimed = None if out.value is None else F.normal(out.value)
    if (out.subject, out.attribute) != (inp.subject, inp.attribute) or claimed != value:
        raise Rejected(f"claimed {out.value!r}, the claims settle on {value!r}")
    if value is None:
        return  # not settled yet, and a new source will bring a new reconciliation
    cited = [claims.get(i) for i in dict.fromkeys(out.claim_fact_ids)]
    if any(c is None or F.normal(c.value["value"]) != value for c in cited):
        raise Rejected("the answer cites a claim that is missing or disagrees")
    if len({c.value["source"] for c in cited}) < 2:
        raise Rejected("the answer must cite claims from two or more sources")
    evidence = [{"fact_id": str(c.id), "url": c.subject.split("@", 1)[1]} for c in cited]
    await crud.upsert_fact(
        conn, coord.sid, F.answer_subject(inp.subject, inp.attribute), F.VALUE, out.value,
        status=VERIFIED, evidence=evidence, source_task_id=task.id,
    )  # fmt: skip


async def accept_report(coord: "Coordinator", conn: AsyncConnection, task: Row, out: ReportOutput) -> None:
    """The report must list exactly the verified answers, each matching its fact,
    and the sources under its summary are written here, from the facts."""
    verified = await _answers(coord, conn)
    cited = [a.fact_id for a in out.findings]
    if sorted(cited) != sorted(verified):
        raise Rejected(f"the report cites {len(cited)} answer(s), {len(verified)} are verified")
    for answer in out.findings:
        fact = verified[answer.fact_id]
        if fact.subject != F.answer_subject(answer.subject, answer.attribute) or not F.same_value(
            answer.value, fact.value
        ):
            raise Rejected(
                f"the report says {answer.value!r} for {fact.subject}, the fact says {fact.value!r}"
            )
    report = out.model_dump(mode="json")
    report["summary"] = out.summary.rstrip() + "\n\n" + await sources_section(coord, conn, verified)
    await coord.finish(conn, report, succeeded=bool(out.findings) and not task.input["partial"])


async def _answers(coord: "Coordinator", conn: AsyncConnection) -> dict[str, Row]:
    q = select(facts).where(
        facts.c.session_id == coord.sid,
        facts.c.subject.like("answer:%"),
        facts.c.key == F.VALUE,
        facts.c.status == VERIFIED,
    )
    return {str(r.id): r for r in (await conn.execute(q)).all()}


async def sources_section(coord: "Coordinator", conn: AsyncConnection, verified: dict[str, Row]) -> str:
    """Where every answer comes from, and what is still open, all from the facts."""
    plan = await crud.current_fact(conn, coord.sid, F.PLAN, F.CELLS)
    lines = ["## Sources", ""]
    answered = {r.subject: r for r in verified.values()}
    open_cells = []
    for subject in plan.value["subjects"] if plan else []:
        for attribute in (a["name"] for a in plan.value["attributes"]):
            claims = await claims_for(coord, conn, subject, attribute)
            answer = answered.get(F.answer_subject(subject, attribute))
            if answer is None:
                open_cells.append((subject, attribute, claims))
                continue
            lines.append(f"- The {attribute} of {subject} is {answer.value}, which these sources agree on.")
            for e in answer.evidence:
                claim = claims.get(e["fact_id"])
                quote = claim.evidence[0]["quote"] if claim is not None and claim.evidence else ""
                lines.append(f'  - {e["url"]}, which says "{quote}"')
    if open_cells:
        lines += ["", "## Not settled", ""]
        for subject, attribute, claims in open_cells:
            seen = Counter(c.value["value"] for c in claims.values())
            heard = ", ".join(f"{v} ({n} page(s))" for v, n in seen.most_common()) or "nothing yet"
            lines.append(f"- The {attribute} of {subject}, where the pages read say {heard}.")
    return "\n".join(lines) + "\n"


# --- replanning, splitting, the goal and stalls ---------------------------------------


async def replan(
    coord: "Coordinator", conn: AsyncConnection, task: Row, kind: str, delay: float, probe: bool
) -> None:
    if task.type == "read_page":
        inp = ReadInput(**task.input)
        subject = F.page_subject(inp.url)
        if kind in ("not_found", "unreadable"):
            await crud.upsert_fact(conn, coord.sid, subject, F.READ, False, source_task_id=task.id)
            return await coord.decide(conn, task, "replan", f"page {kind.replace('_', ' ')}; dropped")
        await crud.upsert_fact(conn, coord.sid, subject, F.UNREACHABLE, True, source_task_id=task.id)
        if inp.round < MAX_ROUNDS:
            nxt = inp.model_copy(update={"round": inp.round + 1})
            await coord.create(conn, task.type, nxt, task.id, delay, probe=probe)
            return await coord.decide(conn, task, "replan", f"new round {nxt.round} later")
    elif task.type in ("plan_research", "search_web", "reconcile_cell"):
        model = {"plan_research": PlanInput, "search_web": SearchInput, "reconcile_cell": ReconcileInput}
        inp = model[task.type](**task.input)
        if inp.round < MAX_ROUNDS:
            nxt = inp.model_copy(update={"round": inp.round + 1})
            await coord.create(conn, task.type, nxt, task.id, delay, probe=probe)
            return await coord.decide(conn, task, "replan", f"new round {nxt.round} later")
    await coord.decide(conn, task, "replan", "out of rounds; given up")


async def split(coord: "Coordinator", conn: AsyncConnection, task: Row, reason: str) -> bool:
    """A document too long for one attempt is read one page per task, where
    the number of pages comes from the failed attempt's own read of page 0."""
    if task.type != "read_page":
        return False
    inp = ReadInput(**task.input)
    if inp.page is not None:
        return False
    first = next(
        (
            p["response"]
            for p in (await coord.cited_responses(conn, task)).values()
            if p["tool"] == "fetch_page" and p["response"]["url"] == inp.url and p["response"]["page"] == 0
        ),
        None,
    )
    if first is None or first["pages"] < 2:
        return False
    for page in range(first["pages"]):
        await coord.create(conn, "read_page", ReadInput(url=inp.url, page=page, round=inp.round), task.id)
    await coord.mark_split(conn, task, f"{reason}; split into {first['pages']} pages")
    return True


async def goal_met(coord: "Coordinator", conn: AsyncConnection) -> bool:
    """Every cell of the plan has a verified answer."""
    plan = await crud.current_fact(conn, coord.sid, F.PLAN, F.CELLS)
    if plan is None:
        return False
    for subject in plan.value["subjects"]:
        for attribute in plan.value["attributes"]:
            answer = await crud.current_fact(
                conn, coord.sid, F.answer_subject(subject, attribute["name"]), F.VALUE
            )
            if answer is None or answer.status != VERIFIED:
                return False
    return True


async def reopen(coord: "Coordinator", conn: AsyncConnection) -> int:
    """One more round for pages given up as unreachable, a reconciliation for
    cells whose claims settle but have no answer, and a search of its own for
    the cells whose claims don't settle, where nothing is already working on
    them. A cell whose sources disagree one against one needs another source,
    which reconciling again can't give it."""
    active = await coord.active_tasks(conn)
    working = {(t, i.get("url"), i.get("subject"), i.get("attribute"), i.get("query")) for t, i in active}
    facts_now = await coord.current_facts(conn)
    plan: list[tuple[str, object]] = []
    for subject in facts_now.get(F.UNREACHABLE, {}):
        url = subject.removeprefix("page:")
        if ("read_page", url, None, None, None) not in working and subject not in facts_now.get(F.READ, {}):
            plan.append(("read_page", ReadInput(url=url, round=MAX_ROUNDS + 1)))
    cells = await crud.current_fact(conn, coord.sid, F.PLAN, F.CELLS)
    if cells is None:
        if not any(t == "plan_research" for t, _ in active):
            plan.append(("plan_research", PlanInput(question=coord.session.goal, round=MAX_ROUNDS + 1)))
    else:
        for subject in cells.value["subjects"]:
            for attribute in (a["name"] for a in cells.value["attributes"]):
                if F.answer_subject(subject, attribute) in facts_now.get(F.VALUE, {}):
                    continue
                by_value = sources_by_value(await claims_for(coord, conn, subject, attribute))
                if settle(by_value) is not None:
                    if ("reconcile_cell", None, subject, attribute, None) not in working:
                        n = len(set().union(*by_value.values()))
                        nxt = ReconcileInput(
                            subject=subject, attribute=attribute, sources=n, round=MAX_ROUNDS + 1
                        )
                        plan.append(("reconcile_cell", nxt))
                else:
                    query = subject  # a wider search than the plan's, for pages it missed
                    if ("search_web", None, None, None, query) not in working:
                        plan.append(("search_web", SearchInput(query=query, round=MAX_ROUNDS + 1)))
    created = 0
    for task_type, inp in plan:
        if await coord.create(conn, task_type, inp, None):
            created += 1
    return created


# --- text -----------------------------------------------------------------------------

URL = re.compile(r"https?://[^\s<>\"'()\[\]]+")


def urls_in(text: str) -> set[str]:
    """Every whole URL in a text, without the punctuation that ends a sentence."""
    return {m.rstrip(".,;:!?") for m in URL.findall(text)}


def _within(needle: str, haystack: str, whole_words: bool = False) -> bool:
    """Whether a quote or value appears in a text, ignoring case, spacing and
    the difference between curly and straight quotes and dashes, where a
    value must be whole words, so '19' is not in 'launched in 1997'."""
    needle, haystack = _flat(needle), _flat(haystack)
    if not needle:
        return False
    if whole_words:
        return re.search(rf"(?<!\w){re.escape(needle)}(?!\w)", haystack) is not None
    return needle in haystack


def _flat(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    for a, b in (("‘", "'"), ("’", "'"), ("“", '"'), ("”", '"'), ("–", "-"), ("—", "-")):
        s = s.replace(a, b)
    return " ".join(s.casefold().split())


def _fold(name: str) -> str:
    return " ".join(name.casefold().split())


def _distinct(names: list[str]) -> list[str]:
    seen, out = set(), []
    for name in (" ".join(n.split()) for n in names):
        if name and name.casefold() not in seen:
            seen.add(name.casefold())
            out.append(name)
    return out


def _like(s: str) -> str:
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


async def _lock(conn: AsyncConnection, key: str) -> None:
    await conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"), {"key": key})
