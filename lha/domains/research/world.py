"""The mock web the research agents search and read, generated from a seed.

Pages live on a handful of sites and have URLs such as
https://oss-wiki.example/atlas-notes-12. They state facts about software
projects in plain sentences ('The Atlas project launched in 2014.', 'Atlas
is released under the MIT license.'), and many of them link to other pages
('See also https://...'). Search finds only the shallow pages, so deeper
pages are found by following links, much like old pages that no search
engine shows.

The question asks for two attributes, the launch year and the license, of
one project, or of four with --goal all. Every value asked about is stated on
three pages on at least two sites, while an outdated page, which is one of
the pages search can find, states a wrong value, so a single page is never
enough. For the
first project asked, its outdated page is copied word for word by a mirror
site, so a copy must not count as a second source, and two of the three
pages with its launch year sit at the end of a chain of links below the
deepest other pages, so that answer is only found late in the run. One page
links to a page that doesn't exist, and one page is long enough to come in
several pages of text.

Links are drawn at random, so a draw can leave a page that states a value
asked about where no search and no link from what search finds leads to it,
and then the answer can't be found by any means. The web is drawn again from
the same seed until the links from what a search for each cell, which is the
project and the attribute, finds lead to every page that states a value
asked about, the outdated pages and the copy included.
"""

import random
import re
from dataclasses import dataclass, field

from lha.domains.research.rules import MAX_LINKS_PER_PAGE
from lha.domains.research.web import paginate

SITES = (
    "devhistory.example",
    "oss-wiki.example",
    "techdigest.example",
    "codearchive.example",
    "newsroom.example",
)
MIRROR_SITE = "mirror.example"
COPIED_SITE = "oss-wiki.example"  # the site the mirror copies from
MOCK_PAGE_CHARS = 1500  # pages of text the mock web serves, sized for the fake model's window
ROOT_PAGES = 6  # pages no page links to as their parent, which search finds
CHAIN_LENGTH = 3
CROSS_LINKS = 2  # links from each page to other pages besides its own children
LONG_PAGE_LINES = 120  # notes on the one long page, enough to need more than one attempt
SEARCH_RESULTS = 5
ASKED = {"one": 1, "all": 4}  # projects asked about, per goal
DRAWS = 100  # draws of one seed's web before giving up on one that keeps its promises

PROJECTS = (
    "Atlas", "Borealis", "Cobalt", "Dynamo", "Ember", "Fathom", "Granite", "Harbor",
    "Iris", "Juniper", "Kestrel", "Lumen", "Meridian", "Nimbus", "Onyx", "Polaris",
)  # fmt: skip
TOPICS = ("history", "notes", "review", "survey", "timeline", "overview", "retrospective", "digest")

# Each attribute asked about, with its possible values and the sentences
# pages state it in, where {s} is the project and {v} the value.
ATTRIBUTES: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "launch year": (
        tuple(str(y) for y in range(1995, 2021)),
        ("The {s} project launched in {v}.", "{s} was first released in {v}."),
    ),
    "license": (
        ("MIT", "Apache-2.0", "GPL-3.0", "BSD-3-Clause", "MPL-2.0", "LGPL-2.1"),
        ("{s} is released under the {v} license.", "The {s} code is licensed under {v}."),
    ),
}
QUESTION = "Find the {attributes} of each of these projects: {projects}."


@dataclass
class Page:
    url: str
    site: str
    title: str
    lines: list[str] = field(default_factory=list)
    depth: int = 0  # links followed from a page search finds
    indexed: bool = False  # whether search finds it

    @property
    def text(self) -> str:
        return "\n".join([f"# {self.title}", "", *self.lines])


@dataclass
class MockWeb:
    seed: int
    size: int
    pages: dict[str, Page] = field(default_factory=dict)  # url -> page
    truth: dict[tuple[str, str], str] = field(default_factory=dict)  # (project, attribute) -> value
    asked: list[str] = field(default_factory=list)  # the projects the question asks about
    outdated: dict[tuple[str, str], tuple[str, str]] = field(default_factory=dict)  # cell -> (url, value)
    mirror: dict[str, str] = field(default_factory=dict)  # copy's url -> original's url
    question: str = ""

    def search(self, query: str) -> list[dict[str, str]]:
        """The indexed pages that mention most words of the query, best first."""
        words = [w for w in re.findall(r"[a-z0-9-]+", query.lower()) if w not in STOPWORDS]
        scored = []
        for page in self.pages.values():
            if page.indexed:
                haystack = page.text.lower()
                score = sum(1 for w in words if w in haystack)
                if score:
                    scored.append((-score, page.url, page))
        return [{"url": p.url, "title": p.title} for _, _, p in sorted(scored)[:SEARCH_RESULTS]]

    def page_text(self, url: str, page: int) -> tuple[Page, list[str]] | None:
        found = self.pages.get(url)
        if found is None:
            return None
        return found, paginate(found.text, MOCK_PAGE_CHARS)


STOPWORDS = {"the", "of", "a", "an", "and", "in", "for", "what", "when", "which", "is", "each", "these"}


def cells_of(projects: list[str]) -> list[tuple[str, str]]:
    return [(p, a) for p in projects for a in ATTRIBUTES]


def generate_web(seed: int, size: int = 60, n_asked: int = 1) -> MockWeb:
    """The seed's web, from the first of its draws that keeps the promises in
    this module's docstring."""
    if size < ROOT_PAGES + CHAIN_LENGTH + 6:
        raise ValueError(f"need at least {ROOT_PAGES + CHAIN_LENGTH + 6} pages")
    for draw in range(DRAWS):
        rng = random.Random(f"web|{seed}|{size}" + (f"|{draw}" if draw else ""))
        try:
            web = _draw(rng, seed, size, n_asked)
        except _BadDraw:
            continue
        if findable(web):
            return web
    raise ValueError(f"no draw of {DRAWS} gave seed {seed} a web where every value can be found")


def findable(web: MockWeb) -> bool:
    """Whether the links from what a search for each cell finds lead to every
    page that states a value asked about."""
    found = {r["url"] for cell in cells_of(web.asked) for r in web.search(" ".join(cell))}
    reached = crawl(web, found)
    cells = set(cells_of(web.asked))
    return all(
        url in reached for url, page in web.pages.items() if any(c[:2] in cells for c in claims_in(page.text))
    )


def crawl(web: MockWeb, start: set[str]) -> set[str]:
    """Every page reached from `start` by following links the way a run does,
    where the fake reader reports every link on a page in order and the
    coordinator follows the first MAX_LINKS_PER_PAGE of them."""
    reached: set[str] = set()
    todo = list(start)
    while todo:
        url = todo.pop()
        if url in reached or url not in web.pages:
            continue
        reached.add(url)
        todo += [link for link in links_in(web.pages[url].text) if link != url][:MAX_LINKS_PER_PAGE]
    return reached


def _draw(rng: random.Random, seed: int, size: int, n_asked: int) -> MockWeb:
    web = MockWeb(seed=seed, size=size)

    # --- the pages and the links between them ------------------------------
    names = [f"page-{i}" for i in range(1, size + 1)]
    pages = [Page(url="", site=rng.choice(SITES), title=rng.choice(TOPICS)) for _ in names]
    roots, rest = pages[:ROOT_PAGES], pages[ROOT_PAGES:]
    chain, others = rest[:CHAIN_LENGTH], rest[CHAIN_LENGTH:]
    links: dict[int, list[int]] = {id(p): [] for p in pages}
    for page in roots:
        page.depth, page.indexed = 0, True
    placed = list(roots)
    for page in others:
        parent = rng.choice([p for p in placed if p.depth <= 2])
        links[id(parent)].append(id(page))
        page.depth = parent.depth + 1
        page.indexed = page.depth <= 1
        placed.append(page)
    deepest = max(p.depth for p in placed)
    parent = rng.choice([p for p in placed if p.depth == deepest])
    for page in chain:
        links[id(parent)].append(id(page))
        page.depth = parent.depth + 1
        parent = page
    shallow = [p for p in placed if p.indexed]
    # The first project's outdated page is on the site the mirror copies.
    first_stale = rng.choice(shallow)
    first_stale.site = COPIED_SITE
    anywhere = [p for p in others if p is not first_stale]
    by_id = {id(p): p for p in pages}

    # --- what the pages say -------------------------------------------------
    web.truth = {(p, a): rng.choice(values) for p in PROJECTS for a, (values, _) in ATTRIBUTES.items()}
    web.asked = rng.sample(PROJECTS, n_asked)

    def state(page: Page, project: str, attribute: str, value: str) -> None:
        template = rng.choice(ATTRIBUTES[attribute][1])
        page.lines.append(template.format(s=project, v=value))

    stale_pages: dict[tuple[str, str], tuple[Page, str]] = {}
    for i, cell in enumerate(cells_of(web.asked)):
        project, attribute = cell
        if i == 0:
            # The first value asked about needs the end of the chain.
            true_pages = [chain[-1], chain[-2], _other_site(rng, anywhere, chain[-1].site)]
        else:
            true_pages = _spread(rng, anywhere, 3)
        for page in true_pages:
            state(page, project, attribute, web.truth[cell])
        # An outdated page that search finds early, with a wrong value.
        if project == web.asked[0]:
            stale = first_stale
        else:
            stale = _pick(rng, [p for p in shallow if p not in true_pages])
        wrong = rng.choice([v for v in ATTRIBUTES[attribute][0] if v != web.truth[cell]])
        state(stale, project, attribute, wrong)
        stale_pages[cell] = (stale, wrong)
    for project in PROJECTS:
        if project not in web.asked:
            for attribute in ATTRIBUTES:
                for page in rng.sample(anywhere, 2):
                    state(page, project, attribute, web.truth[(project, attribute)])

    # One page is long, with notes that answer nothing, so it comes in pages.
    long_page = rng.choice([p for p in anywhere if p not in chain])
    for n in range(LONG_PAGE_LINES):
        project = rng.choice(PROJECTS)
        long_page.lines.append(f"{project} had about {rng.randint(5, 900)} contributors in its year {n + 1}.")

    # --- names, links and the mirror ---------------------------------------
    for page, name in zip(pages, names, strict=True):
        subjects = sorted({w for line in page.lines for w in PROJECTS if w in line})
        about = " and ".join(subjects[:3]) or "Open source"
        page.title = f"{about} {page.title}"
        page.url = f"https://{page.site}/{_slug(page.title)}-{name.split('-')[1]}"
    for page in others:
        # Pages also link across the web, though never into the chain, which
        # stays reachable only from its own end.
        for other in rng.sample([p for p in others if p is not page], CROSS_LINKS):
            if id(other) not in links[id(page)]:
                links[id(page)].append(id(other))
    for page, name in zip(pages, names, strict=True):
        page.lines.append(f"Posted on {page.site} as entry {name.split('-')[1]}.")
        page.lines += [f"See also {by_id[kid].url}." for kid in links[id(page)]]
    dead = rng.choice(shallow)
    dead.lines.append(f"See also https://{dead.site}/retired-notes-{size + rng.randint(10, 99)}.")
    for page in pages:
        rng.shuffle(page.lines)
    web.pages = {p.url: p for p in pages}
    web.outdated = {cell: (page.url, wrong) for cell, (page, wrong) in stale_pages.items()}

    copy = Page(
        url=first_stale.url.replace(f"https://{COPIED_SITE}/", f"https://{MIRROR_SITE}/"),
        site=MIRROR_SITE, title=first_stale.title, lines=list(first_stale.lines), indexed=True,
    )  # fmt: skip
    web.pages[copy.url] = copy
    web.mirror = {copy.url: first_stale.url}
    web.question = QUESTION.format(attributes=" and the ".join(ATTRIBUTES), projects=", ".join(web.asked))
    return web


class _BadDraw(Exception):
    """A draw that can't place everything the web needs, which happens in a
    small web, so the next draw is tried."""


def _pick(rng: random.Random, pool: list[Page]) -> Page:
    if not pool:
        raise _BadDraw
    return rng.choice(pool)


def _other_site(rng: random.Random, pool: list[Page], site: str) -> Page:
    return _pick(rng, [p for p in pool if p.site != site])


def _spread(rng: random.Random, pool: list[Page], n: int) -> list[Page]:
    """n pages from the pool on at least two different sites."""
    if len(pool) < n or len({p.site for p in pool}) < 2:
        raise _BadDraw
    while True:
        chosen = rng.sample(pool, n)
        if len({p.site for p in chosen}) >= 2:
            return chosen


def _slug(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


# --- reading the mock web, for the fake model -------------------------------------

_PATTERNS = [
    (
        attribute,
        re.compile(
            re.escape(t).replace(r"\{s\}", r"(?P<s>[A-Z][a-z]+)").replace(r"\{v\}", r"(?P<v>[\w.-]+?)")
        ),
    )
    for attribute, (_, templates) in ATTRIBUTES.items()
    for t in templates
]
LINK = re.compile(r"https://[a-z0-9.-]+\.example/[a-z0-9-]+")


def claims_in(text: str) -> list[tuple[str, str, str, str]]:
    """Every (project, attribute, value, sentence) the text states, in order."""
    found = []
    for line in text.split("\n"):
        for attribute, pattern in _PATTERNS:
            m = pattern.fullmatch(line)
            if m:
                found.append((m.group("s"), attribute, m.group("v"), line))
    return found


def links_in(text: str) -> list[str]:
    return sorted(set(LINK.findall(text)))
