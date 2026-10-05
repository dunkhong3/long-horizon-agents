"""The library the research agents read, generated from a seed.

The sources are src-1 to src-N, and the agents start knowing only src-1 to
src-3. Every other source is cited by another one ('See also src-17.'), so
the sources form a tree that starts at the starting sources, much like the
audit's hosts. Sources state when projects launched ('The Atlas project
launched in 2014.'), and the question asks for the launch year of a few of
them.

Every project asked about has its true year stated in three sources, and an
outdated source states a wrong year, so a single source is never enough and
the first one read can be wrong. For the first project asked, two of its
three true sources sit at the end of a chain of citations below the deepest
other sources, so the answer is only found late in the run. One source cites
a source that doesn't exist, which is a dead citation.
"""

import random
import re
from dataclasses import dataclass, field

START_SOURCES = ("src-1", "src-2", "src-3")
CHAIN_LENGTH = 3
ASKED = {"one": 1, "all": 6}  # projects asked about, per goal

PROJECTS = (
    "Atlas", "Borealis", "Cobalt", "Dynamo", "Ember", "Fathom", "Granite", "Harbor",
    "Iris", "Juniper", "Kestrel", "Lumen", "Meridian", "Nimbus", "Onyx", "Polaris",
)  # fmt: skip
TOPICS = ("history", "notes", "review", "survey", "timeline", "overview", "retrospective", "digest")


@dataclass
class Library:
    seed: int
    n_sources: int
    texts: dict[str, str] = field(default_factory=dict)  # source -> text
    truth: dict[str, int] = field(default_factory=dict)  # project -> launch year
    asked: list[str] = field(default_factory=list)  # the projects the question asks about
    outdated: dict[str, tuple[str, int]] = field(default_factory=dict)  # project -> (source, wrong year)
    depth: dict[str, int] = field(default_factory=dict)  # source -> citations from the start


def claim(project: str, year: int) -> str:
    return f"The {project} project launched in {year}."


def generate_library(seed: int, n_sources: int = 20, n_asked: int = 1) -> Library:
    if n_sources < len(START_SOURCES) + CHAIN_LENGTH + 4:
        raise ValueError(f"need at least {len(START_SOURCES) + CHAIN_LENGTH + 4} sources")
    rng = random.Random(f"library|{seed}|{n_sources}")
    lib = Library(seed=seed, n_sources=n_sources)

    # --- the citation tree, built like the audit's host tree ----------------
    hidden = [f"src-{i}" for i in range(len(START_SOURCES) + 1, n_sources + 1)]
    rng.shuffle(hidden)
    cites: dict[str, list[str]] = {s: [] for s in (*START_SOURCES, *hidden)}
    for s in START_SOURCES:
        lib.depth[s] = 0
    chain, others = hidden[:CHAIN_LENGTH], hidden[CHAIN_LENGTH:]
    for source in others:
        parent = rng.choice([s for s, d in lib.depth.items() if d <= 2])
        cites[parent].append(source)
        lib.depth[source] = lib.depth[parent] + 1
    deepest = max(lib.depth.values())
    parent = rng.choice(sorted(s for s, d in lib.depth.items() if d == deepest))
    for source in chain:
        cites[parent].append(source)
        lib.depth[source] = lib.depth[parent] + 1
        parent = source

    # --- what the sources say ---------------------------------------------
    lib.truth = {p: rng.randint(1995, 2020) for p in PROJECTS}
    lib.asked = rng.sample(PROJECTS, n_asked)
    lines: dict[str, list[str]] = {s: [] for s in cites}
    shallow = sorted(s for s in others if lib.depth[s] <= 2)
    anywhere = sorted(others)
    for i, project in enumerate(lib.asked):
        year = lib.truth[project]
        if i == 0:
            # The first project's answer needs the end of the chain.
            true_sources = [chain[-1], chain[-2], rng.choice(anywhere)]
        else:
            true_sources = rng.sample(anywhere, 3)
        for source in true_sources:
            lines[source].append(claim(project, year))
        # An outdated source, read early, that gets the year wrong.
        stale = rng.choice([s for s in shallow if s not in true_sources])
        wrong = rng.choice([y for y in range(year - 3, year + 4) if y != year])
        lines[stale].append(claim(project, wrong))
        lib.outdated[project] = (stale, wrong)
    # Projects nobody asked about, as background.
    for project in PROJECTS:
        if project not in lib.asked:
            for source in rng.sample(anywhere, 2):
                lines[source].append(claim(project, lib.truth[project]))

    for source, kids in cites.items():
        lines[source] += [f"See also {kid}." for kid in kids]
    dead = f"src-{n_sources + rng.randint(10, 99)}"
    lines[rng.choice(anywhere)].append(f"See also {dead}.")

    for source in sorted(cites, key=lambda s: int(s.split("-")[1])):
        body = lines[source]
        rng.shuffle(body)
        lib.texts[source] = "\n".join([f"# {source} {rng.choice(TOPICS)}", "", *body])
    return lib


CLAIM = re.compile(r"The (\w+) project launched in (\d{4})\.")
CITE = re.compile(r"\bsrc-\d+\b")


def claims_in(text: str) -> list[tuple[str, int, str]]:
    """Every (project, year, sentence) a source states, in order."""
    return [(m.group(1), int(m.group(2)), m.group(0)) for m in CLAIM.finditer(text)]


def cites_in(text: str, source: str) -> list[str]:
    """Every other source a source cites."""
    return sorted(set(CITE.findall(text)) - {source})
