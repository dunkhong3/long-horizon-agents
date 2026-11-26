"""Domains loaded from configuration: the file, the stages' checks, waiting stages, and full runs."""

import copy
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select, update

from lha import cli, run
from lha.core.coordinator import Coordinator
from lha.core.domain import Rejected, create_start_tasks, domain_of
from lha.db import crud
from lha.db.models import tasks
from lha.domains.config import EXAMPLES, ConfigDomain, find_spec, register, rules
from lha.domains.config import facts as F
from lha.domains.config.agents import stage_instructions
from lha.domains.config.spec import SpecError, load_spec, parse_spec
from lha.domains.config.tasks import StageOutput
from tests.conftest import new_session, session_in

SPEC = {
    "name": "forms",
    "stage": [
        {
            "name": "a",
            "field": [
                {"name": "kind", "one_of": ["red", "blue"]},
                {"name": "code", "pattern": r"[A-Z]{2}-\d+"},
                {"name": "owner", "form": "who filled it in"},
            ],
        },
        {"name": "b", "needs": ["a"], "field": [{"name": "size"}]},
        {"name": "c", "after": ["a"], "field": [{"name": "note"}]},
    ],
    "document": [
        {
            "name": "x.txt",
            "text": "Kind: Red\nCode: AB-12\nOwner: Jo Smith\nSize: 3",
            "expect": {"kind": "red"},
        }
    ],
}
TEXT = SPEC["document"][0]["text"]

# --- the file -------------------------------------------------------------------------


def test_the_example_loads_with_its_stages_roles_and_instructions():
    spec = load_spec(EXAMPLES / "tickets.toml")
    assert [s.name for s in spec.stages] == ["classify", "route"] and spec.stage("route").needs == [
        "classify"
    ]
    assert spec.roles() == ("classify", "route", "writer")
    assert all(d.expect for d in spec.documents)
    text = stage_instructions(spec, spec.stage("route"))
    assert "'engineering', 'finance', 'product', 'support'" in text and "Route the ticket" in text
    # a judged option is still null when the document says nothing about it, which a real
    # model otherwise fills with a usual value, such as a priority of 'normal'
    assert "null when the document says nothing about it" in text
    assert parse_spec(spec.dump()) == spec  # what a session keeps loads back as it was


@pytest.mark.parametrize(
    "change, message",
    [
        (lambda s: s["stage"][1].update(needs=["c"]), "'c', which is not a stage before it"),
        (lambda s: s["stage"][1]["field"].append({"name": "Kind"}), "asked for twice"),
        (lambda s: s["stage"][0]["field"][1].update(pattern="(["), "doesn't compile"),
        (lambda s: s.update(report={"role": "a"}), "give it its own"),
        (lambda s: s.update(name="audit"), "built-in domain's name"),
        (lambda s: s["document"][0].update(expect={"colour": "red"}), "no stage asks for"),
        (lambda s: s["document"][0].update(expect={"kind": "green"}), "not one of the options"),
        (lambda s: s["document"][0].update(expect={"code": "ab-1"}), "doesn't match the pattern"),
        (lambda s: s.update(goal="Read {folder}"), "may only name"),
        (lambda s: s["stage"].append({"name": "write_report", "field": [{"name": "z"}]}), "reserved"),
        (lambda s: s.update(stages_typo=[]), "Extra inputs are not permitted"),
        (lambda s: s["document"].append({"name": "../up.txt", "text": "x"}), "plain relative name"),
    ],
)
def test_a_mistake_in_the_file_is_one_message_that_says_what_is_wrong(change, message):
    spec = _copy(SPEC)
    change(spec)
    with pytest.raises(SpecError, match=re.escape(message)):
        parse_spec(spec)


def test_a_file_that_is_not_toml_says_where(tmp_path):
    bad = tmp_path / "bad.toml"
    bad.write_text('name = "bad"\n[[stage]\n')
    with pytest.raises(SpecError, match="bad.toml"):
        load_spec(bad)


def test_a_field_takes_only_its_options_and_its_pattern():
    spec = parse_spec(_copy(SPEC))
    kind, code = spec.field("kind"), spec.field("CODE")
    assert (
        kind.option(" RED ") == "red" and kind.problem("Blue") is None and "options" in kind.problem("green")
    )
    assert code.problem("AB-12") is None and "pattern" in code.problem("AB-12x")


def test_a_domain_is_found_by_its_name_on_the_search_path(tmp_path, monkeypatch):
    mine = _copy(SPEC) | {"name": "mine"}
    (tmp_path / "mine.toml").write_text(_toml(mine))
    (tmp_path / "other.toml").write_text(_toml(mine))  # its file says other, the file says mine
    monkeypatch.setenv("LHA_DOMAIN_PATH", str(tmp_path))
    assert find_spec("mine").name == "mine" and find_spec("tickets").name == "tickets"
    with pytest.raises(SpecError, match="its file says 'other'"):
        find_spec("other")
    assert run.parse_args(["--domain", "mine"]).hosts == 1  # the size is its own documents
    with pytest.raises(SystemExit):
        run.parse_args(["--domain", "nothing-like-it"])


def test_a_domain_file_names_its_domain_and_what_it_runs_on(tmp_path):
    path = tmp_path / "anything.toml"
    path.write_text(_toml(_copy(SPEC) | {"name": "loaded"}))
    args = run.parse_args(["--domain-file", str(path)])
    assert args.domain == "loaded" and run.real_world_problem(args) is None
    assert "real model" in run.real_world_problem(run.parse_args(["--domain-file", str(path), "--docs", "."]))
    assert (
        run.real_world_problem(run.parse_args(["--domain", "loaded", "--docs", ".", "--model", "test"]))
        is None
    )
    empty = tmp_path / "empty.toml"
    empty.write_text(_toml({k: v for k, v in _copy(SPEC).items() if k != "document"} | {"name": "empty"}))
    assert "takes --docs" in run.real_world_problem(run.parse_args(["--domain-file", str(empty)]))


def test_lha_lists_the_domains_it_can_run(capsys):
    with pytest.raises(SystemExit) as done:
        cli.main(["domains"])
    out = capsys.readouterr().out
    assert done.value.code == 0 and "audit" in out and "tickets" in out and "LHA_DOMAIN_PATH" in out


# --- the coordinator's checks -------------------------------------------------------------


async def test_a_value_counts_only_when_its_quote_holds_it_and_its_field_allows_it(engine):
    sid, coord = await _session(engine)
    async with engine.begin() as conn:
        task, eid = await _read(conn, coord, "a")
        good = [
            {"field": "kind", "value": "Red", "quote": "Kind: Red", "event_id": eid},
            {"field": "code", "value": "AB-12", "quote": "Code: AB-12", "event_id": eid},
            {"field": "owner", "value": None},
        ]
        for i, bad, why in (
            (0, {"value": "green"}, "not one of the options"),
            (1, {"value": "AB-1"}, "not in its quote"),
            (1, {"value": "ab-12", "quote": "Code: AB-12"}, "doesn't match the pattern"),
            (0, {"quote": "Kind: Blue"}, "doesn't say"),
            (1, {"event_id": "not-an-event"}, "not a read of x.txt"),
        ):
            values = [dict(v) for v in good]
            values[i].update(bad)
            with pytest.raises(Rejected, match=why):
                await rules.accept_stage(coord, conn, task, StageOutput(doc="x.txt", values=values))
        await rules.accept_stage(coord, conn, task, StageOutput(doc="x.txt", values=good))
        kind = await crud.current_fact(conn, sid, F.cell_subject("x.txt", "kind"), F.VALUE)
        assert kind.status == "verified" and kind.value["value"] == "red"  # in the option's own spelling
        assert await crud.current_fact(conn, sid, F.cell_subject("x.txt", "owner"), F.MISSING)
        assert (await rules.count_progress(coord, conn))["done"] == 1


async def test_a_stage_that_needs_a_given_up_stage_is_skipped_while_one_after_it_runs(engine):
    sid, coord = await _session(engine)
    async with engine.begin() as conn:
        task, _ = await _read(conn, coord, "a")
        await coord._retry_or_fail(conn, task, "unreadable", "no text")
        await coord.settle_waiting(conn)
        assert await crud.current_fact(conn, sid, F.item_subject("x.txt", "a"), F.UNREADABLE)
        assert await crud.current_fact(conn, sid, F.item_subject("x.txt", "b"), F.SKIPPED)
        assert await _status(conn, sid, "c:x.txt") == "ready"
        counts = await rules.count_progress(coord, conn)
        assert (counts["unreadable"], counts["skipped"], counts["done"]) == (1, 1, 0)
        assert not await rules.goal_met(coord, conn)  # nothing was read


async def test_a_new_round_takes_over_the_stages_waiting_on_it(engine):
    sid, coord = await _session(engine)
    async with engine.begin() as conn:
        task, _ = await _read(conn, coord, "a")
        await conn.execute(update(tasks).where(tasks.c.id == task.id).values(attempt=task.max_attempts))
        task = (await conn.execute(select(tasks).where(tasks.c.id == task.id))).one()
        await coord._retry_or_fail(conn, task, "timeout", "the read timed out")
        assert await _status(conn, sid, "a:x.txt#2") == "ready"
        await coord.settle_waiting(conn)
        assert await _status(conn, sid, "b:x.txt") == "waiting"  # on round 2 now, and not failed
        assert await _status(conn, sid, "c:x.txt") == "waiting"


def test_its_own_documents_go_to_any_machine_and_a_folder_stays_home():
    spec = parse_spec(_copy(SPEC))
    assert not ConfigDomain(spec, "mock").task_types["a"].local
    assert (
        ConfigDomain(spec, "docs").task_types["a"].local
        and not ConfigDomain(spec, "docs").task_types["write_report"].local
    )

    def session(world):
        return SimpleNamespace(
            id=f"s-{world}", domain="forms", world=world, home_machine="alpha", world_url=None,
            params={"spec": spec.dump()},
        )  # fmt: skip

    assert run.join_problem(session("mock"), "beta") is None
    assert "nothing for another machine" in run.join_problem(session("docs"), "beta")


# --- full runs ------------------------------------------------------------------------------


def test_the_example_passes_on_its_own_documents_under_heavy_faults():
    result = _run("--domain", "tickets", "--seed", "11", "--chaos", "0.3")
    assert result.returncode == 0 and "verdict=PASS" in result.stdout, result.stdout + result.stderr
    assert "every value is right (43 stated)" in result.stdout and "splits=0 " not in result.stdout
    sid = re.search(r"session ([0-9a-f-]{36})", result.stdout).group(1)
    table = (Path("runs") / sid / "table.csv").read_text().splitlines()
    assert table[0] == "document,customer,product,category,team,priority" and len(table) == 10


def test_a_domain_file_resumes_from_the_session_alone(tmp_path):
    """The run is killed, its file is deleted, and the resume still has the
    domain, because the session keeps its spec."""
    path = tmp_path / "forms.toml"
    kinds = {i: "red" if i % 2 else "blue" for i in range(1, 7)}
    docs = [
        {"name": f"f-{i}.txt", "text": f"Kind: {kind}\nCode: AB-{i}\nOwner: P {i}\nSize: {i}",
         "expect": {"kind": kind, "code": f"AB-{i}", "size": str(i)}, "missing": ["note"]}
        for i, kind in kinds.items()
    ]  # fmt: skip
    path.write_text(_toml(_copy(SPEC) | {"name": "resumed", "document": docs}))
    crashed = _run("--domain-file", str(path), "--seed", "12", "--kill-at", "20")
    assert crashed.returncode == 137, crashed.stdout + crashed.stderr
    sid = re.search(r"--resume ([0-9a-f-]+)", crashed.stdout).group(1)
    path.unlink()
    resumed = _run("--resume", sid)
    assert resumed.returncode == 0 and "verdict=PASS" in resumed.stdout, resumed.stdout + resumed.stderr
    assert "every unstated value is marked missing (6)" in resumed.stdout


def test_a_second_machine_joins_a_run_of_a_domain_from_configuration(tmp_path):
    """Its own documents are in Postgres, so its stages go to any machine, and
    the joining machine builds the domain from the session alone, because the
    file is gone by the time it joins. The home machine has no workers for
    the stages, so the run only passes through the joined machine's work."""
    path = tmp_path / "joined.toml"
    path.write_text(_toml(_copy(SPEC) | {"name": "joined"}))
    cmd = [sys.executable, "-m", "lha.run", "--quiet", "--model", "fake"]
    log = tmp_path / "home.log"
    with open(log, "w") as out_file:
        home = subprocess.Popen(
            [*cmd, "--domain-file", str(path), "--machine", "alpha", "--workers", "0,0,0"],
            stdout=out_file, stderr=out_file,
        )  # fmt: skip
    try:
        sid = session_in(log)
        path.unlink()
        joined = subprocess.run(
            [*cmd, "--join", sid, "--wait", "30", "--machine", "beta"],
            capture_output=True,
            text=True,
            timeout=300,
        )
        home.wait(timeout=300)
    finally:
        home.kill()
    out = log.read_text()
    assert home.returncode == 0 and "verdict=PASS" in out and "machines: alpha (its home), beta" in out, out
    assert joined.returncode == 0 and "worked on 0 " not in joined.stdout, joined.stdout + joined.stderr


# --- helpers ------------------------------------------------------------------------------------


def _copy(spec: dict) -> dict:
    return copy.deepcopy(spec)


def _toml(spec: dict) -> str:
    """A spec written out as TOML, which the standard library can read but not write."""

    def value(v):
        if isinstance(v, str):
            return '"' + v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'
        if isinstance(v, list):
            return "[" + ", ".join(value(x) for x in v) + "]"
        if isinstance(v, dict):
            return "{ " + ", ".join(f'"{k}" = {value(x)}' for k, x in v.items()) + " }"
        return str(v)

    lines = [f"{k} = {value(v)}" for k, v in spec.items() if not isinstance(v, list)]
    for stage in spec.get("stage", []):
        lines.append("[[stage]]")
        lines += [f"{k} = {value(v)}" for k, v in stage.items() if k != "field"]
        for f in stage["field"]:
            lines.append("[[stage.field]]")
            lines += [f"{k} = {value(v)}" for k, v in f.items()]
    for doc in spec.get("document", []):
        lines.append("[[document]]")
        lines += [f"{k} = {value(v)}" for k, v in doc.items()]
    return "\n".join(lines) + "\n"


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "lha.run", "--quiet", "--model", "fake", *args],
        capture_output=True, text=True, timeout=300,
    )  # fmt: skip


async def _session(engine):
    """A session of the test spec, with every stage of its one document planned."""
    spec = parse_spec(_copy(SPEC))
    register(spec)
    domain = ConfigDomain(spec, "mock")
    params = domain.prepare(None, domain.make_world(1, 1, "one"), None)
    sid = await new_session(engine, domain="forms", n_hosts=1, start_points=[], world="mock", params=params)
    async with engine.begin() as conn:
        await create_start_tasks(conn, sid, domain, domain.start_tasks(None, "", params), partitions=1)
    coord = Coordinator(engine, sid, quiet=True)
    await coord.load()
    assert domain_of(coord.session).spec == spec
    return sid, coord


async def _read(conn, coord, stage):
    """The stage's task for x.txt, claimed, with its one page read in its attempt."""
    task = await crud.claim_task(conn, coord.sid, stage, "w")
    response = {"doc": "x.txt", "page": 0, "pages": 1, "text": TEXT}
    payload = {"tool": "read_document", "path": "read_document:x.txt#0", "ok": True, "response": response}
    eid = str(await crud.log_event(conn, coord.sid, "w", "tool_call", payload, task.id, task.attempt))
    return (await conn.execute(select(tasks).where(tasks.c.id == task.id))).one(), eid


async def _status(conn, sid, key):
    q = select(tasks.c.status).where(tasks.c.session_id == sid, tasks.c.task_key == key)
    return (await conn.execute(q)).scalar_one()
