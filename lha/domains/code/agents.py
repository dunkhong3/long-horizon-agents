"""The code agents, each with its instructions, its fake policy, its tools and a fabricated variant.

The checker runs the whole check on the branch and names the failing items,
the coder fixes one item with edits it has seen pass, and the writer writes
the report from the verified facts. The fake policies only ever run on the
mock repository, whose functions and test messages follow a pattern they
can parse, so they fix a function the way a careful person would, which
includes checking a helper's own test before blaming the function that
calls it.
"""

import re
from typing import Any

from pydantic import BaseModel

from lha.core.agent import AgentContext, Call, Final, run_agent
from lha.core.schemas import VERIFIED, ContextPacket
from lha.domains.code.facts import BRANCH, HEAD, STATUS
from lha.domains.code.tasks import CheckOutput, FixOutput, ReportOutput
from lha.domains.code.tools import PAGE_LINES
from lha.domains.code.workspace import edits_digest

HELPER_TEST = "tests/test_helpers.py"
HELPER_FILE = "mathlib/helpers.py"


class EditArg(BaseModel):
    path: str
    old: str
    new: str


# --- the checker ----------------------------------------------------------------------

CHECK_INSTRUCTIONS = """\
Run the whole check with run_check and no item. If it passes, say so. If it
fails, list every failing item it names, each copied word for word from its
output as the check would take it in place of {item}, such as one test file.
Make no other tool calls.
"""

FAILED = re.compile(r"^FAILED (\S+?)::", re.M)


def check_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Call | Final:
    if not results:
        return Call("run_check", {"item": ""})
    run = results[-1][1]
    items = [] if run["passed"] else list(dict.fromkeys(FAILED.findall(run.get("output", ""))))
    return Final({"passed": run["passed"], "items": items, "check_event_id": run["event_id"]})


def check_fabricate(output: dict[str, Any]) -> dict[str, Any]:
    """A believable lie, which is a check that passed when it didn't, or an item it never named."""
    if not output["passed"]:
        return {**output, "passed": True, "items": []}
    return {**output, "passed": False, "items": ["tests/test_made_up.py"]}


async def run_check_all(ctx: AgentContext) -> CheckOutput:
    async def run_check(item: str = "") -> dict:
        """Run the check on the branch, for one item, or for all of it when item is empty."""
        return await ctx.tools.run_check(item, [])

    return await run_agent(ctx, CheckOutput, check_policy, check_fabricate, [run_check])


# --- the coder ------------------------------------------------------------------------

FIX_INSTRUCTIONS = """\
Make the item in your input pass. Run its check with run_check and no edits
first, and if it already passes, answer 'passing' with that call's
event_id. Otherwise find the cause with search and read_file, which read the
branch as it is now, and propose edits, each replacing text that is in its
file exactly once. A failure can come from code the failing code uses, so
check that code's own items too. Never edit the tests. Call run_check with
the item and your edits, and once it passes, answer 'fixed' with exactly
those edits and that call's event_id. If an edit can't be made, run_check
says why, so fix the edit and run it again.
"""

ASSERT = re.compile(r"AssertionError: (\w+)\((-?\d+)\) should be (-?\d+), got (-?\d+)")
LINEAR = re.compile(r"return x \* (-?\d+) \+ (-?\d+)$")
TIMES = re.compile(r"return x \* (-?\d+)$")
SCALED = re.compile(r"return scale\(x\) \+ (-?\d+)$")


def fix_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Call | Final:
    item = packet.pinned.input["item"]
    checks = [r for tool, r in results if tool == "run_check"]
    if not checks:
        return Call("run_check", {"item": item, "edits": []})
    first = checks[0]
    if first["passed"]:
        return Final({"item": item, "status": "passing", "edits": [], "check_event_id": first["event_id"]})
    searches = {r["text"]: r for tool, r in results if tool == "search"}
    reads = {(r["path"], r["page"]): r for tool, r in results if tool == "read_file" and "text" in r}
    edits: list[dict[str, str]] = []

    def source(fn: str) -> tuple[str, str, Call | None]:
        """The path and the two lines of a function's definition, or the call that finds them."""
        query = f"def {fn}("
        if query not in searches:
            return "", "", Call("search", {"text": query})
        path, line, _ = searches[query]["matches"][0].split(":", 2)
        start, lines = int(line), []
        for page in sorted({(start - 1) // PAGE_LINES["fake"], start // PAGE_LINES["fake"]}):
            if (path, page) not in reads:
                return "", "", Call("read_file", {"path": path, "page": page})
            r = reads[(path, page)]
            lines += [(r["first_line"] + i, t) for i, t in enumerate(r["text"].split("\n"))]
        text = dict(lines)
        return path, f"{text[start]}\n{text[start + 1]}", None

    def k_now() -> tuple[int | None, Call | None]:
        """What scale multiplies by once fixed, from its own test, and the edit that fixes it."""
        helper_runs = [r for r in checks if r["item"] == HELPER_TEST and not r["paths"]]
        if not helper_runs:
            return None, Call("run_check", {"item": HELPER_TEST, "edits": []})
        path, code, call = source("scale")
        if call:
            return None, call
        current = int(TIMES.search(code).group(1))
        wrong = ASSERT.findall(helper_runs[0].get("output", ""))
        if not wrong:
            return current, None
        _, x, expected, _ = wrong[0]
        right = int(expected) // int(x)
        if right != current:
            edits.append({"path": path, "old": code, "new": code.replace(f"x * {current}", f"x * {right}")})
        return right, None

    seen = set()
    for fn, x, expected, _ in ASSERT.findall(first.get("output", "")):
        if fn in seen:
            continue
        seen.add(fn)
        path, code, call = source(fn)
        if call:
            return call
        x, expected = int(x), int(expected)
        if m := LINEAR.search(code):
            a, b = int(m.group(1)), int(m.group(2))
            new = code[: m.start()] + f"return x * {a} + {expected - x * a}"
        elif m := TIMES.search(code):
            new = code[: m.start()] + f"return x * {expected // x}"
        elif m := SCALED.search(code):
            k, call = k_now()
            if call:
                return call
            b = int(m.group(1))
            new = (
                code[: m.start()] + f"return scale(x) + {expected - x * k}" if expected - x * k != b else code
            )
        else:
            continue
        if new != code:
            edits.append({"path": path, "old": code, "new": new})
    edits = list({(e["path"], e["old"]): e for e in edits}.values())
    runs = [r for r in checks if r["item"] == item]
    if not edits:
        # Nothing left to change, which happens when another fix, such as the
        # helper's, landed on the branch after the first run, so run it again.
        again = [r for r in runs[1:] if not r["paths"]]
        if not again:
            return Call("run_check", {"item": item, "edits": []})
        if again[-1]["passed"]:
            return Final(
                {"item": item, "status": "passing", "edits": [], "check_event_id": again[-1]["event_id"]}
            )
        return Final({"item": item, "status": "fixed", "edits": [], "check_event_id": first["event_id"]})
    tried = [r for r in runs if r["edits_digest"] == edits_digest(edits)]
    if not tried:
        return Call("run_check", {"item": item, "edits": edits})
    return Final({"item": item, "status": "fixed", "edits": edits, "check_event_id": tried[-1]["event_id"]})


def fix_fabricate(output: dict[str, Any]) -> dict[str, Any]:
    """A believable lie, which is a fix other than the one that was checked."""
    if not output["edits"]:
        made_up = {"path": "mathlib/__init__.py", "old": "", "new": "FIXED = True\n"}
        return {**output, "status": "fixed", "edits": [made_up]}
    edits = [dict(e) for e in output["edits"]]
    edits[0]["new"] += "  # fixed"
    return {**output, "edits": edits}


async def run_fix(ctx: AgentContext) -> FixOutput:
    async def run_check(item: str, edits: list[EditArg]) -> dict:
        """Run the check for one item on the branch with these edits, without committing them."""
        return await ctx.tools.run_check(item, [e.model_dump() for e in edits])

    async def search(text: str) -> dict:
        """Every line on the branch that contains this text, as path:line:text."""
        return await ctx.tools.search(text)

    async def read_file(path: str, page: int = 0) -> dict:
        """One page of a file on the branch, where the answer says how many pages it has."""
        return await ctx.tools.read_file(path, page)

    async def list_files(prefix: str = "") -> dict:
        """The files on the branch whose path starts with this prefix."""
        return await ctx.tools.list_files(prefix)

    tools = [run_check, search, read_file, list_files]
    return await run_agent(ctx, FixOutput, fix_policy, fix_fabricate, tools)


# --- the writer -----------------------------------------------------------------------

REPORT_INSTRUCTIONS = """\
Write the report from the verified item facts in your prompt. List every
verified item with its status and the id of its fact, and write a short
markdown summary of what was fixed. The commits and what is still failing
are added under it by code. Make no tool calls.
"""


def report_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Final:
    items = sorted(
        (
            f
            for f in packet.facts
            if f.key == STATUS and f.status == VERIFIED and f.subject.startswith("item:")
        ),
        key=lambda f: f.subject,
    )
    findings = [
        {"item": f.subject.removeprefix("item:"), "status": f.value["status"], "fact_id": f.id} for f in items
    ]
    branch = next((f.value for f in packet.facts if f.subject == BRANCH and f.key == HEAD), None)
    lines = ["# Code report", ""]
    fixed = [x["item"] for x in findings if x["status"] == "fixed"]
    lines.append(f"Fixed {len(fixed)} item(s) in {branch['fixes'] if branch else 0} commit(s).")
    lines += [
        f"- `{x['item']}` {'fixed' if x['status'] == 'fixed' else 'passes after other fixes'}"
        for x in findings
    ]
    return Final({"findings": findings, "summary": "\n".join(lines) + "\n"})


def report_fabricate(output: dict[str, Any]) -> dict[str, Any] | None:
    if not output["findings"]:
        return None
    return {**output, "findings": output["findings"][1:]}


async def run_report(ctx: AgentContext) -> ReportOutput:
    return await run_agent(ctx, ReportOutput, report_policy, report_fabricate)
