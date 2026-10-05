"""The naive baseline, which is one agent whose prompt is its whole history.

    python -m lha.baseline --seed 42                    # unlimited history
    python -m lha.baseline --seed 42 --window 4000      # history cut to fit a window
    python -m lha.baseline --seed 42 --reread           # re-read a mismatch once

This is the usual way an agent is built, and it is what we measure the
system against (see 'The benchmark' in docs/design.md). There is one Pydantic
AI agent run, and every tool call and every result stays in the run's
message history, which is sent to the model again on every call, so the
prompt grows with the run. When a window is set, the oldest messages are
dropped to make room, which is what a chat loop does when it runs out of
space, but the first message with the goal is always kept.

It audits the same mock network as the system, with the same tools, the
same seeded faults and the same seeded model errors, and its fake model
follows the obvious plan for the task. The difference is that the baseline
trusts what it reads, keeps all its state in the prompt and nowhere else, and
has no coordinator checking its work. It needs no Postgres, and a crash
loses everything.
"""

import argparse
import asyncio
import json
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart, ToolReturnPart, UserPromptPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.usage import UsageLimits

from lha.agents.base import part_content, parts_tokens
from lha.config import DEFAULT_FAULT_RATE, DEFAULT_HOSTS, DEFAULT_STEP_BUDGET, MODEL_ERROR_RATE
from lha.context import estimate_tokens
from lha.faults import roll
from lha.scoring import answer_checks, prompt_stats, repeated_reads
from lha.tools import ToolBox, ToolFailure
from lha.world.model import REGISTRY_DOC, START_HOSTS, generate_world, registry_entries

HOST_NAME = re.compile(r"\bhost-\d+\b")
GIVE_UP_AFTER = 3  # failures in a row of the same call, as seen in the window

GOAL = (
    "Audit the deployment. Exactly one service runs a different number of "
    "replicas than registry.json says. Find it and report it. You start "
    "knowing only these hosts: {hosts}."
)


class Finding(BaseModel):
    service: str | None
    host: str | None
    expected: int | None
    actual: int | None
    summary: str


@dataclass
class Run:
    """What one baseline run counts, kept in memory only."""

    seed: int
    window: int
    reread: bool
    step_budget: int
    tool_calls: int = 0
    model_calls: int = 0
    finals: int = 0
    model_errors: int = 0
    prompts: list[int] = field(default_factory=list)
    reads: list[tuple[str, bool]] = field(default_factory=list)  # (path, deliberate)

    @property
    def steps(self) -> int:
        return self.tool_calls + self.model_calls


# --- what the model can see -------------------------------------------------------


def visible(messages: list[ModelMessage], window: int) -> list[Any]:
    """The parts of the history that fit in the window, oldest first.

    The first part (the goal) is always kept, and then whole parts are added
    from the newest backwards for as long as they fit, so the oldest tool
    results are the first to go. A window of 0 means no limit.
    """
    parts = [part for msg in messages for part in msg.parts]
    if window <= 0 or not parts:
        return parts
    first, rest = parts[0], parts[1:]
    used = estimate_tokens(part_content(first))
    kept: list[Any] = []
    for part in reversed(rest):
        cost = estimate_tokens(part_content(part))
        if used + cost > window:
            break
        kept.append(part)
        used += cost
    return [first, *reversed(kept)]


@dataclass
class View:
    """What the model knows, read off the visible parts and nothing else."""

    start_hosts: list[str]
    hosts: dict[str, dict] = field(default_factory=dict)  # host -> get_host result
    missing: set[str] = field(default_factory=set)  # hosts that answered 404
    reads: dict[tuple[str, str], int] = field(default_factory=dict)  # (host, service) -> replicas
    rechecks: dict[tuple[str, str], int] = field(default_factory=dict)
    documents: dict[tuple[str, str, int], str] = field(default_factory=dict)  # (host, name, page) -> text
    pages: dict[tuple[str, str], int] = field(default_factory=dict)  # (host, name) -> page count
    mentioned: list[str] = field(default_factory=list)  # hosts named in documents, in order
    failures: dict[str, int] = field(default_factory=dict)  # call -> failures in a row
    last_failed: tuple[str, dict] | None = None  # the newest result, if it was a failure

    @property
    def registry(self) -> dict[str, int] | None:
        """The registry, once every page of it is visible at the same time."""
        key = (START_HOSTS[0], REGISTRY_DOC)
        if key not in self.pages or any((*key, p) not in self.documents for p in range(self.pages[key])):
            return None
        entries: dict[str, int] = {}
        for page in range(self.pages[key]):
            entries.update(registry_entries(self.documents[(*key, page)]))
        return entries


def read_view(parts: list[Any]) -> View:
    goal = parts[0].content if parts and isinstance(parts[0], UserPromptPart) else ""
    view = View(start_hosts=[h for h in HOST_NAME.findall(goal)])
    for part in parts:
        if not isinstance(part, ToolReturnPart):
            continue
        r = part.content
        call = _call_id(part.tool_name, r["args"])
        if r.get("error") == "not_found":
            # A 404 is an answer and not a fault, so it is never tried again.
            view.failures[call] = GIVE_UP_AFTER
            view.last_failed = None
            if part.tool_name == "get_host":
                view.missing.add(r["args"]["host"])
            continue
        if "error" in r:
            view.failures[call] = view.failures.get(call, 0) + 1
            view.last_failed = (part.tool_name, r["args"])
            continue
        view.failures[call] = 0
        view.last_failed = None
        if part.tool_name == "get_host":
            view.hosts[r["host"]] = r
        elif part.tool_name == "get_service":
            key = (r["host"], r["service"])
            (view.rechecks if r["args"].get("recheck") else view.reads)[key] = r["replicas"]
        elif part.tool_name == "fetch_document":
            view.documents[(r["host"], r["name"], r["page"])] = r["content"]
            view.pages[(r["host"], r["name"])] = r["pages"]
            for host in HOST_NAME.findall(r["content"]):
                if host != r["host"] and host not in view.mentioned:
                    view.mentioned.append(host)
    return view


def _call_id(tool: str, args: dict) -> str:
    return f"{tool}:{json.dumps(args, sort_keys=True)}"


# --- the fake model ---------------------------------------------------------------


@dataclass
class Next:
    tool: str
    args: dict[str, Any]


def policy(view: View, reread: bool) -> Next | dict[str, Any]:
    """The obvious plan, which is to read everything, compare with the registry, and report.

    It returns the next tool call, or the final finding as a dict.
    """
    # A call that just failed is tried again, unless it has failed too often.
    if view.last_failed is not None:
        tool, args = view.last_failed
        if view.failures[_call_id(tool, args)] < GIVE_UP_AFTER:
            return Next(tool, args)

    def gave_up(tool: str, args: dict) -> bool:
        return view.failures.get(_call_id(tool, args), 0) >= GIVE_UP_AFTER

    # Compare every read we can see with the registry, if we can see it.
    registry = view.registry
    if registry is not None:
        for (host, service), replicas in view.reads.items():
            expected = registry.get(service)
            if expected is None or replicas == expected:
                continue
            if not reread:
                return _finding(service, host, expected, replicas)
            again = view.rechecks.get((host, service))
            if again is None:
                args = {"host": host, "service": service, "recheck": True}
                if not gave_up("get_service", args):
                    return Next("get_service", args)
            elif again != expected:
                return _finding(service, host, expected, again)

    # Finish the hosts we have read, then visit the next one we know of.
    for host, info in view.hosts.items():
        for service in info["services"]:
            args = {"host": host, "service": service}
            if (host, service) not in view.reads and not gave_up("get_service", args):
                return Next("get_service", args)
        for name in info["documents"]:
            for page in range(view.pages.get((host, name), 1)):
                args = {"host": host, "name": name, "page": page}
                if (host, name, page) not in view.documents and not gave_up("fetch_document", args):
                    return Next("fetch_document", args)
    for host in [*view.start_hosts, *view.mentioned]:
        if host not in view.hosts and host not in view.missing and not gave_up("get_host", {"host": host}):
            return Next("get_host", {"host": host})
    return _finding(None, None, None, None)


def _finding(service, host, expected, actual) -> dict[str, Any]:
    if service is None:
        summary = "# Finding\n\nNo replica drift was found.\n"
    else:
        summary = (
            f"# Finding\n\n**{service}** on **{host}** runs {actual}; the registry expects {expected}.\n"
        )
    return {"service": service, "host": host, "expected": expected, "actual": actual, "summary": summary}


def fake_model(run: Run) -> FunctionModel:
    async def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        run.model_calls += 1
        parts = visible(messages, run.window)
        run.prompts.append(parts_tokens(parts))
        decision = policy(read_view(parts), run.reread)
        if run.steps >= run.step_budget:
            decision = _finding(None, None, None, None)
            decision["summary"] = "# Finding\n\nThe step budget ran out before a drift was found.\n"
        if isinstance(decision, Next):
            return ModelResponse(parts=[ToolCallPart(decision.tool, decision.args)])
        # The same seeded model errors as the system, on the final answer.
        r = roll(run.seed, "baseline:final", run.finals)
        run.finals += 1
        if r < MODEL_ERROR_RATE / 2:
            run.model_errors += 1
            decision = dict(list(decision.items())[1:])  # malformed, so Pydantic AI asks again
        elif r < MODEL_ERROR_RATE and decision["expected"] is not None:
            run.model_errors += 1
            decision = {**decision, "expected": decision["expected"] + 1}  # fabricated, and believed
        return ModelResponse(parts=[ToolCallPart(info.output_tools[0].name, decision)])

    return FunctionModel(respond)


# --- tools ------------------------------------------------------------------------


class Tools:
    """The system's ToolBox, with failures handed to the model as results.

    Reads made while exploring a host use the task key `discover_host:<host>`,
    and a deliberate re-check uses `verify_drift:<service>@<host>#1`, the same
    keys the system uses, so the fault rolls work the same way and the decoys
    behave the same (a first read is stale, a re-check is fresh).
    """

    def __init__(self, http: httpx.AsyncClient, run: Run):
        self.http = http
        self.run = run
        self.boxes: dict[str, ToolBox] = {}

    def box(self, task_key: str, deliberate: bool) -> ToolBox:
        if task_key not in self.boxes:

            async def log(kind: str, payload: dict[str, Any]) -> str:
                self.run.tool_calls += 1
                if payload.get("ok"):
                    self.run.reads.append((payload["path"], deliberate))
                return f"call-{self.run.tool_calls}"

            self.boxes[task_key] = ToolBox(self.http, task_key, 1, log)
        return self.boxes[task_key]

    async def get_host(self, host: str) -> dict:
        """Read a host, meaning its services and documents."""
        return await self._try({"host": host}, self.box(f"discover_host:{host}", False).get_host(host))

    async def get_service(self, host: str, service: str, recheck: bool = False) -> dict:
        """Read a service's replica count, where recheck=True asks again on purpose."""
        args: dict[str, Any] = {"host": host, "service": service}
        if recheck:
            args["recheck"] = True
        key = f"verify_drift:{service}@{host}#1" if recheck else f"discover_host:{host}"
        return await self._try(args, self.box(key, recheck).get_service(host, service))

    async def fetch_document(self, host: str, name: str, page: int = 0) -> dict:
        """Read one page of a document stored on a host."""
        args = {"host": host, "name": name, "page": page}
        return await self._try(
            args, self.box(f"discover_host:{host}", False).fetch_document(host, name, page)
        )

    async def _try(self, args: dict, call) -> dict:
        try:
            data = await call
        except ToolFailure as e:
            return {"args": args, "error": e.kind}
        data.pop("event_id", None)
        return {**data, "args": args}


# --- running it -------------------------------------------------------------------


async def run_baseline(
    world_url: str, seed: int, n_hosts: int, window: int, reread: bool, step_budget: int
) -> dict[str, Any]:
    """One full baseline run, returning its finding, its score and its numbers."""
    run = Run(seed=seed, window=window, reread=reread, step_budget=step_budget)
    started = time.monotonic()
    async with httpx.AsyncClient(base_url=world_url) as http:
        tools = Tools(http, run)
        agent = Agent(
            fake_model(run),
            output_type=Finding,
            retries=3,  # a malformed answer is sent back to the model, as usual
            tools=[tools.get_host, tools.get_service, tools.fetch_document],
        )
        result = await agent.run(
            GOAL.format(hosts=", ".join(START_HOSTS)), usage_limits=UsageLimits(request_limit=None)
        )
    report = result.output.model_dump()
    checks = answer_checks(generate_world(seed, n_hosts), report)
    return {
        "passed": all(ok for _, ok in checks),
        "checks": checks,
        "report": report,
        "stats": {
            "steps": run.steps,
            "model_calls": run.model_calls,
            "tool_calls": run.tool_calls,
            "model_errors_injected": run.model_errors,
            **prompt_stats(run.prompts),
            "repeated_reads": repeated_reads(run.reads),
            "elapsed": round(time.monotonic() - started, 1),
        },
    }


async def main(args: argparse.Namespace) -> int:
    from lha.run import start_world

    world = await start_world(args.seed, args.hosts, args.chaos)
    try:
        out = await run_baseline(world.url, args.seed, args.hosts, args.window, args.reread, args.step_budget)
    finally:
        await world.stop()
    print(out["report"]["summary"].strip())
    print()
    for name, ok in out["checks"]:
        print(f"  {'✓' if ok else '✗'} {name}")
    print()
    print(" ".join(f"{k}={v}" for k, v in out["stats"].items()))
    print(f"verdict={'PASS' if out['passed'] else 'FAIL'}")
    if args.out:
        _save(out, args.out)
    return 0 if out["passed"] else 1


def _save(out: dict[str, Any], path: str) -> None:
    Path(path).write_text(json.dumps(out, indent=2))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--hosts", type=int, default=DEFAULT_HOSTS)
    p.add_argument("--chaos", type=float, default=DEFAULT_FAULT_RATE)
    p.add_argument("--step-budget", type=int, default=DEFAULT_STEP_BUDGET)
    p.add_argument("--window", type=int, default=0, help="prompt window in tokens (0 = no limit)")
    p.add_argument("--reread", action="store_true", help="re-read a mismatch once before reporting it")
    p.add_argument("--out", help="also save the result as JSON here")
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(asyncio.run(main(parse_args())))
