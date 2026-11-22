"""The code tools, which read the session branch and run its check.

They work on the git workspace directly, inside the worker, and not over
HTTP, but every call still goes through ToolBox._call, so it is logged as a
tool_call event with its full response, retried within the attempt when it
fails for a short-lived reason, and cited by results like any other call.
On the mock repository each call can fail on purpose, with the same seeded
dice the mock worlds use (lha/core/world.py), and a real repository gets no
injected faults.

Files are read from the branch head, the newest commit the coordinator has
made, and run_check always checks the head as well. A mistake in what the
model asked for, such as an edit whose text isn't in its file, comes back
as an answer that says what is wrong, so the model can try again within
the same attempt.
"""

import asyncio
import shlex
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Row

from lha.config import CONTEXT_WINDOW_TOKENS
from lha.core.tools import ToolBox, ToolFailure
from lha.domains.code import workspace as ws
from lha.faults import pick_fault, roll

PAGE_LINES = {"fake": 20, "real": 200}  # lines per page of a file, by the size of the window
SEARCH_MATCHES = 30
LISTED_FILES = 300


class CodeTools(ToolBox):
    def __init__(self, *args: Any, session: Row, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.work = Path(session.params["work"])
        self.template = session.params["check"].replace("{python}", shlex.quote(sys.executable))
        self.protected = tuple(session.params.get("protected", ()))
        self.mock = session.world == "mock"
        self.seed, self.fault_rate = session.seed, session.fault_rate
        self.page_lines = PAGE_LINES["fake" if session.context_window <= CONTEXT_WINDOW_TOKENS else "real"]
        self._work_fn: Callable[[], dict[str, Any]] | None = None

    # --- the tools ----------------------------------------------------------------------

    async def list_files(self, prefix: str = "") -> dict[str, Any]:
        def work() -> dict[str, Any]:
            files = [
                f
                for f in ws.git(self.work, "ls-tree", "-r", "--name-only", "HEAD").decode().split()
                if f.startswith(prefix)
            ]
            return {"files": files[:LISTED_FILES], "total": len(files)}

        return await self._run("list_files", f"list_files:{prefix}", work)

    async def read_file(self, path: str, page: int = 0) -> dict[str, Any]:
        def work() -> dict[str, Any]:
            try:
                text = ws.git(self.work, "show", f"HEAD:{ws.safe_path(path)}").decode(errors="replace")
            except ws.WorkspaceError as e:
                return {"path": path, "error": f"can't read it ({e})"}
            lines = text.splitlines()
            pages = max(1, -(-len(lines) // self.page_lines))
            if not 0 <= page < pages:
                return {"path": path, "error": f"it has pages 0 to {pages - 1}"}
            start = page * self.page_lines
            return {
                "path": path, "page": page, "pages": pages, "first_line": start + 1,
                "text": "\n".join(lines[start : start + self.page_lines]),
            }  # fmt: skip

        return await self._run("read_file", f"read_file:{path}#{page}", work)

    async def search(self, text: str) -> dict[str, Any]:
        def work() -> dict[str, Any]:
            try:
                out = ws.git(self.work, "grep", "-n", "-F", "-e", text, "HEAD", "--").decode(errors="replace")
            except ws.WorkspaceError:
                out = ""  # git grep exits 1 when nothing matches
            matches = [m.removeprefix("HEAD:") for m in out.splitlines()]
            return {"text": text, "matches": matches[:SEARCH_MATCHES], "total": len(matches)}

        return await self._run("search", f"search:{text}", work)

    async def run_check(self, item: str = "", edits: list[dict[str, str]] | None = None) -> dict[str, Any]:
        edits = [{"path": e["path"], "old": e["old"], "new": e["new"]} for e in edits or []]
        digest = ws.edits_digest(edits)

        def work() -> dict[str, Any]:
            commit = ws.head(self.work)
            out: dict[str, Any] = {"item": item, "commit": commit, "edits_digest": digest,
                                   "paths": sorted({e["path"] for e in edits})}  # fmt: skip
            try:
                run = ws.run_check(self.work, commit, self.template, item or None, edits, self.protected)
            except ws.WorkspaceError as e:
                return {**out, "passed": False, "error": str(e)}
            return {**out, "passed": run.passed, "exit_code": run.exit_code, "output": run.output}

        return await self._run("run_check", f"run_check:{item}:{digest}", work)

    # --- calls --------------------------------------------------------------------------

    async def _run(self, tool: str, path: str, work: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        self._work_fn = work  # calls within an attempt come one at a time
        return await self._call(tool, path, ())

    async def _request(self, path: str, call_no: int, required: tuple[str, ...]) -> dict[str, Any]:
        if self.mock:
            fault = pick_fault(roll(self.seed, self.task_key, self.attempt, call_no), self.fault_rate)
            if fault is not None:
                raise ToolFailure(fault)
        assert self._work_fn is not None
        try:
            return await asyncio.to_thread(self._work_fn)
        except ws.WorkspaceError as e:
            raise ToolFailure("workspace_error", str(e)) from None
