"""The code tools, which read the session branch and run its check.

They work on the git workspace directly, inside the worker, as a
LocalToolBox (lha/core/tools.py), so every call is logged, retried and cited
like any other, and on the mock repository each call can fail on purpose.

Files are read from the branch head, the newest commit the coordinator has
made, and run_check always checks the head as well. A mistake in what the
model asked for, such as an edit whose text isn't in its file, comes back
as an answer that says what is wrong, so the model can try again within
the same attempt.
"""

import shlex
import sys
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Row

from lha.config import CONTEXT_WINDOW_TOKENS
from lha.core.tools import LocalToolBox, ToolFailure
from lha.domains.code import workspace as ws

PAGE_LINES = {"fake": 20, "real": 200}  # lines per page of a file, by the size of the window
SEARCH_MATCHES = 30
LISTED_FILES = 300


class CodeTools(LocalToolBox):
    def __init__(self, *args: Any, session: Row, **kwargs: Any):
        super().__init__(*args, session=session, **kwargs)
        self.work = Path(session.params["work"])
        self.template = session.params["check"].replace("{python}", shlex.quote(sys.executable))
        self.protected = tuple(session.params.get("protected", ()))
        self.page_lines = PAGE_LINES["fake" if session.context_window <= CONTEXT_WINDOW_TOKENS else "real"]

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

    async def _request(self, path: str, call_no: int, required: tuple[str, ...]) -> dict[str, Any]:
        try:
            return await super()._request(path, call_no, required)
        except ws.WorkspaceError as e:
            raise ToolFailure("workspace_error", str(e)) from None
