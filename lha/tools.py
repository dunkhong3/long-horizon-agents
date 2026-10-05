"""HTTP tools the agents use to read the mock network.

Every call is logged as a `tool_call` event (a step), success or not, with
the raw response, so facts can cite the exact event they came from.

Failures are never silent, because each response is checked, and an empty
200 or a malformed body counts as a failure and not a success. A short-lived
failure is retried a couple of times inside the attempt (each try is a new
call number, so a new fault roll), and if it keeps failing the tool raises,
the whole attempt fails, and the coordinator's retry rules take over.
"""

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

import httpx

from lha.config import TOOL_RETRIES, TOOL_RETRY_DELAY_SECONDS, TOOL_TIMEOUT_SECONDS

# log(kind, payload) -> event id, supplied by the worker.
EventLogger = Callable[[str, dict[str, Any]], Awaitable[UUID]]
# load(event id) -> the payload of a successful tool call from an earlier
# attempt of the same task, or None, also supplied by the worker.
PointerLoader = Callable[[str], Awaitable[dict[str, Any] | None]]


class ToolFailure(Exception):
    """A tool call failed. `kind` says how (timeout, server_error, ...)."""

    permanent = False

    def __init__(self, kind: str, message: str = ""):
        super().__init__(f"{kind}: {message}" if message else kind)
        self.kind = kind
        self.message = message


class NotFound(ToolFailure):
    """404, meaning the host, service or document doesn't exist. That is a valid
    answer and not a fault, and retrying won't change it."""

    permanent = True

    def __init__(self, message: str = ""):
        super().__init__("not_found", message)


class ToolBox:
    """The tools of one task attempt, which numbers its calls 0, 1, 2 and so on."""

    def __init__(
        self,
        http: httpx.AsyncClient,
        task_key: str,
        attempt: int,
        log: EventLogger,
        load: PointerLoader | None = None,
    ):
        self.http = http
        self.task_key = task_key
        self.attempt = attempt
        self.log = log
        self.load = load
        self.call_no = 0

    async def get_host(self, host: str) -> dict[str, Any]:
        return await self._call("get_host", f"/hosts/{host}", ("host", "services", "documents"))

    async def get_service(self, host: str, service: str) -> dict[str, Any]:
        path = f"/hosts/{host}/services/{service}"
        return await self._call("get_service", path, ("host", "service", "replicas"))

    async def fetch_document(self, host: str, name: str, page: int = 0) -> dict[str, Any]:
        path = f"/hosts/{host}/documents/{name}?page={page}"
        return await self._call("fetch_document", path, ("host", "name", "page", "pages", "content"))

    async def fetch_pointer(self, event_id: str) -> dict[str, Any]:
        """Fetch an earlier attempt's raw output by its pointer, instead of calling the network.

        It reads Postgres and not the network, so it has no faults, but it is
        logged like any tool call, with the copied response, so a fact can
        cite it. The coordinator checks every such copy against its original.
        """
        original = await self.load(event_id) if self.load else None
        if original is None:
            raise ToolFailure("bad_pointer", event_id)
        record = {"tool": "fetch_pointer", "path": original["path"], "call_no": self.call_no,
                  "pointer": event_id, "of": original["tool"]}  # fmt: skip
        self.call_no += 1
        new_id = await self.log("tool_call", {**record, "ok": True, "response": original["response"]})
        return {**original["response"], "event_id": str(new_id), "_of": original["tool"]}

    async def _call(self, tool: str, path: str, required: tuple[str, ...]) -> dict[str, Any]:
        last_failure: ToolFailure | None = None
        for _ in range(1 + TOOL_RETRIES):
            call_no = self.call_no
            self.call_no += 1
            record = {"tool": tool, "path": path, "call_no": call_no}
            try:
                data = await self._request(path, call_no, required)
            except ToolFailure as failure:
                await self.log("tool_call", {**record, "ok": False, "error": failure.kind})
                if failure.permanent:
                    raise
                last_failure = failure
                await asyncio.sleep(TOOL_RETRY_DELAY_SECONDS)
                continue
            event_id = await self.log("tool_call", {**record, "ok": True, "response": data})
            # The model gets the event id along with the data, so the facts
            # it reports can cite their source.
            return {**data, "event_id": str(event_id)}
        assert last_failure is not None
        raise last_failure

    async def _request(self, path: str, call_no: int, required: tuple[str, ...]) -> dict[str, Any]:
        headers = {
            "X-Task-Key": self.task_key,
            "X-Attempt": str(self.attempt),
            "X-Call-No": str(call_no),
        }
        try:
            resp = await self.http.get(path, headers=headers, timeout=TOOL_TIMEOUT_SECONDS)
        except httpx.TimeoutException:
            raise ToolFailure("timeout") from None
        except httpx.HTTPError as e:
            raise ToolFailure("connection_error", str(e)) from None

        if resp.status_code == 404:
            raise NotFound(path)
        if resp.status_code == 429:
            raise ToolFailure("rate_limited")
        if resp.status_code >= 500:
            raise ToolFailure("server_error", str(resp.status_code))
        if resp.status_code != 200:
            raise ToolFailure(f"http_{resp.status_code}")
        if not resp.content:
            raise ToolFailure("empty_response")  # a 200 with nothing in it
        try:
            data = resp.json()
        except json.JSONDecodeError:
            raise ToolFailure("malformed_response") from None
        if not isinstance(data, dict) or any(k not in data for k in required):
            raise ToolFailure("malformed_response", f"missing one of {required}")
        return data
