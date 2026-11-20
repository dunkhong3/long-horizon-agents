"""The research tools, which search for pages and read them, on the mock web or the real one.

On the mock web both tools are HTTP calls to the mock world, with its seeded
faults. On the real web a real model searches with its provider's own web
search (see agents.py), and fetch_page reads the real page with httpx and
turns it into text (see web.py), with each document read once per attempt
and served in pages.
"""

from typing import Any
from urllib.parse import quote

import httpx

from lha.core.tools import NotFound, ToolBox, ToolFailure
from lha.domains.research.tasks import page_path
from lha.domains.research.web import (
    FETCH_TIMEOUT_SECONDS,
    MAX_BYTES,
    TEXT_TYPES,
    USER_AGENT,
    html_to_text,
    paginate,
)

PAGE_FIELDS = ("url", "title", "page", "pages", "text")


class Unreadable(ToolFailure):
    """A page that is not text, such as a PDF or an image, which another try won't change."""

    permanent = True

    def __init__(self, message: str = ""):
        super().__init__("unreadable", message)


class ResearchTools(ToolBox):
    def __init__(self, *args: Any, world: str = "mock", **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.world = world
        self.documents: dict[str, tuple[str, list[str]]] = {}  # url -> (title, pages), real web only

    async def web_search(self, query: str) -> dict[str, Any]:
        return await self._call("web_search", f"/search?q={quote(query)}", ("results",))

    async def fetch_page(self, url: str, page: int = 0) -> dict[str, Any]:
        path = page_path(url, page) if self.world == "mock" else f"{url}#page={page}"
        return await self._call("fetch_page", path, PAGE_FIELDS)

    async def _request(self, path: str, call_no: int, required: tuple[str, ...]) -> dict[str, Any]:
        if self.world == "mock":
            return await super()._request(path, call_no, required)
        url, _, page = path.rpartition("#page=")
        if url not in self.documents:
            self.documents[url] = await self._download(url)
        title, pages = self.documents[url]
        n = int(page)
        if not 0 <= n < len(pages):
            raise NotFound(path)
        return {"url": url, "title": title, "page": n, "pages": len(pages), "text": pages[n]}

    async def _download(self, url: str) -> tuple[str, list[str]]:
        """A real page as its title and its text in pages, with failures sorted
        the same way as the mock web's (see lha/core/tools.py). The status and
        the type are checked before the body is read, and at most MAX_BYTES of
        it is read, so a large file is turned down without being downloaded."""
        headers = {"User-Agent": USER_AGENT}
        try:
            async with self.http.stream(
                "GET", url, follow_redirects=True, timeout=FETCH_TIMEOUT_SECONDS, headers=headers
            ) as resp:
                kind = _check(url, resp)
                raw = bytearray()
                async for chunk in resp.aiter_bytes():
                    raw += chunk
                    if len(raw) >= MAX_BYTES:
                        break
                body = bytes(raw[:MAX_BYTES]).decode(resp.encoding or "utf-8", errors="replace")
                final_url = str(resp.url)
        except httpx.TimeoutException:
            raise ToolFailure("timeout") from None
        except httpx.HTTPError as e:
            raise ToolFailure("connection_error", str(e)) from None
        title, text = html_to_text(body, final_url) if kind != "text/plain" else ("", body)
        if not text.strip():
            raise Unreadable("no text")
        return title, paginate(text)


def _check(url: str, resp: httpx.Response) -> str:
    """The response's content type, once its status and type say it can be read."""
    if resp.status_code in (404, 410):
        raise NotFound(f"{url}#page=0")
    if resp.status_code == 429:
        raise ToolFailure("rate_limited")
    if resp.status_code >= 500:
        raise ToolFailure("server_error", str(resp.status_code))
    if resp.status_code != 200:
        raise Unreadable(f"HTTP {resp.status_code}")
    kind = resp.headers.get("content-type", "").split(";")[0].strip().lower()
    if kind and kind not in TEXT_TYPES:
        raise Unreadable(f"{kind} is not text")
    return kind
