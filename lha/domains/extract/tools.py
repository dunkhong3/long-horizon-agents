"""The extraction tool, which reads one page of one document.

It reads the folder directly, inside the worker, as a LocalToolBox
(lha/core/tools.py), so every call is logged, retried and cited like any
other, and with the mock documents each call can fail on purpose. A
document that can't be read as text fails for good, and a page that doesn't
exist comes back as an answer that says which pages there are, so the
model can ask again within the same attempt.
"""

from pathlib import Path
from typing import Any

from sqlalchemy.engine import Row

from lha.config import CONTEXT_WINDOW_TOKENS
from lha.core.tools import LocalToolBox, Unreadable
from lha.domains.extract.documents import PAGE_CHARS, DocumentError, pages_of


class ExtractTools(LocalToolBox):
    def __init__(self, *args: Any, session: Row, **kwargs: Any):
        super().__init__(*args, session=session, **kwargs)
        self.folder = Path(session.params["folder"])
        self.page_chars = PAGE_CHARS["fake" if session.context_window <= CONTEXT_WINDOW_TOKENS else "real"]

    async def read_document(self, doc: str, page: int = 0) -> dict[str, Any]:
        def work() -> dict[str, Any]:
            try:
                pages = pages_of(self.folder, doc, self.page_chars)
            except DocumentError as e:
                raise Unreadable(str(e)) from None
            if not 0 <= page < len(pages):
                return {"doc": doc, "error": f"it has pages 0 to {len(pages) - 1}"}
            return {"doc": doc, "page": page, "pages": len(pages), "text": pages[page]}

        return await self._run("read_document", f"read_document:{doc}#{page}", work)
