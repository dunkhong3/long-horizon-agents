"""The one tool of a domain loaded from configuration, which reads one page of one document.

A document comes from the folder given with --docs, which only the run's
home machine has, or from the configuration's own documents, which are
kept with the session in Postgres, so any machine can read those. It works
inside the worker, as a LocalToolBox (lha/core/tools.py), like batch
extraction's tool, so every call is logged, retried and cited like any
other, and with the configuration's own documents each call can fail on
purpose. A page that doesn't exist comes back as an answer that says which
pages there are.
"""

from pathlib import Path
from typing import Any

from sqlalchemy.engine import Row

from lha.config import CONTEXT_WINDOW_TOKENS
from lha.core.tools import LocalToolBox, Unreadable
from lha.domains.extract.documents import PAGE_CHARS, DocumentError, read_text
from lha.domains.research.web import html_to_text, paginate


class ConfigTools(LocalToolBox):
    def __init__(self, *args: Any, session: Row, **kwargs: Any):
        super().__init__(*args, session=session, **kwargs)
        folder = session.params.get("folder")
        self.folder = Path(folder) if folder else None
        self.texts = {d["name"]: d["text"] for d in session.params["spec"].get("document", [])}
        self.page_chars = PAGE_CHARS["fake" if session.context_window <= CONTEXT_WINDOW_TOKENS else "real"]

    async def read_document(self, doc: str, page: int = 0) -> dict[str, Any]:
        def work() -> dict[str, Any]:
            try:
                pages = paginate(self._text(doc), self.page_chars)
            except DocumentError as e:
                raise Unreadable(str(e)) from None
            if not 0 <= page < len(pages):
                return {"doc": doc, "error": f"it has pages 0 to {len(pages) - 1}"}
            return {"doc": doc, "page": page, "pages": len(pages), "text": pages[page]}

        return await self._run("read_document", f"read_document:{doc}#{page}", work)

    def _text(self, doc: str) -> str:
        if self.folder is not None:
            return read_text(self.folder, doc)
        if doc not in self.texts:
            raise DocumentError(f"{doc} is not one of the documents")
        text = self.texts[doc]
        if doc.lower().endswith((".html", ".htm")):
            text = html_to_text(text)[1].split("\nLinks on this page:\n")[0]
        return text
