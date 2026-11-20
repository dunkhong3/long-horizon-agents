"""The mock web as an HTTP service, on the shared mock app with its seeded faults."""

from fastapi import FastAPI, HTTPException

from lha.core.world import mock_app
from lha.domains.research.world import MockWeb


def create_app(web: MockWeb, fault_rate: float) -> FastAPI:
    app = mock_app("mock web", web.seed, fault_rate)

    @app.get("/search")
    async def search(q: str) -> dict:
        # Search finds only shallow pages, so deeper ones are found through links.
        return {"query": q, "results": web.search(q)}

    @app.get("/page")
    async def page(url: str, page: int = 0) -> dict:
        found = web.page_text(url, page)
        if found is None or not 0 <= page < len(found[1]):
            raise HTTPException(404, f"no such page: {url} ({page})")
        doc, pages = found
        return {"url": url, "title": doc.title, "page": page, "pages": len(pages), "text": pages[page]}

    return app
