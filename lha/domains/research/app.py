"""The research library as a mock HTTP service, on the shared mock app with its seeded faults."""

from fastapi import FastAPI, HTTPException

from lha.core.world import mock_app
from lha.domains.research.world import Library


def create_app(library: Library, fault_rate: float) -> FastAPI:
    app = mock_app("mock library", library.seed, fault_rate)

    @app.get("/sources/{source}")
    async def get_source(source: str) -> dict:
        # There is no 'list all sources', so sources are only found through citations.
        text = library.texts.get(source)
        if text is None:
            raise HTTPException(404, f"no such source: {source}")
        return {"source": source, "text": text}

    return app
