"""The audit's mock network, which is a few endpoints on the shared mock app.

There is the seeded fault middleware of lha/core/world.py, plus the outage
host, which answers 503 to every task of the first round, meaning every task
key without a '#n' round suffix.
"""

from fastapi import FastAPI, HTTPException, Request

from lha.core.world import mock_app
from lha.domains.audit.world import World, doc_page


def create_app(world: World, fault_rate: float) -> FastAPI:
    def down(path: str, task_key: str) -> bool:
        parts = path.split("/")
        return len(parts) > 2 and parts[2] == world.outage_host and "#" not in task_key

    app = mock_app("mock network", world.seed, fault_rate, down)

    @app.get("/hosts/{host}")
    async def get_host(host: str) -> dict:
        # There is deliberately no 'list all hosts', so hidden hosts can only
        # be found through documents.
        if host not in world.hosts:
            raise HTTPException(404, f"no such host: {host}")
        return {
            "host": host,
            "services": world.hosts[host],
            "documents": sorted(world.documents[host]),
        }

    @app.get("/hosts/{host}/services/{service}")
    async def get_service(host: str, service: str, request: Request) -> dict:
        s = world.services.get(service)
        if s is None or s.host != host:
            raise HTTPException(404, f"no service {service} on {host}")
        replicas = s.actual
        # Decoys, where discovery reads see a stale count and verify reads see
        # the truth. Stale reads are never applied to the drifted service.
        if service in world.decoys and request.headers.get("x-task-key", "").startswith("discover_host:"):
            replicas = world.decoys[service]
        return {"host": host, "service": service, "replicas": replicas}

    @app.get("/hosts/{host}/documents/{name}")
    async def fetch_document(host: str, name: str, page: int = 0) -> dict:
        # Documents come in pages, because a big one (registry.json in a big
        # world) would not fit in any context window in one piece.
        text = world.documents.get(host, {}).get(name)
        found = doc_page(text, page) if text is not None else None
        if found is None:
            raise HTTPException(404, f"no page {page} of document {name} on {host}")
        content, pages = found
        return {"host": host, "name": name, "page": page, "pages": pages, "content": content}

    return app
