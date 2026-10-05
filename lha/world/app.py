"""The mock network as an HTTP service (FastAPI).

It plays 'the outside world', so it keeps no state and writes nothing to
Postgres. Every request carries three headers from the calling tool.

    X-Task-Key, X-Attempt, X-Call-No

The fault middleware rolls a seeded die on those headers to decide whether
this call fails, so faults are reproducible per seed. It also plays the
outage host, which answers 503 to every task of the first round, meaning
every task key without a '#n' round suffix.
"""

import asyncio

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response

from lha.config import TIMEOUT_FAULT_SECONDS
from lha.faults import pick_fault, roll
from lha.world.model import World, doc_page


def create_app(world: World, fault_rate: float) -> FastAPI:
    app = FastAPI(title="mock network")

    @app.middleware("http")
    async def inject_faults(request: Request, call_next):
        if request.url.path == "/health":
            return await call_next(request)
        task_key = request.headers.get("x-task-key", "")
        parts = request.url.path.split("/")
        if len(parts) > 2 and parts[2] == world.outage_host and "#" not in task_key:
            return JSONResponse({"detail": "host is down"}, status_code=503)
        attempt = request.headers.get("x-attempt", "0")
        call_no = request.headers.get("x-call-no", "0")
        fault = pick_fault(roll(world.seed, task_key, attempt, call_no), fault_rate)
        if fault == "server_error":
            return JSONResponse({"detail": "internal error"}, status_code=500)
        if fault == "rate_limited":
            return JSONResponse({"detail": "slow down"}, status_code=429)
        if fault == "empty_response":
            return Response(content=b"", status_code=200)  # 'silent success'
        if fault == "timeout":
            await asyncio.sleep(TIMEOUT_FAULT_SECONDS)  # the client gives up first
        return await call_next(request)

    @app.get("/health")
    async def health() -> dict:
        return {"ok": True}

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
