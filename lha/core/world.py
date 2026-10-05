"""A domain's mock world, as an HTTP service on its own (FastAPI).

    python -m lha.core.world --domain audit --seed 42 --size 20 --goal one --port 8765

The world plays 'the outside world', so it keeps no state and writes nothing
to Postgres. Every request carries three headers from the calling tool.

    X-Task-Key, X-Attempt, X-Call-No

The fault middleware rolls a seeded die on those headers to decide whether
a call fails, so faults are reproducible per seed, and it is the same for
every domain. A domain only adds its endpoints, and optionally a rule for
requests that are down on purpose (such as the audit's outage host).
"""

import argparse
import asyncio
import os
import threading
import time
from collections.abc import Callable

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from lha.config import DEFAULT_FAULT_RATE, TIMEOUT_FAULT_SECONDS
from lha.faults import pick_fault, roll


def mock_app(
    title: str, seed: int, fault_rate: float, down: Callable[[str, str], bool] | None = None
) -> FastAPI:
    """A FastAPI app with the seeded fault middleware and a /health endpoint.

    `down(path, task_key)` returns True for a request that should get a 503,
    which comes before any rolled fault.
    """
    app = FastAPI(title=title)

    @app.middleware("http")
    async def inject_faults(request: Request, call_next):
        if request.url.path == "/health":
            return await call_next(request)
        task_key = request.headers.get("x-task-key", "")
        if down is not None and down(request.url.path, task_key):
            return JSONResponse({"detail": "down"}, status_code=503)
        attempt = request.headers.get("x-attempt", "0")
        call_no = request.headers.get("x-call-no", "0")
        fault = pick_fault(roll(seed, task_key, attempt, call_no), fault_rate)
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

    return app


def main() -> None:
    from lha.core.domain import get_domain

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--domain", required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--size", type=int, required=True)
    parser.add_argument("--goal", default="one")
    parser.add_argument("--fault-rate", type=float, default=DEFAULT_FAULT_RATE)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    _exit_when_orphaned()
    domain = get_domain(args.domain)
    world = domain.make_world(args.seed, args.size, args.goal)
    app = domain.create_app(world, args.fault_rate)
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning", access_log=False)


def _exit_when_orphaned() -> None:
    """Stop if the supervisor dies without stopping us (e.g. it was SIGKILLed)."""
    parent = os.getppid()

    def watch() -> None:
        while os.getppid() == parent:
            time.sleep(1)
        os._exit(0)

    threading.Thread(target=watch, daemon=True).start()


if __name__ == "__main__":
    main()
