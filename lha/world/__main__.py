"""Run the mock network with python -m lha.world --seed 42 --port 8765"""

import argparse
import os
import threading
import time

import uvicorn

from lha.config import DEFAULT_FAULT_RATE, DEFAULT_HOSTS
from lha.world.app import create_app
from lha.world.model import generate_world


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--hosts", type=int, default=DEFAULT_HOSTS)
    parser.add_argument("--drifts", type=int, default=1)
    parser.add_argument("--fault-rate", type=float, default=DEFAULT_FAULT_RATE)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    _exit_when_orphaned()
    world = generate_world(args.seed, args.hosts, args.drifts)
    app = create_app(world, args.fault_rate)
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
