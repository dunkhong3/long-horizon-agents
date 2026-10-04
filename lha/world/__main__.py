"""Run the mock network: python -m lha.world --seed 42 --port 8765"""

import argparse

import uvicorn

from lha.config import DEFAULT_FAULT_RATE, DEFAULT_HOSTS
from lha.world.app import create_app
from lha.world.model import generate_world


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--hosts", type=int, default=DEFAULT_HOSTS)
    parser.add_argument("--fault-rate", type=float, default=DEFAULT_FAULT_RATE)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    world = generate_world(args.seed, args.hosts)
    app = create_app(world, args.fault_rate)
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning", access_log=False)


if __name__ == "__main__":
    main()
