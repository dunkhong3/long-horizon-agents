"""long-horizon-agents, a set of agents that stay coherent over a long run."""

import os

# Pydantic AI prints a banner on first use, so we switch it off to keep worker output quiet.
os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")
