"""The research brief's one tool, which reads a source from the mock library."""

from typing import Any

from lha.core.tools import ToolBox


class ResearchTools(ToolBox):
    async def get_source(self, source: str) -> dict[str, Any]:
        return await self._call("get_source", f"/sources/{source}", ("source", "text"))
