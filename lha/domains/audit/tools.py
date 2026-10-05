"""The audit's tools, which read hosts, services and documents from the mock network."""

from typing import Any

from lha.core.tools import ToolBox


class AuditTools(ToolBox):
    async def get_host(self, host: str) -> dict[str, Any]:
        return await self._call("get_host", f"/hosts/{host}", ("host", "services", "documents"))

    async def get_service(self, host: str, service: str) -> dict[str, Any]:
        path = f"/hosts/{host}/services/{service}"
        return await self._call("get_service", path, ("host", "service", "replicas"))

    async def fetch_document(self, host: str, name: str, page: int = 0) -> dict[str, Any]:
        path = f"/hosts/{host}/documents/{name}?page={page}"
        return await self._call("fetch_document", path, ("host", "name", "page", "pages", "content"))
