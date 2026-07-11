"""HTTP client for a remote Phronexus service.

Mirrors the in-process facade (:class:`phronexus.core.Phronexus`) so switching
between embedded and remote is a one-line change. Supports API-key/bearer auth
and TLS/mTLS via the underlying ``httpx`` client. ``httpx`` is an optional
dependency (``pip install 'phronexus-core[client]'``).
"""

from __future__ import annotations

from typing import Any, Optional

from phronexus.errors import (
    DocumentNotFound,
    PhronexusError,
    QueryError,
    ValidationError,
)

try:
    import httpx
except ImportError:  # pragma: no cover
    httpx = None


class PhronexusClient:
    def __init__(
        self,
        base_url: str,
        *,
        api_key: Optional[str] = None,
        bearer_token: Optional[str] = None,
        api_key_header: str = "X-API-Key",
        verify: str | bool = True,          # CA bundle path or bool
        cert: Optional[tuple[str, str]] = None,  # (client_cert, client_key) for mTLS
        timeout: float = 10.0,
    ):
        if httpx is None:  # pragma: no cover
            raise RuntimeError("PhronexusClient requires httpx: pip install 'phronexus-core[client]'")
        headers: dict[str, str] = {}
        if api_key:
            headers[api_key_header] = api_key
        if bearer_token:
            headers["Authorization"] = f"Bearer {bearer_token}"
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"), headers=headers,
            verify=verify, cert=cert, timeout=timeout,
        )

    # --- data plane -----------------------------------------------------

    def put(self, entity: str, document: dict[str, Any]) -> str:
        r = self._request("PUT", f"/entities/{entity}/documents", json={"document": document})
        return r["doc_id"]

    def put_many(self, entity: str, documents: list[dict[str, Any]]) -> list[str]:
        """Bulk-write documents in one request (each independently committed)."""
        r = self._request("POST", f"/entities/{entity}/documents/batch",
                           json={"documents": documents})
        return r["doc_ids"]

    def get(self, entity: str, doc_id: str) -> Optional[dict[str, Any]]:
        try:
            return self._request("GET", f"/entities/{entity}/documents/{doc_id}")
        except DocumentNotFound:
            return None

    def delete(self, entity: str, doc_id: str) -> bool:
        self._request("DELETE", f"/entities/{entity}/documents/{doc_id}")
        return True

    def view(self, entity: str, view: str, doc_id: str) -> Optional[dict[str, Any]]:
        try:
            return self._request("GET", f"/entities/{entity}/views/{view}/documents/{doc_id}")
        except DocumentNotFound:
            return None

    def query(
        self, entity: str, where: list[dict], *, limit: int = 100, view: Optional[str] = None
    ) -> list[dict[str, Any]]:
        params = {"view": view} if view else None
        r = self._request(
            "POST", f"/entities/{entity}/query",
            json={"where": where, "limit": limit}, params=params,
        )
        return r["documents"]

    def query_pattern(self, entity: str, pattern: str, **params: Any) -> list[dict[str, Any]]:
        r = self._request(
            "POST", f"/entities/{entity}/patterns/{pattern}", json={"params": params}
        )
        return r["documents"]

    # --- admin ----------------------------------------------------------

    def publish_contract(self, contract: dict, *, activate: bool = True) -> dict:
        return self._request("POST", "/contracts", json=contract, params={"activate": activate})

    # --- internals ------------------------------------------------------

    def _request(self, method: str, path: str, **kwargs) -> Any:
        resp = self._http.request(method, path, **kwargs)
        if resp.status_code >= 400:
            self._raise(resp)
        return resp.json()

    @staticmethod
    def _raise(resp) -> None:
        try:
            body = resp.json()
            code, detail = body.get("error", "error"), body.get("detail", resp.text)
        except Exception:  # noqa: BLE001
            code, detail = "http_error", resp.text
        mapping = {
            "document_not_found": DocumentNotFound,
            "query_error": QueryError,
            "validation_error": ValidationError,
        }
        raise mapping.get(code, PhronexusError)(f"{code}: {detail}")

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "PhronexusClient":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
