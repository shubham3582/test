"""Build physical projection records from a logical document + storage contract.

Each projection record stores the projected data under a ``doc`` bin plus a
small envelope of reserved bins:

    _doc_id : document id (derived from primary-key fields)
    _txn    : id of the write that produced this record (see the manifest)
    _cver   : storage-contract version used
    _pjn    : projection name

Keeping data in one ``doc`` bin avoids collisions with the reserved envelope and
makes both canonical reconstruction and index extraction trivial.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from phronexus.contracts.models import Projection, StorageContract
from phronexus.errors import ValidationError

DOC_BIN = "doc"
META_DOC_ID = "_doc_id"
META_TXN = "_txn"
META_CVER = "_cver"
META_PJN = "_pjn"


def _resolve(doc: dict[str, Any], token: str) -> Any:
    """Resolve a possibly-dotted token (``a.b.c``) against a document."""
    cur: Any = doc
    for part in token.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise ValidationError(f"document is missing field {token!r}")
        cur = cur[part]
    return cur


@dataclass
class ProjectionRecord:
    projection: Projection
    key: str
    bins: dict[str, Any]
    ttl: int
    data: dict[str, Any]  # the projected field subset (for index extraction)


class ProjectionEngine:
    def compute_doc_id(self, contract: StorageContract, doc: dict[str, Any]) -> str:
        parts = [str(_resolve(doc, f)) for f in contract.primary_key]
        return "|".join(parts)

    def render_key(self, projection: Projection, doc: dict[str, Any]) -> str:
        key = projection.key
        for tok in projection.key_tokens():
            key = key.replace("{" + tok + "}", str(_resolve(doc, tok)))
        return key

    def _select(self, projection: Projection, doc: dict[str, Any]) -> dict[str, Any]:
        if projection.is_full:
            return dict(doc)
        out: dict[str, Any] = {}
        for f in projection.fields:
            # Missing fields are simply omitted from partial projections.
            if f in doc:
                out[f] = doc[f]
        return out

    def build(
        self,
        contract: StorageContract,
        doc: dict[str, Any],
        *,
        doc_id: str,
        txn_id: str,
    ) -> list[ProjectionRecord]:
        records: list[ProjectionRecord] = []
        for p in contract.projections:
            data = self._select(p, doc)
            key = self.render_key(p, doc)
            bins = {
                DOC_BIN: data,
                META_DOC_ID: doc_id,
                META_TXN: txn_id,
                META_CVER: contract.version,
                META_PJN: p.name,
            }
            records.append(ProjectionRecord(projection=p, key=key, bins=bins, ttl=p.ttl, data=data))
        return records
