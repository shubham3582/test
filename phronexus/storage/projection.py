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

from phronexus import codec
from phronexus.contracts.models import Projection, StorageContract
from phronexus.errors import ValidationError

DOC_BIN = "doc"
META_DOC_ID = "_doc_id"
META_TXN = "_txn"
META_CVER = "_cver"
META_PJN = "_pjn"

# Reserved envelope bins — never treated as document data when reconstructing a
# ``bins``-encoded record.
_RESERVED = {DOC_BIN, META_DOC_ID, META_TXN, META_CVER, META_PJN}


def encode_payload(projection: Projection, data: dict[str, Any]) -> dict[str, Any]:
    """Physical bins for a projection's projected ``data``, per its encoding."""
    if projection.encoding == "msgpack":
        return {DOC_BIN: codec.pack(data)}
    if projection.encoding == "bins":
        spread_fields = {s.field for s in projection.spread}
        # Scalar/nested elements -> their own bin (optionally renamed). Spread
        # fields are exploded below instead of stored as a single map bin.
        out = {projection.bin_for(f): v for f, v in data.items() if f not in spread_fields}
        for s in projection.spread:
            m = data.get(s.field)
            if m is None:
                continue
            if not isinstance(m, dict):
                raise ValidationError(f"spread field {s.field!r} must be a map, got {type(m).__name__}")
            for k, v in m.items():
                bn = f"{s.prefix}{k}"
                if len(bn) > 15:
                    raise ValidationError(
                        f"spread bin {bn!r} exceeds 15 chars (field {s.field!r}, key {k!r})")
                out[bn] = v
        return out
    return {DOC_BIN: data}  # map (default)


def decode_record(projection: Projection, bins: dict[str, Any]) -> dict[str, Any]:
    """Reconstruct a document dict from a record's bins, per the projection's
    encoding (inverse of :func:`encode_payload`)."""
    if projection.encoding == "msgpack":
        raw = bins.get(DOC_BIN)
        return dict(codec.unpack(raw)) if raw is not None else {}
    if projection.encoding == "bins":
        rev = {b: f for f, b in projection.bin_map.items()}
        return {rev.get(k, k): v for k, v in bins.items() if k not in _RESERVED}
    return dict(bins.get(DOC_BIN) or {})  # map


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
            # Encoding controls only the physical layout (doc-map / msgpack blob /
            # per-element bins). ``data`` (the dict) is always carried separately
            # for index extraction, so encoding never affects indexing.
            bins = {
                **encode_payload(p, data),
                META_DOC_ID: doc_id,
                META_TXN: txn_id,
                META_CVER: contract.version,
                META_PJN: p.name,
            }
            records.append(ProjectionRecord(projection=p, key=key, bins=bins, ttl=p.ttl, data=data))
        return records
