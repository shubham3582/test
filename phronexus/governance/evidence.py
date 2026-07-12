"""Audit evidence export.

Assembles a windowed, tamper-evident evidence bundle from the per-document audit
trail (``_audit``), the request/response interactions journal (``_interactions``),
and the immutable governance log. The bundle carries a content hash and the
governance-log head hash, so a reviewer can prove the evidence was neither
altered nor detached from the log it references. The export is itself recorded in
the governance log.
"""

from __future__ import annotations

import time
from typing import Optional

from phronexus.governance.promote import content_hash


def _in_window(ts: Optional[float], start: Optional[float], end: Optional[float]) -> bool:
    if ts is None:
        return True  # undated rows can't be windowed — always keep
    if start is not None and ts < start:
        return False
    if end is not None and ts > end:
        return False
    return True


def build_evidence(px, actor: str, *, entity: Optional[str] = None,
                   doc_id: Optional[str] = None, start_ts: Optional[float] = None,
                   end_ts: Optional[float] = None,
                   sources: Optional[list[str]] = None) -> dict:
    sources = sources or ["audit", "interactions", "gov_log"]
    s = px.settings
    records: dict[str, list] = {}

    if "audit" in sources:
        rows = []
        for _key, rec in px.store.scan(s.audit.audit_set):
            b = rec.bins
            if entity and b.get("entity") != entity:
                continue
            if doc_id and b.get("doc_id") != doc_id:
                continue
            if not _in_window(b.get("ts"), start_ts, end_ts):
                continue
            rows.append(dict(b))
        rows.sort(key=lambda r: r.get("ts", 0))
        records["audit"] = rows

    if "interactions" in sources:
        rows = []
        for _key, rec in px.store.scan(s.journal.interactions_set):
            b = rec.bins
            if entity and b.get("entity") != entity:
                continue
            if not _in_window(b.get("ts"), start_ts, end_ts):
                continue
            # keep the metadata; the msgpack req/resp blobs are opaque bytes
            rows.append({k: v for k, v in b.items() if k not in ("req", "resp")})
        rows.sort(key=lambda r: r.get("ts", 0))
        records["interactions"] = rows

    if "gov_log" in sources:
        entries = px.governance.log.entries(target=entity) if entity \
            else px.governance.log.entries()
        records["gov_log"] = [e for e in entries if _in_window(e.get("ts"), start_ts, end_ts)]

    manifest = {
        "generated_ts": time.time(), "generated_by": actor,
        "filters": {"entity": entity, "doc_id": doc_id,
                    "start_ts": start_ts, "end_ts": end_ts, "sources": sources},
        "counts": {k: len(v) for k, v in records.items()},
        "gov_log_head": px.governance.log.head(),
    }
    manifest["content_hash"] = content_hash(records)
    bundle = {"manifest": manifest, "records": records}
    # The export is itself a governance event.
    px.governance.log.append("evidence.exported", actor, target=entity,
                             detail={"counts": manifest["counts"],
                                     "content_hash": manifest["content_hash"]})
    return bundle
