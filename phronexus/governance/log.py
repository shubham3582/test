"""Immutable, hash-chained governance history (``_gov_log``).

Every control-plane action (draft, submit, approve, publish, rollback, promote,
COB change, backfill control, evidence export) is appended here as one immutable
record linked to the previous by a SHA-256 hash chain, so any later tampering —
editing, reordering, or deleting an entry — is detectable by :meth:`verify`.

Append is atomic: the new entry (insert-only) and the head pointer (generation
CAS) commit in one transaction; concurrent appends retry. This reuses the same
insert-only + optimistic-concurrency primitives as the retention log and the
contract registry.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Optional

from phronexus.errors import GenerationConflict
from phronexus.kv.base import KVStore

_HEAD = "__head__"
_CHAINED = ("seq", "ts", "actor", "action", "target", "detail", "prev_hash")


def _canonical(body: dict[str, Any]) -> str:
    return json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)


def _hash(body: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(body).encode()).hexdigest()


class GovernanceLog:
    def __init__(self, store: KVStore, set_name: str = "_gov_log", retries: int = 8):
        self._store = store
        self._set = set_name
        self._retries = retries

    def _head(self) -> tuple[int, str, int]:
        rec = self._store.get(self._set, _HEAD)
        if rec is None:
            return 0, "", 0
        return rec.bins["seq"], rec.bins["hash"], rec.generation

    def append(self, action: str, actor: str, *, target: Optional[str] = None,
               detail: Optional[dict] = None, ts: Optional[float] = None) -> dict:
        """Append one immutable, chained entry. Returns the stored record."""
        ts = time.time() if ts is None else ts
        for attempt in range(self._retries + 1):
            seq0, prev_hash, head_gen = self._head()
            body = {
                "seq": seq0 + 1, "ts": ts, "actor": actor, "action": action,
                "target": target, "detail": detail or {}, "prev_hash": prev_hash,
            }
            record = {**body, "hash": _hash(body)}
            try:
                with self._store.transaction() as txn:
                    # insert-only: an entry key is never overwritten
                    self._store.put(self._set, f"entry:{body['seq']:012d}", record,
                                    expected_generation=0, txn=txn)
                    self._store.put(self._set, _HEAD,
                                    {"seq": body["seq"], "hash": record["hash"]},
                                    expected_generation=head_gen, txn=txn)
                return record
            except GenerationConflict:
                if attempt >= self._retries:
                    raise
        raise RuntimeError("unreachable")

    def head(self) -> dict:
        seq, h, _gen = self._head()
        return {"seq": seq, "hash": h}

    def entries(self, *, action: Optional[str] = None,
                target: Optional[str] = None) -> list[dict]:
        out: list[dict] = []
        for key, rec in self._store.scan(self._set):
            if key == _HEAD:
                continue
            e = dict(rec.bins)
            if action is not None and e.get("action") != action:
                continue
            if target is not None and e.get("target") != target:
                continue
            out.append(e)
        out.sort(key=lambda e: e["seq"])
        return out

    def verify(self) -> dict:
        """Re-walk the chain. Returns ``{ok, count, bad_seq?}``; ``ok=False`` with
        the first offending seq if any entry was tampered with or the chain broke."""
        prev = ""
        count = 0
        for e in self.entries():
            count += 1
            body = {k: e.get(k) for k in _CHAINED}
            if e.get("prev_hash") != prev or e.get("hash") != _hash(body):
                return {"ok": False, "count": count, "bad_seq": e.get("seq")}
            prev = e["hash"]
        return {"ok": True, "count": count, "bad_seq": None}
