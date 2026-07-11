"""A self-maintained inverted index built on the KV store.

Chosen over Aerospike secondary indexes: it is portable across backends, has no
cluster-side index-build step, and fits the manifest model — the index is a
*derived hint*, never the source of truth. A stale or missing posting only means
a candidate is validated away (or briefly not found by search); the document is
always addressable by primary key, and the index can be rebuilt from manifests.

Layout (all in the ``_inv`` set) — posting lists are **segmented** so appending
to a hot term stays O(segment), not O(list):
    head     "{entity}\\x1f{field}\\x1f{value}"       -> {docs: [...open seg...], segs: N}
    segment  "{entity}\\x1f{field}\\x1f{value}\\x1f#j" -> {docs: [...]}   (sealed, immutable)
    terms    "{entity}\\x1f{field}\\x1f__terms__"     -> {terms: [value, ...]}   (numeric only)

Appends go to the open head segment; when it fills (``segment_size``) it is sealed
as ``#j`` and a fresh head starts — so bulk-ingesting N docs that share an indexed
value is O(N), not O(N^2). Equality / ``in`` union the head + sealed segments;
numeric ranges read the term dictionary, select values in range, and union their
posting lists. Updates use generation CAS with bounded retries so concurrent
writers don't lose postings.
"""

from __future__ import annotations

from typing import Any, Iterable

import structlog

from phronexus.errors import GenerationConflict
from phronexus.kv.base import KVStore

log = structlog.get_logger(__name__)

_SEP = "\x1f"
_MAX_RETRIES = 5


def _posting_key(entity: str, field: str, value: Any) -> str:
    return f"{entity}{_SEP}{field}{_SEP}{value}"


def _segment_key(head_key: str, j: int) -> str:
    return f"{head_key}{_SEP}#{j}"


def _terms_key(entity: str, field: str) -> str:
    return f"{entity}{_SEP}{field}{_SEP}__terms__"


class InvertedIndex:
    def __init__(self, store: KVStore, index_set: str = "_inv", segment_size: int = 512) -> None:
        self._store = store
        self._set = index_set
        self._seg = max(1, segment_size)

    # --- maintenance ----------------------------------------------------

    def add(self, entity: str, field: str, value: Any, doc_id: str, *, numeric: bool = False) -> None:
        key = _posting_key(entity, field, value)
        for _ in range(_MAX_RETRIES):
            head = self._store.get(self._set, key)
            docs = list(head.bins.get("docs", [])) if head else []
            segs = head.bins.get("segs", 0) if head else 0
            new_value = head is None  # first doc ever for this (field, value)
            if doc_id in docs:
                break  # already in the open segment
            try:
                if len(docs) >= self._seg:
                    # Seal the full head as an immutable segment; start a fresh head.
                    self._store.put(self._set, _segment_key(key, segs), {"docs": docs},
                                    expected_generation=0)
                    docs, segs = [], segs + 1
                docs.append(doc_id)
                self._store.put(self._set, key, {"docs": docs, "segs": segs},
                                expected_generation=head.generation if head else 0)
                break
            except GenerationConflict:
                new_value = False
                continue  # someone else wrote the head; re-read and retry
        else:
            log.warning("index.posting.cas_exhausted", entity=entity, field=field)
        if numeric and new_value:
            self._add_term(entity, field, value)

    def remove(self, entity: str, field: str, value: Any, doc_id: str) -> None:
        key = _posting_key(entity, field, value)
        for _ in range(_MAX_RETRIES):
            head = self._store.get(self._set, key)
            if head is None:
                return
            docs = list(head.bins.get("docs", []))
            segs = head.bins.get("segs", 0)
            if doc_id in docs:
                docs = [d for d in docs if d != doc_id]
                try:
                    if not docs and segs == 0:
                        self._store.remove(self._set, key, expected_generation=head.generation)
                    else:
                        self._store.put(self._set, key, {"docs": docs, "segs": segs},
                                        expected_generation=head.generation)
                    return
                except GenerationConflict:
                    continue
            # Not in the open head — find it in a sealed segment.
            if self._remove_from_sealed(key, segs, doc_id, txn=None):
                return
            return  # not present anywhere

    def _add_term(self, entity, field, value) -> None:
        key = _terms_key(entity, field)
        for _ in range(_MAX_RETRIES):
            head = self._store.get(self._set, key)
            terms = list(head.bins.get("terms", [])) if head else []
            segs = head.bins.get("segs", 0) if head else 0
            if value in terms:
                return  # in the open segment already
            try:
                if len(terms) >= self._seg:
                    self._store.put(self._set, _segment_key(key, segs), {"terms": terms},
                                    expected_generation=0)
                    terms, segs = [], segs + 1
                terms.append(value)
                self._store.put(self._set, key, {"terms": terms, "segs": segs},
                                expected_generation=head.generation if head else 0)
                return
            except GenerationConflict:
                continue

    # --- transactional maintenance --------------------------------------
    # These stage a single read-modify-write into a caller's transaction, so
    # index mutations commit atomically with the manifest (no CAS retry — the
    # transaction provides isolation; a conflict aborts the whole write).

    def stage_add(self, entity, field, value, doc_id, *, numeric: bool, txn) -> None:
        key = _posting_key(entity, field, value)
        head = self._store.get(self._set, key, txn=txn)
        docs = list(head.bins.get("docs", [])) if head else []
        segs = head.bins.get("segs", 0) if head else 0
        new_value = head is None  # first doc ever for this (field, value)
        if doc_id not in docs:
            if len(docs) >= self._seg:
                # Seal the full head as an immutable segment; start a fresh head.
                self._store.put(self._set, _segment_key(key, segs), {"docs": docs}, txn=txn)
                docs, segs = [], segs + 1
            docs.append(doc_id)
            self._store.put(self._set, key, {"docs": docs, "segs": segs}, txn=txn)
        if numeric and new_value:
            self._stage_add_term(entity, field, value, txn)

    def stage_remove(self, entity, field, value, doc_id, *, txn) -> None:
        key = _posting_key(entity, field, value)
        head = self._store.get(self._set, key, txn=txn)
        if head is None:
            return
        docs = list(head.bins.get("docs", []))
        segs = head.bins.get("segs", 0)
        if doc_id in docs:
            docs = [d for d in docs if d != doc_id]
            if not docs and segs == 0:
                self._store.remove(self._set, key, txn=txn)
            else:
                self._store.put(self._set, key, {"docs": docs, "segs": segs}, txn=txn)
            return
        # Not in the open head — find it in a sealed segment.
        self._remove_from_sealed(key, segs, doc_id, txn=txn)

    def _remove_from_sealed(self, key: str, segs: int, doc_id: str, *, txn) -> bool:
        for j in range(segs):
            sk = _segment_key(key, j)
            rec = self._store.get(self._set, sk, txn=txn)
            if rec is None:
                continue
            sdocs = rec.bins.get("docs", [])
            if doc_id in sdocs:
                self._store.put(self._set, sk, {"docs": [d for d in sdocs if d != doc_id]},
                                expected_generation=(None if txn is not None else rec.generation),
                                txn=txn)
                return True
        return False

    def _stage_add_term(self, entity, field, value, txn) -> None:
        key = _terms_key(entity, field)
        head = self._store.get(self._set, key, txn=txn)
        terms = list(head.bins.get("terms", [])) if head else []
        segs = head.bins.get("segs", 0) if head else 0
        if value in terms:
            return
        if len(terms) >= self._seg:
            self._store.put(self._set, _segment_key(key, segs), {"terms": terms}, txn=txn)
            terms, segs = [], segs + 1
        terms.append(value)
        self._store.put(self._set, key, {"terms": terms, "segs": segs}, txn=txn)

    # --- lookups --------------------------------------------------------

    def lookup_eq(self, entity: str, field: str, value: Any) -> set[str]:
        key = _posting_key(entity, field, value)
        head = self._store.get(self._set, key)
        if head is None:
            return set()
        out = set(head.bins.get("docs", []))
        for j in range(head.bins.get("segs", 0)):
            seg = self._store.get(self._set, _segment_key(key, j))
            if seg is not None:
                out |= set(seg.bins.get("docs", []))
        return out

    def lookup_in(self, entity: str, field: str, values: Iterable[Any]) -> set[str]:
        out: set[str] = set()
        for v in values:
            out |= self.lookup_eq(entity, field, v)
        return out

    def lookup_range(
        self,
        entity: str,
        field: str,
        *,
        lo: Any = None,
        hi: Any = None,
        incl_lo: bool = True,
        incl_hi: bool = True,
    ) -> set[str]:
        terms = self.all_terms(entity, field)
        out: set[str] = set()
        for t in terms:
            if lo is not None and (t < lo or (t == lo and not incl_lo)):
                continue
            if hi is not None and (t > hi or (t == hi and not incl_hi)):
                continue
            out |= self.lookup_eq(entity, field, t)
        return out

    def all_terms(self, entity: str, field: str) -> list[Any]:
        key = _terms_key(entity, field)
        head = self._store.get(self._set, key)
        if head is None:
            return []
        terms = list(head.bins.get("terms", []))
        for j in range(head.bins.get("segs", 0)):
            seg = self._store.get(self._set, _segment_key(key, j))
            if seg is not None:
                terms.extend(seg.bins.get("terms", []))
        return terms
