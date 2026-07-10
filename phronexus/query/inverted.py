"""A self-maintained inverted index built on the KV store.

Chosen over Aerospike secondary indexes: it is portable across backends, has no
cluster-side index-build step, and fits the manifest model — the index is a
*derived hint*, never the source of truth. A stale or missing posting only means
a candidate is validated away (or briefly not found by search); the document is
always addressable by primary key, and the index can be rebuilt from manifests.

Layout (all in the ``_inv`` set):
    posting  "{entity}\\x1f{field}\\x1f{value}"      -> {docs: [doc_id, ...]}
    terms    "{entity}\\x1f{field}\\x1f__terms__"    -> {terms: [value, ...]}   (numeric only)

Equality / ``in`` read posting lists directly; numeric ranges read the term
dictionary, select values in range, and union their posting lists. Updates use
generation CAS with bounded retries so concurrent writers don't lose postings.
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


def _terms_key(entity: str, field: str) -> str:
    return f"{entity}{_SEP}{field}{_SEP}__terms__"


class InvertedIndex:
    def __init__(self, store: KVStore, index_set: str = "_inv") -> None:
        self._store = store
        self._set = index_set

    # --- maintenance ----------------------------------------------------

    def add(self, entity: str, field: str, value: Any, doc_id: str, *, numeric: bool = False) -> None:
        self._mutate_posting(entity, field, value, doc_id, add=True)
        if numeric:
            self._add_term(entity, field, value)

    def remove(self, entity: str, field: str, value: Any, doc_id: str) -> None:
        self._mutate_posting(entity, field, value, doc_id, add=False)

    def _mutate_posting(self, entity, field, value, doc_id, *, add: bool) -> None:
        key = _posting_key(entity, field, value)
        for _ in range(_MAX_RETRIES):
            rec = self._store.get(self._set, key)
            docs = list(rec.bins.get("docs", [])) if rec else []
            if add:
                if doc_id in docs:
                    return
                docs.append(doc_id)
            else:
                if doc_id not in docs:
                    return
                docs = [d for d in docs if d != doc_id]
            try:
                gen = rec.generation if rec else 0
                if not docs and not add:
                    self._store.remove(self._set, key, expected_generation=gen)
                else:
                    self._store.put(self._set, key, {"docs": docs}, expected_generation=gen)
                return
            except GenerationConflict:
                continue  # someone else wrote; re-read and retry
        log.warning("index.posting.cas_exhausted", entity=entity, field=field)

    def _add_term(self, entity, field, value) -> None:
        key = _terms_key(entity, field)
        for _ in range(_MAX_RETRIES):
            rec = self._store.get(self._set, key)
            terms = list(rec.bins.get("terms", [])) if rec else []
            if value in terms:
                return
            terms.append(value)
            try:
                self._store.put(
                    self._set, key, {"terms": terms},
                    expected_generation=rec.generation if rec else 0,
                )
                return
            except GenerationConflict:
                continue

    # --- lookups --------------------------------------------------------

    def lookup_eq(self, entity: str, field: str, value: Any) -> set[str]:
        rec = self._store.get(self._set, _posting_key(entity, field, value))
        return set(rec.bins.get("docs", [])) if rec else set()

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
        rec = self._store.get(self._set, _terms_key(entity, field))
        terms = rec.bins.get("terms", []) if rec else []
        out: set[str] = set()
        for t in terms:
            if lo is not None and (t < lo or (t == lo and not incl_lo)):
                continue
            if hi is not None and (t > hi or (t == hi and not incl_hi)):
                continue
            out |= self.lookup_eq(entity, field, t)
        return out

    def all_terms(self, entity: str, field: str) -> list[Any]:
        rec = self._store.get(self._set, _terms_key(entity, field))
        return list(rec.bins.get("terms", [])) if rec else []
