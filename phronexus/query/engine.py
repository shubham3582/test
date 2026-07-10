"""Execute JSON/YAML queries against the inverted index.

Flow: validate predicate fields against the query contract → resolve candidate
doc-ids from the index (intersecting ANDed predicates) → load each candidate
through the manifest (so results are always committed & consistent) → re-check
every predicate on the loaded document. That final re-check makes results
correct even if the index is momentarily stale.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Protocol

import structlog

from phronexus.contracts.models import Predicate, QueryContract
from phronexus.contracts.registry import ContractRegistry
from phronexus.errors import QueryError
from phronexus.query.inverted import InvertedIndex
from phronexus.query.models import QueryDoc, bind_pattern

log = structlog.get_logger(__name__)

_POSITIVE = {"eq", "in", "gt", "gte", "lt", "lte"}


class DocReader(Protocol):
    def read(self, entity: str, doc_id: str) -> Optional[dict[str, Any]]: ...


class QueryEngine:
    def __init__(self, registry: ContractRegistry, index: InvertedIndex, reader: DocReader):
        self._registry = registry
        self._index = index
        self._reader = reader

    def run(self, query: QueryDoc | dict) -> list[dict[str, Any]]:
        q = query if isinstance(query, QueryDoc) else QueryDoc.model_validate(query)
        qc = self._registry.active_query(q.entity)
        self._validate(qc, q.where)

        positive = [p for p in q.where if p.op in _POSITIVE]
        if not positive:
            raise QueryError(
                "query needs at least one eq/in/range predicate to seed candidates"
            )

        candidates: Optional[set[str]] = None
        for pred in positive:
            ids = self._resolve(qc, q.entity, pred)
            candidates = ids if candidates is None else (candidates & ids)
            if not candidates:
                return []

        results: list[dict[str, Any]] = []
        for doc_id in candidates or set():
            doc = self._reader.read(q.entity, doc_id)
            if doc is None:
                continue  # index was stale / doc since deleted
            if all(self._match(pred, doc) for pred in q.where):
                results.append(doc)
                if len(results) >= q.limit:
                    break
        return results

    def run_pattern(self, entity: str, pattern: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        qc = self._registry.active_query(entity)
        pat = next((p for p in qc.patterns if p.name == pattern), None)
        if pat is None:
            raise QueryError(f"no query pattern {pattern!r} for entity {entity!r}")
        where = bind_pattern(pat.where, params)
        return self.run(QueryDoc(entity=entity, where=where, limit=pat.limit))

    # --- internals ------------------------------------------------------

    def _validate(self, qc: QueryContract, where: list[Predicate]) -> None:
        idx = qc.field_index()
        for pred in where:
            sf = idx.get(pred.field)
            if sf is None:
                raise QueryError(f"field {pred.field!r} is not searchable for {qc.entity!r}")
            if pred.op in {"gt", "gte", "lt", "lte"} and not sf.supports_range:
                raise QueryError(
                    f"range op {pred.op!r} requires a numeric index on {pred.field!r}"
                )

    def _resolve(self, qc: QueryContract, entity: str, pred: Predicate) -> set[str]:
        if pred.op == "eq":
            return self._index.lookup_eq(entity, pred.field, pred.value)
        if pred.op == "in":
            if not isinstance(pred.value, (list, tuple, set)):
                raise QueryError(f"'in' predicate on {pred.field!r} needs a list value")
            return self._index.lookup_in(entity, pred.field, pred.value)
        if pred.op == "gte":
            return self._index.lookup_range(entity, pred.field, lo=pred.value, incl_lo=True)
        if pred.op == "gt":
            return self._index.lookup_range(entity, pred.field, lo=pred.value, incl_lo=False)
        if pred.op == "lte":
            return self._index.lookup_range(entity, pred.field, hi=pred.value, incl_hi=True)
        if pred.op == "lt":
            return self._index.lookup_range(entity, pred.field, hi=pred.value, incl_hi=False)
        raise QueryError(f"unsupported positive op {pred.op!r}")

    @staticmethod
    def _match(pred: Predicate, doc: dict[str, Any]) -> bool:
        actual = doc.get(pred.field)
        op, value = pred.op, pred.value
        if op == "eq":
            return actual == value
        if op == "ne":
            return actual != value
        if op == "in":
            return actual in value
        if actual is None:
            return False
        if op == "gt":
            return actual > value
        if op == "gte":
            return actual >= value
        if op == "lt":
            return actual < value
        if op == "lte":
            return actual <= value
        return False
