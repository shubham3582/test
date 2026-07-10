"""The JSON/YAML query document.

Queries are data, not code — the same shape as a contract's ``where`` clause:

    {"entity": "trade",
     "where": [{"field": "counterparty", "op": "eq", "value": "GS"},
               {"field": "trade_date",   "op": "gte", "value": 20250101}],
     "limit": 100}

Predicates are ANDed. Named patterns from the query contract render into the
same structure after ``${param}`` substitution.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from phronexus.contracts.models import Predicate


class QueryDoc(BaseModel):
    model_config = {"extra": "forbid"}

    entity: str
    where: list[Predicate] = Field(default_factory=list)
    limit: int = 100


def bind_pattern(where: list[Predicate], params: dict[str, Any]) -> list[Predicate]:
    """Substitute ``${param}`` placeholders in a pattern's predicate values."""
    bound: list[Predicate] = []
    for pred in where:
        value = pred.value
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            name = value[2:-1]
            if name not in params:
                raise KeyError(f"missing query parameter {name!r}")
            value = params[name]
        bound.append(Predicate(field=pred.field, op=pred.op, value=value))
    return bound
