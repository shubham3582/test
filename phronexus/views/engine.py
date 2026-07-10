"""Apply a view contract to a document.

Order: allow-list the view's ``fields`` → apply ``transform`` functions → apply
``mask`` redaction. Transforms are looked up by name from a central registry so
new ones are added in one place and referenced from any view contract.
"""

from __future__ import annotations

from typing import Any, Callable

from phronexus.contracts.registry import ContractRegistry
from phronexus.errors import ViewError

Transform = Callable[[Any], Any]

_TRANSFORMS: dict[str, Transform] = {
    "round2": lambda v: round(float(v), 2) if v is not None else None,
    "upper": lambda v: v.upper() if isinstance(v, str) else v,
    "lower": lambda v: v.lower() if isinstance(v, str) else v,
    "abs": lambda v: abs(v) if isinstance(v, (int, float)) else v,
}

_MASK = "****"


def register_transform(name: str, fn: Transform) -> None:
    _TRANSFORMS[name] = fn


class ViewEngine:
    def __init__(self, registry: ContractRegistry):
        self._registry = registry

    def apply(self, entity: str, view: str, document: dict[str, Any]) -> dict[str, Any]:
        vc = self._registry.active_view(entity, view)
        out: dict[str, Any] = {}
        for f in vc.fields:
            if f not in document:
                continue
            value = document[f]
            tname = vc.transform.get(f)
            if tname:
                fn = _TRANSFORMS.get(tname)
                if fn is None:
                    raise ViewError(f"unknown transform {tname!r} on field {f!r}")
                value = fn(value)
            if f in vc.mask:
                value = _MASK
            out[f] = value
        return out

    def apply_many(self, entity: str, view: str, docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [self.apply(entity, view, d) for d in docs]
