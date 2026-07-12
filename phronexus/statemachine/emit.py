"""Build an outbound event payload from a candidate document.

A transition's :class:`~phronexus.contracts.models.EmitSpec` may shape the
outbound message from *specific* fields of the candidate document instead of
emitting the whole thing:

    allow-list ``fields``  ->  per-field ``transform``  ->  ``rename`` to output keys

The default (``fields: ["*"]``, no rename/transform) reproduces the full
document, so contracts that don't opt in are unchanged. The shaped payload is
then validated against the entity's stream schema at produce time.
"""

from __future__ import annotations

from typing import Any

from phronexus.contracts.models import EmitSpec
from phronexus.views.engine import get_transform


def build_emit_payload(spec: EmitSpec, doc: dict[str, Any]) -> dict[str, Any]:
    src = dict(doc) if spec.is_full else {f: doc[f] for f in spec.fields if f in doc}
    out: dict[str, Any] = {}
    for field, value in src.items():
        tname = spec.transform.get(field)
        if tname:
            value = get_transform(tname)(value)
        out[spec.rename.get(field, field)] = value
    return out
