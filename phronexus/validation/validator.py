"""Validate a document against its validation contract.

Two layers, both metadata-driven:

1. **JSON Schema** (Draft 2020-12) — structural / syntax validation of the
   document shape. Any schema violation is an error.
2. **Data-quality checks** — declarative field-scoped rules (required, type,
   allow-list, min/max, length, regex) and cross-field boolean expressions
   (sandboxed, reusing the transition-guard evaluator). Each carries a severity.

The contract's ``mode`` decides enforcement: ``enforce`` (errors block the
write), ``warn_only`` (everything downgraded to warnings), or ``off``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import structlog

from phronexus.contracts.models import DQCheck, ValidationMode
from phronexus.contracts.registry import ContractRegistry
from phronexus.errors import ContractNotFound, ValidationError
from phronexus.statemachine.guard import safe_eval

log = structlog.get_logger(__name__)

_MISSING = object()


@dataclass
class ValidationReport:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def raise_if_failed(self) -> None:
        if not self.ok:
            raise ValidationError("validation failed: " + "; ".join(self.errors))


class Validator:
    def __init__(self, registry: ContractRegistry):
        self._registry = registry

    def validate(self, entity: str, document: dict[str, Any]) -> ValidationReport:
        try:
            vc = self._registry.active_validation(entity)
        except ContractNotFound:
            return ValidationReport(ok=True)  # no contract -> nothing to validate
        if vc.mode == ValidationMode.off:
            return ValidationReport(ok=True)

        errors: list[str] = []
        warnings: list[str] = []

        # 1) JSON Schema (structural) — violations are errors.
        if vc.json_schema is not None:
            errors.extend(f"schema: {e}" for e in self._schema_errors(vc.json_schema, document))

        # 2) Data-quality checks.
        for check in vc.dq_checks:
            passed, detail = self._eval_check(check, document)
            if not passed:
                msg = check.message or f"{check.name}: {detail}"
                (errors if check.severity == "error" else warnings).append(msg)

        # warn_only never blocks: downgrade errors to warnings.
        if vc.mode == ValidationMode.warn_only:
            warnings = warnings + errors
            errors = []

        return ValidationReport(ok=not errors, errors=errors, warnings=warnings)

    # --- internals ------------------------------------------------------

    @staticmethod
    def _schema_errors(schema: dict[str, Any], document: dict[str, Any]) -> list[str]:
        from jsonschema import Draft202012Validator

        validator = Draft202012Validator(schema)
        out: list[str] = []
        for err in sorted(validator.iter_errors(document), key=lambda e: list(e.path)):
            loc = "/".join(str(p) for p in err.path) or "<root>"
            out.append(f"{loc}: {err.message}")
        return out

    def _eval_check(self, check: DQCheck, doc: dict[str, Any]) -> tuple[bool, str]:
        # Cross-field expression.
        if check.expr:
            try:
                return (bool(safe_eval(check.expr, doc)), f"expression {check.expr!r} is false")
            except Exception as exc:  # noqa: BLE001 - guard/eval error -> failed check
                return (False, f"expression error: {exc}")

        # Field-scoped constraints.
        val = doc.get(check.field, _MISSING)
        if check.required and (val is _MISSING or val is None):
            return (False, f"field {check.field!r} is required")
        if val is _MISSING or val is None:
            return (True, "")  # absent optional field: nothing else to check

        if check.type and not _type_ok(check.type, val):
            return (False, f"field {check.field!r} expected type {check.type}")
        if check.allowed is not None and val not in check.allowed:
            return (False, f"field {check.field!r}={val!r} not in {check.allowed}")
        if check.min is not None and _is_num(val) and val < check.min:
            return (False, f"field {check.field!r}={val} < min {check.min}")
        if check.max is not None and _is_num(val) and val > check.max:
            return (False, f"field {check.field!r}={val} > max {check.max}")
        if check.min_len is not None and hasattr(val, "__len__") and len(val) < check.min_len:
            return (False, f"field {check.field!r} shorter than {check.min_len}")
        if check.max_len is not None and hasattr(val, "__len__") and len(val) > check.max_len:
            return (False, f"field {check.field!r} longer than {check.max_len}")
        if check.regex is not None and not re.search(check.regex, str(val)):
            return (False, f"field {check.field!r} does not match /{check.regex}/")
        return (True, "")


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _type_ok(expected: str, v: Any) -> bool:
    if expected == "string":
        return isinstance(v, str)
    if expected == "boolean":
        return isinstance(v, bool)
    if expected == "integer":
        return isinstance(v, int) and not isinstance(v, bool)
    if expected == "number":
        return _is_num(v)
    return True
