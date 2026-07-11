"""Parse contract documents (dict / YAML / JSON) into validated models."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError as PydanticValidationError

from phronexus.contracts.models import (
    ContractKind,
    QueryContract,
    StorageContract,
    StreamContract,
    TransitionContract,
    ValidationContract,
    ViewContract,
)
from phronexus.errors import ContractValidationError

_MODEL_BY_KIND = {
    ContractKind.storage.value: StorageContract,
    ContractKind.query.value: QueryContract,
    ContractKind.view.value: ViewContract,
    ContractKind.transition.value: TransitionContract,
    ContractKind.validation.value: ValidationContract,
    ContractKind.stream.value: StreamContract,
}

Contract = (
    StorageContract | QueryContract | ViewContract | TransitionContract
    | ValidationContract | StreamContract
)


def parse_contract(doc: dict[str, Any]) -> Contract:
    kind = doc.get("kind")
    model = _MODEL_BY_KIND.get(kind)
    if model is None:
        raise ContractValidationError(
            f"unknown or missing contract kind: {kind!r} "
            f"(expected one of {sorted(_MODEL_BY_KIND)})"
        )
    try:
        return model.model_validate(doc)
    except PydanticValidationError as exc:
        raise ContractValidationError(f"invalid {kind} contract: {exc}") from exc


def load_file(path: str | Path) -> Contract:
    text = Path(path).read_text()
    return parse_contract(yaml.safe_load(text))


def load_dir(path: str | Path) -> list[Contract]:
    """Load every ``*.yaml`` / ``*.yml`` / ``*.json`` contract under a directory."""
    root = Path(path)
    out: list[Contract] = []
    for p in sorted(root.rglob("*")):
        if p.suffix.lower() in {".yaml", ".yml", ".json"}:
            out.append(load_file(p))
    return out
