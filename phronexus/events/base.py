"""Event model and sink protocol."""

from __future__ import annotations

import abc
from dataclasses import asdict, dataclass, field
from typing import Any, Literal


@dataclass
class CommitEvent:
    entity: str
    doc_id: str
    txn_id: str
    contract_version: int
    op: Literal["upsert", "delete"]
    ts: float
    document: dict[str, Any] | None = None  # full doc for upserts; None for deletes

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class EventSink(abc.ABC):
    @abc.abstractmethod
    def emit(self, event: CommitEvent) -> None: ...

    def close(self) -> None:  # pragma: no cover - trivial default
        pass


class NullSink(EventSink):
    def emit(self, event: CommitEvent) -> None:  # pragma: no cover
        pass
