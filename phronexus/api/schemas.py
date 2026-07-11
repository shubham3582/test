"""Request/response models for the REST layer."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from phronexus.contracts.models import Predicate


class WriteRequest(BaseModel):
    document: dict[str, Any]


class WriteResponse(BaseModel):
    entity: str
    doc_id: str


class SortKeyModel(BaseModel):
    field: str
    order: str = "asc"


class QueryRequest(BaseModel):
    where: list[Predicate] = Field(default_factory=list)
    sort: list[SortKeyModel] = Field(default_factory=list)
    limit: int = 100
    offset: int = 0


class PatternRequest(BaseModel):
    params: dict[str, Any] = Field(default_factory=dict)


class DocumentsResponse(BaseModel):
    entity: str
    count: int
    documents: list[dict[str, Any]]
    offset: int = 0
    limit: int = 100
    has_more: bool = False


class ErrorResponse(BaseModel):
    error: str  # stable error code
    detail: str


class ContractResponse(BaseModel):
    published: str  # contract identity
    activated: bool


class EventRequest(BaseModel):
    """A domain event submitted synchronously (the REST twin of the Kafka input)."""

    event_type: str
    key: str                                  # entity instance id (partition key)
    payload: dict[str, Any] = Field(default_factory=dict)
    # Supply a stable id for idempotency; omitted => each call is a distinct event.
    event_id: str | None = None
    ts: float = 0.0


class ValidationReportResponse(BaseModel):
    ok: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class EventResponse(BaseModel):
    status: str                               # applied | duplicate | rejected
    doc_id: str | None = None
    from_state: str | None = None
    to_state: str | None = None
    emitted: list[str] = Field(default_factory=list)
    reason: str | None = None
