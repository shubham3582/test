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


class QueryRequest(BaseModel):
    where: list[Predicate] = Field(default_factory=list)
    limit: int = 100


class PatternRequest(BaseModel):
    params: dict[str, Any] = Field(default_factory=dict)


class DocumentsResponse(BaseModel):
    entity: str
    count: int
    documents: list[dict[str, Any]]


class ErrorResponse(BaseModel):
    error: str  # stable error code
    detail: str


class ContractResponse(BaseModel):
    published: str  # contract identity
    activated: bool
