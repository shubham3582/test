"""Data-plane endpoints: write / read / delete / query / view."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from phronexus.api.auth import Principal
from phronexus.api.deps import get_px, require_principal
from phronexus.api.schemas import (
    DocumentsResponse,
    PatternRequest,
    QueryRequest,
    ValidationReportResponse,
    WriteRequest,
    WriteResponse,
)
from phronexus.core import Phronexus
from phronexus.errors import DocumentNotFound
from phronexus.query.models import QueryDoc

router = APIRouter(tags=["data"], dependencies=[Depends(require_principal)])


@router.put("/entities/{entity}/documents", response_model=WriteResponse)
def write(entity: str, body: WriteRequest, px: Phronexus = Depends(get_px)) -> WriteResponse:
    doc_id = px.put(entity, body.document)
    return WriteResponse(entity=entity, doc_id=doc_id)


@router.post("/entities/{entity}/validate", response_model=ValidationReportResponse)
def validate(entity: str, body: WriteRequest, px: Phronexus = Depends(get_px)) -> ValidationReportResponse:
    """Dry-run JSON Schema + DQ checks without writing (200 with ok/errors/warnings)."""
    report = px.validate(entity, body.document)
    return ValidationReportResponse(ok=report.ok, errors=report.errors, warnings=report.warnings)


@router.get("/entities/{entity}/documents/{doc_id}")
def read(entity: str, doc_id: str, px: Phronexus = Depends(get_px)) -> dict:
    doc = px.get(entity, doc_id)
    if doc is None:
        raise DocumentNotFound(f"{entity}/{doc_id} not found")
    return doc


@router.delete("/entities/{entity}/documents/{doc_id}")
def delete(entity: str, doc_id: str, px: Phronexus = Depends(get_px)) -> dict:
    px.delete(entity, doc_id)
    return {"deleted": True, "entity": entity, "doc_id": doc_id}


@router.get("/entities/{entity}/views/{view}/documents/{doc_id}")
def read_view(entity: str, view: str, doc_id: str, px: Phronexus = Depends(get_px)) -> dict:
    doc = px.view(entity, view, doc_id)
    if doc is None:
        raise DocumentNotFound(f"{entity}/{doc_id} not found")
    return doc


@router.post("/entities/{entity}/query", response_model=DocumentsResponse)
def query(
    entity: str,
    body: QueryRequest,
    view: str | None = None,
    px: Phronexus = Depends(get_px),
) -> DocumentsResponse:
    q = QueryDoc(entity=entity, where=body.where, limit=body.limit)
    docs = px.query_view(entity, view, q) if view else px.query(q)
    return DocumentsResponse(entity=entity, count=len(docs), documents=docs)


@router.post("/entities/{entity}/patterns/{pattern}", response_model=DocumentsResponse)
def query_pattern(
    entity: str,
    pattern: str,
    body: PatternRequest,
    px: Phronexus = Depends(get_px),
) -> DocumentsResponse:
    docs = px.query_pattern(entity, pattern, **body.params)
    return DocumentsResponse(entity=entity, count=len(docs), documents=docs)
