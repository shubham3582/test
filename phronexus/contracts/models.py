"""Pydantic models for the three contract kinds.

These are the *only* place business-entity shape is described. Onboarding a new
entity (trade, FX, repo, …) means authoring these documents — no code changes.
Everything is validated on load so a malformed contract fails fast rather than
at write time.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

_KEY_TOKEN = re.compile(r"\{([a-zA-Z0-9_.]+)\}")


class UpdatePolicy(str, Enum):
    upsert = "upsert"
    insert_only = "insert_only"


class DeletePolicy(str, Enum):
    soft = "soft"
    hard = "hard"


class IndexType(str, Enum):
    string = "string"
    numeric = "numeric"


class ContractKind(str, Enum):
    storage = "storage"
    query = "query"
    view = "view"
    transition = "transition"
    validation = "validation"
    stream = "stream"


class _Base(BaseModel):
    model_config = {"extra": "forbid"}


# --- storage contract ----------------------------------------------------

class IcebergConfig(_Base):
    enabled: bool = False
    table: Optional[str] = None
    partition_by: list[str] = Field(default_factory=list)
    retention_days: int = 0


class BinSpread(_Base):
    """Transpose a map-valued field into one bin PER ENTRY (data-driven bin names).

    Pivots ``{field: {key: value, ...}}`` into bins ``prefix+key -> value`` — e.g.
    a future-value cube's ``curve: {"20260712": ..., "20260718": ...}`` becomes
    bins ``d20260712``, ``d20260718``. The keys aren't known at contract time, so
    bin-name length (≤15) is checked at write time.
    """

    field: str          # a map-valued document field to explode
    prefix: str = ""    # bin name = prefix + str(key)


class Projection(_Base):
    name: str
    set: str  # Aerospike set the projection records live in
    key: str  # key template, e.g. "{counterparty}:{trade_id}"
    fields: list[str] = Field(default_factory=lambda: ["*"])  # ["*"] = whole doc
    ttl: int = 0  # seconds; 0 = never expire
    # How the projected payload is laid out in the record:
    #   map      — one ``doc`` bin holding an Aerospike map (default)
    #   msgpack  — one ``doc`` bin holding a compact binary blob (byte-faithful,
    #              cross-language) — good for a space-efficient whole-document store
    #   bins     — each selected element becomes its OWN top-level Aerospike bin,
    #              so it's natively addressable (secondary indexes, expressions,
    #              partial reads). Use ``bin_map`` to name/shorten bins.
    encoding: Literal["map", "msgpack", "bins"] = "map"
    # For ``encoding: bins`` only — rename a document field to an Aerospike bin
    # (e.g. shorten "counterparty_id" -> "cp"). Fields not listed keep their name.
    bin_map: dict[str, str] = Field(default_factory=dict)
    # For ``encoding: bins`` only — transpose a map field into per-entry bins
    # (data-driven bin names). See :class:`BinSpread`.
    spread: list[BinSpread] = Field(default_factory=list)
    # Exactly one projection per contract must be canonical: it holds the full
    # document and is what reads reconstruct from.
    canonical: bool = False

    @property
    def is_full(self) -> bool:
        return self.fields == ["*"]

    def key_tokens(self) -> list[str]:
        return _KEY_TOKEN.findall(self.key)

    def bin_for(self, field: str) -> str:
        """The Aerospike bin name a document field maps to under ``encoding: bins``."""
        return self.bin_map.get(field, field)

    @model_validator(mode="after")
    def _validate_encoding(self) -> "Projection":
        _RESERVED = {"doc", "_doc_id", "_txn", "_cver", "_pjn"}
        if self.bin_map and self.encoding != "bins":
            raise ValueError(f"projection {self.name!r}: bin_map requires encoding: bins")
        if self.spread and self.encoding != "bins":
            raise ValueError(f"projection {self.name!r}: spread requires encoding: bins")
        if self.spread and self.canonical:
            # Reconstruction from data-driven bin names isn't supported; keep the
            # canonical projection a clean nested doc (map/msgpack).
            raise ValueError(f"projection {self.name!r}: spread not allowed on the canonical projection")
        spread_fields = {s.field for s in self.spread}
        for s in self.spread:
            if self.fields != ["*"] and s.field not in self.fields:
                raise ValueError(f"projection {self.name!r}: spread field {s.field!r} must be in fields")
        if self.encoding == "bins":
            # Validate the bin names we can know at publish time: every explicit
            # non-spread field's bin, plus every bin_map target. (Wildcard '*'
            # fields and spread keys are checked at write time.) Aerospike caps
            # bin names at 15 bytes.
            known = set(self.bin_map.values())
            known |= {self.bin_for(f) for f in self.fields if f != "*" and f not in spread_fields}
            seen: set[str] = set()
            for bn in known:
                if len(bn) > 15:
                    raise ValueError(f"projection {self.name!r}: bin name {bn!r} exceeds 15 chars")
                if bn in _RESERVED:
                    raise ValueError(f"projection {self.name!r}: bin name {bn!r} is reserved")
                if bn in seen:
                    raise ValueError(f"projection {self.name!r}: duplicate bin name {bn!r}")
                seen.add(bn)
        return self


class StorageContract(_Base):
    kind: Literal[ContractKind.storage] = ContractKind.storage
    entity: str
    version: int
    primary_key: list[str]
    manifest_set: str
    projections: list[Projection]
    update_policy: UpdatePolicy = UpdatePolicy.upsert
    delete_policy: DeletePolicy = DeletePolicy.soft
    # Per-entity override of the backend's ``aerospike.use_native_txn``. The
    # manifest is always the visibility commit point; this only chooses HOW the
    # multi-record write underneath is done:
    #   None  — inherit the backend setting (default)
    #   true  — wrap the write in a native Aerospike multi-record txn (8.0+ EE)
    #   false — ordered puts, manifest written last as the commit point (works on CE)
    native_txn: Optional[bool] = None
    iceberg: IcebergConfig = Field(default_factory=IcebergConfig)

    @field_validator("primary_key")
    @classmethod
    def _pk_nonempty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("primary_key must have at least one field")
        return v

    @model_validator(mode="after")
    def _validate(self) -> "StorageContract":
        names = [p.name for p in self.projections]
        if len(names) != len(set(names)):
            raise ValueError("projection names must be unique")
        canon = [p for p in self.projections if p.canonical]
        if len(canon) != 1:
            raise ValueError("exactly one projection must be marked canonical")
        if not canon[0].is_full:
            raise ValueError("the canonical projection must have fields = ['*']")
        # A canonical projection's key can only reference primary-key fields, so
        # a document is always addressable by its id alone.
        pk = set(self.primary_key)
        for tok in canon[0].key_tokens():
            if tok not in pk:
                raise ValueError(
                    f"canonical projection key references non-PK field {tok!r}"
                )
        return self

    @property
    def canonical_projection(self) -> Projection:
        return next(p for p in self.projections if p.canonical)

    def identity(self) -> str:
        return f"storage:{self.entity}:v{self.version}"


# --- query contract ------------------------------------------------------

class SearchableField(_Base):
    field: str
    index: IndexType = IndexType.string
    # Optional human label for the inverted index on this field (e.g. "idx_cp").
    # The index itself is maintained per (entity, field); this names it for
    # operators/docs and shows up in index-maintenance logs.
    name: Optional[str] = None
    # Ranges (gt/gte/lt/lte) require numeric ordering; string fields are eq/in.
    @property
    def supports_range(self) -> bool:
        return self.index == IndexType.numeric

    @property
    def index_name(self) -> str:
        return self.name or f"idx_{self.field}"


class Predicate(_Base):
    field: str
    op: Literal["eq", "ne", "in", "gt", "gte", "lt", "lte"] = "eq"
    value: Any = None


class QueryPattern(_Base):
    """A named, parameterised query defined in config (JSON/YAML).

    ``where`` values may use ``${param}`` placeholders bound at call time, e.g.
    ``{field: counterparty, op: eq, value: "${cpty}"}``.
    """

    name: str
    where: list[Predicate]
    limit: int = 100


class QueryContract(_Base):
    kind: Literal[ContractKind.query] = ContractKind.query
    entity: str
    version: int
    searchable: list[SearchableField]
    patterns: list[QueryPattern] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate(self) -> "QueryContract":
        fields = {s.field for s in self.searchable}
        for p in self.patterns:
            for pred in p.where:
                if pred.field not in fields:
                    raise ValueError(
                        f"pattern {p.name!r} filters non-searchable field {pred.field!r}"
                    )
        return self

    def field_index(self) -> dict[str, SearchableField]:
        return {s.field: s for s in self.searchable}

    def identity(self) -> str:
        return f"query:{self.entity}:v{self.version}"


# --- view contract -------------------------------------------------------

class ViewContract(_Base):
    kind: Literal[ContractKind.view] = ContractKind.view
    entity: str
    view: str
    version: int
    fields: list[str]  # allow-list projected into the output
    mask: list[str] = Field(default_factory=list)  # fields to redact
    transform: dict[str, str] = Field(default_factory=dict)  # field -> transform name

    @model_validator(mode="after")
    def _validate(self) -> "ViewContract":
        allowed = set(self.fields)
        for m in self.mask:
            if m not in allowed:
                raise ValueError(f"masked field {m!r} not in view fields")
        for f in self.transform:
            if f not in allowed:
                raise ValueError(f"transformed field {f!r} not in view fields")
        return self

    def identity(self) -> str:
        return f"view:{self.entity}:{self.view}:v{self.version}"


# --- transition contract (state machine) --------------------------------

class EmitSpec(_Base):
    topic: str                       # output destination, e.g. "kafka://trades.booked"
    type: Optional[str] = None       # output event type; defaults to the transition's `on`


class Transition(_Base):
    # Input event type that triggers this transition. Named ``event`` (not ``on``)
    # because YAML 1.1 parses a bare ``on:`` key as the boolean True.
    event: str
    # source state; None matches "no existing document" (creation), "*" matches any.
    from_: Optional[str] = Field(default=None, alias="from")
    to: str                          # target state written into state_field
    guard: Optional[str] = None      # safe boolean expression over the candidate doc
    emit: list[EmitSpec] = Field(default_factory=list)

    model_config = {"extra": "forbid", "populate_by_name": True}


class TransitionContract(_Base):
    kind: Literal[ContractKind.transition] = ContractKind.transition
    entity: str
    version: int
    state_field: str = "status"
    inputs: list[str] = Field(default_factory=list)  # informational: input topics
    transitions: list[Transition]

    @model_validator(mode="after")
    def _validate(self) -> "TransitionContract":
        if not self.transitions:
            raise ValueError("a transition contract needs at least one transition")
        return self

    def match(self, event_type: str, current_state: Optional[str]) -> Optional[Transition]:
        """Find the transition for an event type given the current state."""
        for t in self.transitions:
            if t.event != event_type:
                continue
            if t.from_ == "*":
                return t
            if t.from_ is None and current_state is None:
                return t
            if t.from_ == current_state:
                return t
        return None

    def identity(self) -> str:
        return f"transition:{self.entity}:v{self.version}"


# --- validation contract (JSON Schema + data-quality checks) -------------

class ValidationMode(str, Enum):
    enforce = "enforce"        # errors block the write
    warn_only = "warn_only"    # everything downgraded to a warning; never blocks
    off = "off"                # skip validation


class DQCheck(_Base):
    """A declarative data-quality rule — field-scoped or a cross-field expression."""

    name: str
    severity: Literal["error", "warn"] = "error"
    message: Optional[str] = None
    # cross-field: a sandboxed boolean expression over the document
    expr: Optional[str] = None
    # field-scoped constraints (any subset; all must hold)
    field: Optional[str] = None
    required: bool = False
    type: Optional[Literal["string", "number", "integer", "boolean"]] = None
    allowed: Optional[list[Any]] = Field(default=None, alias="in")  # value allow-list
    min: Optional[float] = None
    max: Optional[float] = None
    min_len: Optional[int] = None
    max_len: Optional[int] = None
    regex: Optional[str] = None
    # context-aware checks (require a store lookup)
    unique: bool = False               # value must be unique across the entity
    references: Optional[str] = None   # value must be an existing doc id of this entity

    model_config = {"extra": "forbid", "populate_by_name": True}

    @model_validator(mode="after")
    def _validate(self) -> "DQCheck":
        if not self.expr and not self.field:
            raise ValueError(f"dq check {self.name!r} must set either 'expr' or 'field'")
        return self


class ValidationContract(_Base):
    kind: Literal[ContractKind.validation] = ContractKind.validation
    entity: str
    version: int
    mode: ValidationMode = ValidationMode.enforce
    json_schema: Optional[dict[str, Any]] = None   # structural / syntax validation
    dq_checks: list[DQCheck] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate(self) -> "ValidationContract":
        if self.json_schema is not None:
            # Fail fast on a malformed schema at publish time.
            from jsonschema import Draft202012Validator

            Draft202012Validator.check_schema(self.json_schema)
        return self

    def identity(self) -> str:
        return f"validation:{self.entity}:v{self.version}"


# --- stream contract (JSON Schema on published events; no external registry) --

class EventSchema(_Base):
    type: str                          # output event type (matches emit.type)
    json_schema: dict[str, Any]

    @model_validator(mode="after")
    def _check(self) -> "EventSchema":
        from jsonschema import Draft202012Validator

        Draft202012Validator.check_schema(self.json_schema)
        return self


class StreamContract(_Base):
    """Registers a JSON Schema per outbound event type.

    A registry substitute: events stay JSON on the wire but are validated against
    their schema at produce time, so malformed events are never published.
    """

    kind: Literal[ContractKind.stream] = ContractKind.stream
    entity: str
    version: int
    mode: ValidationMode = ValidationMode.enforce
    events: list[EventSchema]

    def schema_for(self, event_type: str) -> Optional[dict[str, Any]]:
        for e in self.events:
            if e.type == event_type:
                return e.json_schema
        return None

    def identity(self) -> str:
        return f"stream:{self.entity}:v{self.version}"
