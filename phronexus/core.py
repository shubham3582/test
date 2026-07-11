"""The Phronexus facade — the single object apps and the REST layer use.

Wires the KV backend, contract registry, write/read/delete engine, inverted
index + query engine, and view engine together from :class:`Settings`.
"""

from __future__ import annotations

from typing import Any, Optional

import structlog

from phronexus.audit import AuditLog, AuditingSink, AuditWorker
from phronexus.config import Settings
from phronexus.contracts.loader import Contract, load_dir, load_file, parse_contract
from phronexus.contracts.registry import ContractRegistry
from phronexus.errors import ContractNotFound
from phronexus.events import build_sink
from phronexus.kv import build_store
from phronexus.manifest.manager import ManifestManager
from phronexus.manifest.reaper import Reaper
from phronexus.observability.logging import configure_logging
from phronexus.observability.telemetry import Telemetry
from phronexus.query.engine import QueryEngine
from phronexus.query.inverted import InvertedIndex
from phronexus.query.models import QueryDoc
from phronexus.validation import Validator
from phronexus.views.engine import ViewEngine

log = structlog.get_logger(__name__)


class _ManifestReader:
    """Adapts ManifestManager to the QueryEngine's DocReader protocol."""

    def __init__(self, manager: ManifestManager):
        self._m = manager

    def read(self, entity: str, doc_id: str) -> Optional[dict[str, Any]]:
        return self._m.read(entity, doc_id)

    def read_many(self, entity: str, doc_ids) -> dict[str, dict[str, Any]]:
        return self._m.read_many(entity, doc_ids)


class _Lookup:
    """Store lookups for context-aware DQ checks (unique / references)."""

    def __init__(self, px: "Phronexus"):
        self._px = px

    def doc_id(self, entity: str, document: dict[str, Any]) -> Optional[str]:
        try:
            sc = self._px.registry.active_storage(entity)
            return self._px.manifest._proj.compute_doc_id(sc, document)  # noqa: SLF001
        except Exception:  # noqa: BLE001 - missing PK etc. -> no self-exclusion
            return None

    def exists(self, entity: str, doc_id: str) -> bool:
        return self._px.manifest.read(entity, doc_id) is not None

    def others_with(self, entity, field, value, exclude_doc_id) -> bool:
        px = self._px
        # Fast path: the inverted index, when the field is searchable.
        try:
            qc = px.registry.active_query(entity)
            if field in {s.field for s in qc.searchable}:
                ids = px.index.lookup_eq(entity, field, value)
                return any(d != exclude_doc_id for d in ids)
        except ContractNotFound:
            pass
        # Correctness fallback: scan committed documents (O(n) — index the field).
        from phronexus.manifest.manager import M_STATUS, STATUS_COMMITTED

        sc = px.registry.active_storage(entity)
        for key, rec in px.store.scan(sc.manifest_set):
            if key == exclude_doc_id or rec.bins.get(M_STATUS) != STATUS_COMMITTED:
                continue
            doc = px.manifest.read(entity, key)
            if doc and doc.get(field) == value:
                return True
        return False


class Phronexus:
    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        configure_logging(self.settings.observability)

        self.telemetry = Telemetry(self.settings.observability)
        self.store = build_store(self.settings)
        self.registry = ContractRegistry(
            self.store,
            contracts_set=self.settings.aerospike.contracts_set,
            refresh_seconds=self.settings.contracts.refresh_seconds,
            background_refresh=self.settings.contracts.background_refresh,
        )
        self.index = InvertedIndex(
            self.store, index_set=self.settings.aerospike.index_set,
            segment_size=self.settings.index.segment_size,
        )
        self.sink = build_sink(self.settings)
        # Audit trail behind the trace / debug view. A change-feed consumer
        # populates it — standalone in production (`python -m phronexus.audit.main`),
        # off the hot write path. With no Kafka (dev), tee the in-process feed into
        # an inline consumer so a trace still exists with zero services.
        acfg = self.settings.audit
        self.audit = AuditLog(self.store, acfg.audit_set, acfg.ttl_seconds, acfg.enabled)
        if acfg.enabled and not self.settings.kafka.enabled:
            self.sink = AuditingSink(self.sink, AuditWorker(self.audit, self.registry))
        self.validator = Validator(self.registry)
        self.manifest = ManifestManager(
            self.store, self.registry, self.index, self.sink, self.telemetry,
            validator=self.validator, index_in_txn=self.settings.index.in_txn,
            changefeed_set=self.settings.aerospike.changefeed_outbox_set,
            inline_changefeed=self.settings.changefeed.inline_relay,
            write_max_retries=self.settings.aerospike.write_max_retries,
        )
        # Wire store lookups now that the manifest/index exist (enables the
        # unique / references DQ checks).
        self.validator.set_lookup(_Lookup(self))
        self.query_engine = QueryEngine(self.registry, self.index, _ManifestReader(self.manifest))
        self.view_engine = ViewEngine(self.registry)
        self.reaper = Reaper(self.store, self.registry)
        log.info("phronexus.ready", backend=self.settings.backend)

    # --- contract admin -------------------------------------------------

    def publish_contract(
        self, contract: Contract | dict, *, activate: bool = True, force: bool = False
    ) -> None:
        c = contract if not isinstance(contract, dict) else parse_contract(contract)
        self.registry.publish(c, activate=activate, force=force)

    def load_contract_file(self, path: str, *, activate: bool = True) -> None:
        self.registry.publish(load_file(path), activate=activate)

    def load_contract_dir(self, path: str, *, activate: bool = True) -> None:
        for c in load_dir(path):
            self.registry.publish(c, activate=activate)

    def refresh_contracts(self) -> None:
        self.registry.refresh(force=True)

    def list_contracts(self) -> dict:
        """List contracts persisted in the store (source of truth)."""
        return self.registry.list_contracts()

    def get_contract(self, identity: str) -> dict:
        """Fetch one stored contract by identity (e.g. 'storage:trade:v1')."""
        return self.registry.get_version(identity).model_dump(mode="json", by_alias=True)

    def validate_contract(self, doc: dict) -> dict:
        """Dry-run a contract: parse + compatibility check, without publishing."""
        from phronexus.contracts.models import StorageContract
        from phronexus.errors import PhronexusError

        try:
            c = parse_contract(doc)
            if isinstance(c, StorageContract):
                self.registry._check_storage_compatible(c)  # noqa: SLF001
            return {"ok": True, "identity": c.identity(), "kind": c.kind.value}
        except PhronexusError as exc:
            return {"ok": False, "errors": [str(exc)]}

    # --- data plane -----------------------------------------------------

    def put(self, entity: str, document: dict[str, Any]) -> str:
        return self.manifest.write(entity, document)

    def put_many(self, entity: str, documents: list[dict[str, Any]]) -> list[str]:
        """Bulk-ingest documents. Each is a durable manifest commit (own CAS);
        the change-feed is relayed once at the end, not per document."""
        return self.manifest.write_many(entity, documents)

    def get(self, entity: str, doc_id: str) -> Optional[dict[str, Any]]:
        return self.manifest.read(entity, doc_id)

    def get_many(self, entity: str, doc_ids) -> dict[str, dict[str, Any]]:
        """Batch-read documents by id — one batched round-trip per set instead of
        one read each. Missing/uncommitted ids are omitted from the result."""
        return self.manifest.read_many(entity, doc_ids)

    def find(self, entity: str, field: str, value: Any) -> list[dict[str, Any]]:
        """Pull every document whose ``field == value`` via the inverted index
        (a posting list of primary keys) + a single batch read. E.g. all trades
        for a counterparty: ``px.find("trade", "counterparty_id", "CP-GS")``."""
        doc_ids = self.index.lookup_eq(entity, field, value)
        return list(self.get_many(entity, doc_ids).values())

    def trace(self, entity: str, doc_id: str) -> list[dict[str, Any]]:
        """Return the audit trail for one document — every commit/delete with its
        state, oldest first, with transitions derived. Populated by the audit
        change-feed consumer (see :mod:`phronexus.audit`)."""
        return self.audit.trace(entity, doc_id)

    def validate(self, entity: str, document: dict[str, Any]):
        """Run JSON Schema + DQ checks without writing. Returns a ValidationReport."""
        return self.validator.validate(entity, document)

    def validate_event(self, entity: str, event_type: str, payload: dict[str, Any]):
        """Validate an outbound event payload against its stream JSON Schema."""
        return self.validator.validate_event(entity, event_type, payload)

    def delete(self, entity: str, doc_id: str) -> bool:
        return self.manifest.delete(entity, doc_id)

    def query(self, query: QueryDoc | dict) -> list[dict[str, Any]]:
        return self.query_engine.run(query)

    def query_page(self, query: QueryDoc | dict) -> dict[str, Any]:
        """Query with paging metadata: {documents, count, offset, limit, has_more}."""
        return self.query_engine.run_page(query)

    def query_pattern(self, entity: str, pattern: str, **params: Any) -> list[dict[str, Any]]:
        return self.query_engine.run_pattern(entity, pattern, params)

    def view(self, entity: str, view: str, doc_id: str) -> Optional[dict[str, Any]]:
        doc = self.get(entity, doc_id)
        return self.view_engine.apply(entity, view, doc) if doc is not None else None

    def query_view(self, entity: str, view: str, query: QueryDoc | dict) -> list[dict[str, Any]]:
        return self.view_engine.apply_many(entity, view, self.query(query))

    # --- state machine --------------------------------------------------

    def state_machine(self, output=None, hooks=None):
        """Build a transactional state-machine processor over this instance."""
        from phronexus.statemachine.machine import build_state_machine

        return build_state_machine(self, output=output, hooks=hooks)

    def scheduler(self, output=None):
        """Build a distributed scheduler (exactly-once firing) over this instance."""
        from phronexus.scheduler.engine import Scheduler

        return Scheduler(self, output=output)

    # --- journals (binary msgpack) --------------------------------------

    def request_journal(self):
        """Journal of request/response pairs (msgpack blobs + metadata bins)."""
        from phronexus.journal import RequestJournal

        j = self.settings.journal
        return RequestJournal(self.store, set_name=j.interactions_set, ttl=j.ttl_seconds)

    def message_journal(self):
        """Journal of raw inbound messages (full envelope as a msgpack blob)."""
        from phronexus.journal import MessageJournal

        j = self.settings.journal
        return MessageJournal(self.store, set_name=j.messages_set, ttl=j.ttl_seconds)

    # --- native access --------------------------------------------------

    def native_aerospike(self):
        """Supported access to native Aerospike features (expressions, CDT ops,
        operate(), batch, secondary-index queries, UDFs).

        Returns a :class:`phronexus.native.NativeAerospike` that guards
        Phronexus-managed sets. Requires ``backend='aerospike'``.
        """
        from phronexus.native import NativeAerospike

        return NativeAerospike(self)

    # --- lifecycle ------------------------------------------------------

    def close(self) -> None:
        self.registry.close()
        self.sink.close()
        self.store.close()
