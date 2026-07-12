"""The write/read/delete engine built on the manifest pattern.

Write: all projection records are written first, then the manifest last — the
single commit point that makes a document visible. On backends with native
multi-record transactions (Aerospike 8.0+, and the in-memory backend) the whole
set is one atomic transaction; the manifest still exists so reads have a single
authoritative "is this committed?" record and so partial/crashed writes leave
only invisible orphans (swept by the reaper).

Concurrency: the manifest is written with a generation CAS, giving optimistic
concurrency across competing writers. The inverted index is maintained *after*
commit as a derived structure — a stale posting is validated away on read.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

import structlog

from phronexus.contracts.models import DeletePolicy, StorageContract, UpdatePolicy
from phronexus.contracts.registry import ContractRegistry
from phronexus.errors import (
    ContractNotFound,
    DocumentAlreadyExists,
    DocumentNotFound,
    GenerationConflict,
)
from phronexus.events.base import CommitEvent, EventSink
from phronexus.kv.base import KVStore
from phronexus.observability.telemetry import Telemetry
from phronexus.query.inverted import InvertedIndex
from phronexus.storage.projection import META_TXN, ProjectionEngine, decode_record

log = structlog.get_logger(__name__)

_MISSING = object()

# Manifest bins
M_STATUS = "status"
M_TXN = "txn"
M_CVER = "cver"
M_DOC_ID = "doc_id"
M_CANONICAL = "canonical"  # {"set":..., "key":...}
M_PROJECTIONS = "projections"  # [{"set":..., "key":...}, ...]
M_TS = "ts"
M_PREV_TXN = "prev_txn"

STATUS_COMMITTED = "committed"
STATUS_DELETED = "deleted"


@dataclass
class StagedWrite:
    """Everything :meth:`ManifestManager.post_write` needs after a staged commit."""

    entity: str
    doc_id: str
    txn_id: str
    sc: Any
    records: list
    old_data: dict
    existing: Any
    ts: float


class ManifestManager:
    def __init__(
        self,
        store: KVStore,
        registry: ContractRegistry,
        index: InvertedIndex,
        sink: EventSink,
        telemetry: Telemetry,
        validator=None,
        index_in_txn: bool = True,
        changefeed_set: str = "_cf_outbox",
        inline_changefeed: bool = True,
        write_max_retries: int = 3,
        verify_commit: bool = False,
        orphan_grace_seconds: int = 300,
    ):
        self._store = store
        self._registry = registry
        self._index = index
        self._sink = sink
        self._tel = telemetry
        self._validator = validator
        self._index_in_txn = index_in_txn
        self._changefeed_set = changefeed_set
        self._inline_changefeed = inline_changefeed
        self._write_max_retries = write_max_retries
        # Verify a doc committed before relaying its event (guards against phantom
        # events from a non-transactional crash); reap orphan outbox rows past the
        # grace period. A no-op cost path when verify_commit is False (native txn).
        self._verify_commit = verify_commit
        self._orphan_grace_seconds = orphan_grace_seconds
        self._proj = ProjectionEngine()

    # --- write ----------------------------------------------------------

    def write(self, entity: str, document: dict[str, Any], *, relay: bool = True) -> str:
        with self._tel.span("manifest.write", entity=entity), self._tel.timed(
            "phronexus.write.latency", entity=entity
        ):
            # Stage the projections + manifest into one transaction, commit, then
            # run post-commit side effects (index, reap, change-feed relay).
            # Retry on optimistic-concurrency conflicts (competing writers /
            # hot-term index contention) — stage_write re-reads current state.
            native = self.native_txn_for(entity)
            for attempt in range(self._write_max_retries + 1):
                try:
                    with self._store.transaction(native=native) as txn:
                        staged = self.stage_write(entity, document, txn)
                    break
                except GenerationConflict:
                    self._tel.incr("phronexus.write.conflicts", entity=entity)
                    if attempt >= self._write_max_retries:
                        raise
            self.post_write(staged, document, relay=relay)
            return staged.doc_id

    def native_txn_for(self, entity: str) -> Optional[bool]:
        """The storage contract's per-entity native-transaction override; ``None``
        means inherit the backend's ``aerospike.use_native_txn``."""
        try:
            return self._registry.active_storage(entity).native_txn
        except ContractNotFound:
            return None

    def write_many(self, entity: str, documents: list[dict[str, Any]]) -> list[str]:
        """Bulk write: one transaction per document (each has its own manifest
        CAS), but the change-feed is relayed **once** at the end instead of per
        document — a big win for bulk ingestion."""
        ids = [self.write(entity, d, relay=False) for d in documents]
        if self._inline_changefeed:
            self.drain_changefeed()
        return ids

    def stage_write(self, entity: str, document: dict[str, Any], txn,
                    stage_extra=None) -> "StagedWrite":
        """Stage projections + manifest into ``txn`` without committing.

        Exposed so a caller (e.g. the state-machine processor) can compose a
        document write with additional records — outbox events, dedup markers —
        in a single atomic transaction. Call :meth:`post_write` after commit.

        ``stage_extra(txn)`` is invoked just before the manifest put, so a caller
        can stage its own records (e.g. state-machine output events) *before* the
        commit point — this keeps the manifest genuinely the last write, so on a
        non-transactional (CE) crash a committed document always implies its
        outputs are already durable (they are never lost).
        """
        # Validate at the write boundary — covers put(), the state machine, and
        # backfill uniformly. Errors abort the (surrounding) transaction.
        if self._validator is not None:
            report = self._validator.validate(entity, document)
            if report.warnings:
                log.warning("validation.warnings", entity=entity, warnings=report.warnings)
                self._tel.incr("phronexus.validation.warnings", entity=entity)
            report.raise_if_failed()

        sc = self._registry.active_storage(entity)
        doc_id = self._proj.compute_doc_id(sc, document)
        txn_id = uuid.uuid4().hex

        existing = self._store.get(sc.manifest_set, doc_id)
        prev_committed = existing is not None and existing.bins.get(M_STATUS) == STATUS_COMMITTED
        if sc.update_policy == UpdatePolicy.insert_only and prev_committed:
            raise DocumentAlreadyExists(f"{entity}/{doc_id} already exists")

        old_data = self._read_canonical_data(existing, sc)
        ts = time.time()
        # Stamp the same write time into every projection envelope (_ts) and the
        # manifest, so the reaper can tell a young in-flight write from an orphan.
        records = self._proj.build(sc, document, doc_id=doc_id, txn_id=txn_id, ts=ts)
        canonical = next(r for r in records if r.projection.canonical)

        manifest_bins = {
            M_STATUS: STATUS_COMMITTED,
            M_TXN: txn_id,
            M_CVER: sc.version,
            M_DOC_ID: doc_id,
            M_CANONICAL: {"set": canonical.projection.set, "key": canonical.key},
            M_PROJECTIONS: [{"set": r.projection.set, "key": r.key} for r in records],
            M_TS: ts,
            M_PREV_TXN: existing.bins.get(M_TXN) if existing else None,
        }
        expected_gen = existing.generation if existing else 0

        # Order matters when there is no true transaction (e.g. Aerospike
        # Community Edition): write everything the manifest will reference FIRST,
        # then the manifest LAST as the commit point. Under a real multi-record
        # transaction the order is irrelevant (all-or-nothing); under sequential
        # writes it guarantees that a visible manifest implies its projections,
        # index entries, and change-feed event are already durable.
        for r in records:
            self._store.put(r.projection.set, r.key, r.bins, ttl=r.ttl, txn=txn)
        # Inverted-index update — atomic with the manifest (no missed-index window).
        if self._index_in_txn:
            self._stage_reindex(entity, doc_id, old_data, document, txn)
        # Durable change-feed event (transactional outbox) — retention can't miss it.
        event = CommitEvent(
            entity=entity, doc_id=doc_id, txn_id=txn_id, contract_version=sc.version,
            op="upsert", ts=ts, document=dict(document), version=expected_gen + 1,
        )
        # Wrap in one 'ev' bin — Aerospike bin names are capped at 15 chars, and
        # the event has a "contract_version" field; map keys have no such limit.
        self._store.put(self._changefeed_set, f"{doc_id}:{txn_id}", {"ev": event.to_dict()}, txn=txn)
        # Caller-supplied records (e.g. state-machine output events) go here, still
        # BEFORE the manifest, so a committed manifest implies they are durable.
        if stage_extra is not None:
            stage_extra(txn)
        # Manifest LAST — the commit point.
        self._store.put(
            sc.manifest_set, doc_id, manifest_bins, expected_generation=expected_gen, txn=txn
        )
        return StagedWrite(
            entity=entity, doc_id=doc_id, txn_id=txn_id, sc=sc,
            records=records, old_data=old_data, existing=existing, ts=ts,
        )

    def post_write(self, staged: "StagedWrite", document: dict[str, Any], *, relay: bool = True) -> None:
        """Post-commit side effects for a staged write."""
        self._tel.incr("phronexus.writes", entity=staged.entity)
        if not self._index_in_txn:
            self._reindex(staged.entity, staged.doc_id, staged.old_data, document)
        self._reap_superseded(staged.existing, staged.records, staged.sc)
        # The change-feed event is already durably staged; relay it to the sink.
        # Bulk callers pass relay=False and drain once at the end.
        if relay and self._inline_changefeed:
            self.drain_changefeed()
        log.info("document.committed", entity=staged.entity, doc_id=staged.doc_id, txn=staged.txn_id)

    def drain_changefeed(self, max_events: int = 1000, *, now: Optional[float] = None) -> int:
        """Relay durably-staged change-feed events to the sink. Idempotent-safe.

        When ``verify_commit`` is on, an event whose document did not actually
        commit (a phantom from a non-transactional crash) is not relayed; once it
        is older than the grace period it is reaped from the outbox. ``now`` is
        injectable for deterministic tests.
        """
        now = time.time() if now is None else now
        published = 0
        # Relay in per-document version order so a downstream consumer keyed by
        # doc_id sees a document's versions monotonically (the scan itself is
        # unordered). Cross-document order is irrelevant — retention reconciles.
        rows = [(key, CommitEvent(**rec.bins["ev"]))
                for key, rec in self._store.scan(self._changefeed_set)]
        rows.sort(key=lambda ke: (ke[1].doc_id, ke[1].version))
        for key, event in rows:
            if self._verify_commit and not self._write_committed(event):
                # Not (yet) committed: leave it in case it commits shortly, but
                # reap it once it is clearly an orphan (older than the grace).
                if (now - (event.ts or 0.0)) >= self._orphan_grace_seconds:
                    self._store.remove(self._changefeed_set, key)
                    log.info("changefeed.reaped_orphan", key=key)
                continue
            try:
                self._sink.emit(event)
            except Exception:  # noqa: BLE001 - leave for retry
                log.warning("changefeed.emit_failed", key=key)
                continue
            self._store.remove(self._changefeed_set, key)
            published += 1
            if published >= max_events:
                break
        return published

    def _write_committed(self, event: CommitEvent) -> bool:
        """Did ``event``'s write actually commit? True if a manifest for the doc
        reflects this txn as its current or immediately-previous write."""
        try:
            sc = self._registry.active_storage(event.entity)
        except ContractNotFound:
            return True  # can't check -> don't suppress (fail open)
        m = self._store.get(sc.manifest_set, event.doc_id)
        if m is None:
            return False
        return event.txn_id in (m.bins.get(M_TXN), m.bins.get(M_PREV_TXN))

    # --- read -----------------------------------------------------------

    def read(self, entity: str, doc_id: str) -> Optional[dict[str, Any]]:
        with self._tel.span("manifest.read", entity=entity):
            sc = self._registry.active_storage(entity)
            manifest = self._store.get(sc.manifest_set, doc_id)
            if manifest is None or manifest.bins.get(M_STATUS) != STATUS_COMMITTED:
                return None
            canon = manifest.bins[M_CANONICAL]
            rec = self._store.get(canon["set"], canon["key"])
            if rec is None:
                return None
            # Snapshot check: the canonical key is PK-derived and stable, so with
            # atomic commits its txn always matches the manifest. A mismatch means
            # a non-transactional partial write — treat as not-yet-visible.
            if rec.bins.get(META_TXN) != manifest.bins.get(M_TXN):
                log.warning("manifest.txn_mismatch", entity=entity, doc_id=doc_id)
                return None
            self._tel.incr("phronexus.reads", entity=entity)
            return decode_record(sc.canonical_projection, rec.bins)

    def exists(self, entity: str, doc_id: str) -> bool:
        return self.read(entity, doc_id) is not None

    def read_many(self, entity: str, doc_ids) -> dict[str, dict[str, Any]]:
        """Batch-read many documents by id: one batch on the manifest set, then
        one batch per canonical set — 2 round-trips instead of 2N. Missing,
        uncommitted, or half-written docs are simply omitted. Pairs with the
        inverted index (``value -> [doc ids]``) to pull a whole set, e.g. all
        trades for a counterparty."""
        ids = list(dict.fromkeys(doc_ids))  # de-dupe, keep order
        if not ids:
            return {}
        with self._tel.span("manifest.read_many", entity=entity):
            sc = self._registry.active_storage(entity)
            manifests = self._store.batch_get(sc.manifest_set, ids)
            by_set: dict[str, list[str]] = {}
            meta: dict[str, tuple[str, str, Any]] = {}
            for doc_id, m in manifests.items():
                if m.bins.get(M_STATUS) != STATUS_COMMITTED:
                    continue
                canon = m.bins.get(M_CANONICAL)
                if not canon:
                    continue
                by_set.setdefault(canon["set"], []).append(canon["key"])
                meta[doc_id] = (canon["set"], canon["key"], m.bins.get(M_TXN))
            recs: dict[tuple[str, str], Any] = {}
            for set_name, keys in by_set.items():
                for k, r in self._store.batch_get(set_name, keys).items():
                    recs[(set_name, k)] = r
            out: dict[str, dict[str, Any]] = {}
            for doc_id, (cset, ckey, txn) in meta.items():
                rec = recs.get((cset, ckey))
                if rec is None or rec.bins.get(META_TXN) != txn:
                    continue  # missing or a non-transactional partial write
                out[doc_id] = decode_record(sc.canonical_projection, rec.bins)
            self._tel.incr("phronexus.reads", len(out), entity=entity)
            return out

    # --- delete ---------------------------------------------------------

    def delete(self, entity: str, doc_id: str) -> bool:
        with self._tel.span("manifest.delete", entity=entity):
            sc = self._registry.active_storage(entity)
            manifest = self._store.get(sc.manifest_set, doc_id)
            if manifest is None or manifest.bins.get(M_STATUS) != STATUS_COMMITTED:
                raise DocumentNotFound(f"{entity}/{doc_id} not found")

            old_data = self._read_canonical_data(manifest, sc)
            expected_gen = manifest.generation
            del_txn_id = uuid.uuid4().hex
            del_event = CommitEvent(
                entity=entity, doc_id=doc_id, txn_id=del_txn_id,
                contract_version=sc.version, op="delete", ts=time.time(), document=None,
                version=expected_gen + 1,  # tombstone supersedes the current version
            )

            if sc.delete_policy == DeletePolicy.soft:
                new_bins = dict(manifest.bins)
                new_bins[M_STATUS] = STATUS_DELETED
                new_bins[M_TS] = time.time()
                with self._store.transaction(native=sc.native_txn) as txn:
                    self._store.put(
                        sc.manifest_set, doc_id, new_bins,
                        expected_generation=expected_gen, txn=txn,
                    )
                    if self._index_in_txn:
                        self._stage_deindex(entity, doc_id, old_data, txn)
                    self._store.put(self._changefeed_set, f"{doc_id}:{del_txn_id}",
                                    {"ev": del_event.to_dict()}, txn=txn)
            else:  # hard delete: remove the document DATA, keep a delete tombstone
                # The projection records (the document bytes) are physically
                # removed, but a minimal manifest tombstone remains so that:
                #   - versions stay MONOTONIC across delete -> re-insert (a lower-
                #     versioned tombstone must never shadow a later re-insert in the
                #     retention view), and
                #   - the change feed keeps a delete marker and a re-delete is a
                #     clean DocumentNotFound.
                tomb = {
                    M_STATUS: STATUS_DELETED,
                    M_TXN: del_txn_id,
                    M_CVER: sc.version,
                    M_DOC_ID: doc_id,
                    M_CANONICAL: manifest.bins.get(M_CANONICAL),
                    M_PROJECTIONS: [],  # data is gone; nothing to reference/reap
                    M_TS: time.time(),
                    M_PREV_TXN: manifest.bins.get(M_TXN),
                }
                with self._store.transaction(native=sc.native_txn) as txn:
                    for p in manifest.bins.get(M_PROJECTIONS, []):
                        self._store.remove(p["set"], p["key"], txn=txn)
                    self._store.put(
                        sc.manifest_set, doc_id, tomb,
                        expected_generation=expected_gen, txn=txn,
                    )
                    if self._index_in_txn:
                        self._stage_deindex(entity, doc_id, old_data, txn)
                    self._store.put(self._changefeed_set, f"{doc_id}:{del_txn_id}",
                                    {"ev": del_event.to_dict()}, txn=txn)

            if not self._index_in_txn:
                self._deindex(entity, doc_id, old_data)
            self._tel.incr("phronexus.deletes", entity=entity)
            if self._inline_changefeed:
                self.drain_changefeed()
            log.info("document.deleted", entity=entity, doc_id=doc_id, policy=sc.delete_policy.value)
            return True

    # --- index maintenance ---------------------------------------------

    def _searchable(self, entity: str):
        try:
            return self._registry.active_query(entity).searchable
        except ContractNotFound:
            return []  # no query contract yet -> nothing to index

    def _stage_reindex(self, entity, doc_id, old_data, new_doc, txn) -> None:
        """Stage index add/removes into the write transaction (atomic)."""
        for sf in self._searchable(entity):
            old_v = old_data.get(sf.field, _MISSING)
            new_v = new_doc.get(sf.field, _MISSING)
            if old_v == new_v:
                continue
            log.debug("index.update", entity=entity, index=sf.index_name, field=sf.field, doc_id=doc_id)
            if old_v is not _MISSING:
                self._index.stage_remove(entity, sf.field, old_v, doc_id, txn=txn)
            if new_v is not _MISSING:
                self._index.stage_add(entity, sf.field, new_v, doc_id,
                                      numeric=sf.supports_range, txn=txn)

    def _stage_deindex(self, entity, doc_id, old_data, txn) -> None:
        for sf in self._searchable(entity):
            v = old_data.get(sf.field, _MISSING)
            if v is not _MISSING:
                self._index.stage_remove(entity, sf.field, v, doc_id, txn=txn)

    def _reindex(self, entity, doc_id, old_data, new_doc) -> None:
        for sf in self._searchable(entity):
            old_v = old_data.get(sf.field, _MISSING)
            new_v = new_doc.get(sf.field, _MISSING)
            if old_v == new_v:
                continue
            if old_v is not _MISSING:
                self._index.remove(entity, sf.field, old_v, doc_id)
            if new_v is not _MISSING:
                self._index.add(entity, sf.field, new_v, doc_id, numeric=sf.supports_range)

    def _deindex(self, entity, doc_id, old_data) -> None:
        for sf in self._searchable(entity):
            v = old_data.get(sf.field, _MISSING)
            if v is not _MISSING:
                self._index.remove(entity, sf.field, v, doc_id)

    # --- helpers --------------------------------------------------------

    def _read_canonical_data(self, manifest, sc: StorageContract) -> dict[str, Any]:
        if manifest is None:
            return {}
        canon = manifest.bins.get(M_CANONICAL)
        if not canon:
            return {}
        rec = self._store.get(canon["set"], canon["key"])
        return decode_record(sc.canonical_projection, rec.bins) if rec else {}

    def _reap_superseded(self, existing, records, sc: StorageContract) -> None:
        """Remove projection records no longer referenced after an update."""
        if existing is None:
            return
        new_keys = {(r.projection.set, r.key) for r in records}
        for p in existing.bins.get(M_PROJECTIONS, []):
            if (p["set"], p["key"]) not in new_keys:
                try:
                    self._store.remove(p["set"], p["key"])
                except Exception:  # noqa: BLE001 - best effort
                    log.warning("reap.superseded_failed", set=p["set"], key=p["key"])
