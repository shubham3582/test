"""Contract registry: versioned storage in the KV store + hot-reloading cache.

Contracts are persisted in the ``_contracts`` set (one record per version) plus
an "active pointer" record per (kind, entity[, view]). An in-process cache holds
parsed models and refreshes on a configurable cadence (default 300s), which is
what lets schema evolve without redeploying: publish a new version, flip the
pointer, and running apps pick it up on their next refresh.

Reads pin the *version that produced a document* (recorded in its manifest), so
older documents stay interpretable after the active pointer moves on.
"""

from __future__ import annotations

import threading
import time
from typing import Optional

import structlog

from phronexus.contracts.loader import Contract, parse_contract
from phronexus.contracts.models import (
    IngressContract,
    QueryContract,
    StorageContract,
    StreamContract,
    TransitionContract,
    ValidationContract,
    ViewContract,
)
from phronexus.errors import ContractNotFound, ContractValidationError, GenerationConflict
from phronexus.kv.base import KVStore

log = structlog.get_logger(__name__)

_PUBLISH_RETRIES = 5  # optimistic-concurrency retries for competing publishers


def _active_key(kind: str, entity: str, view: Optional[str] = None) -> str:
    return f"active:{kind}:{entity}" + (f":{view}" if view else "")


class ContractRegistry:
    def __init__(
        self,
        store: KVStore,
        contracts_set: str = "_contracts",
        refresh_seconds: int = 300,
        background_refresh: bool = False,
    ) -> None:
        self._store = store
        self._set = contracts_set
        self._refresh_seconds = refresh_seconds
        self._lock = threading.RLock()
        self._by_identity: dict[str, Contract] = {}
        self._active: dict[str, str] = {}  # active-key -> identity
        self._last_refresh = 0.0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.refresh(force=True)
        if background_refresh:
            self._start_background()

    # --- publishing -----------------------------------------------------

    def publish(self, contract: Contract, *, activate: bool = True, force: bool = False) -> None:
        identity = contract.identity()
        view = getattr(contract, "view", None)
        akey = _active_key(contract.kind.value, contract.entity, view)
        body = {"kind": contract.kind.value, "doc": contract.model_dump(mode="json")}

        # Concurrent publishers on the same entity must be serializable: the body
        # and the active-pointer flip commit atomically (one transaction), both
        # generation-guarded. On a CAS conflict we refresh and retry, so the
        # compatibility gate is re-evaluated against the latest active version
        # (closing the check-then-act TOCTOU) rather than a stale snapshot.
        for attempt in range(_PUBLISH_RETRIES + 1):
            # Compatibility gate: reject a storage change that would break
            # addressing of existing documents (PK, manifest set, canonical
            # projection). force=True overrides intentionally.
            if isinstance(contract, StorageContract) and not force:
                self._check_storage_compatible(contract)
            try:
                with self._store.transaction() as txn:
                    existing = self._store.get(self._set, identity)
                    if existing is not None and not force:
                        # Same identity already published: idempotent if the body
                        # is byte-identical, a hard error if it diverges (never a
                        # silent overwrite of a different contract at this version).
                        if existing.bins.get("doc") != body["doc"]:
                            raise ContractValidationError(
                                f"{identity} already published with different content; "
                                f"publish a new version or use force=True to overwrite"
                            )
                    else:
                        self._store.put(
                            self._set, identity, body,
                            expected_generation=(existing.generation if existing else 0),
                            txn=txn,
                        )
                    if activate:
                        ptr = self._store.get(self._set, akey)
                        self._store.put(
                            self._set, akey,
                            {"target": identity, "version": contract.version},
                            expected_generation=(ptr.generation if ptr else 0),
                            txn=txn,
                        )
                break
            except GenerationConflict:
                if attempt >= _PUBLISH_RETRIES:
                    raise
                self.refresh(force=True)  # re-read latest state, then retry
        log.info("contract.published", identity=identity, activated=activate)
        self.refresh(force=True)

    def activate(self, identity: str) -> Contract:
        """Flip the active pointer to an already-stored version (rollback/forward).

        Unlike :meth:`publish`, this writes no contract body and runs no
        compatibility gate — the version was validated when first published, so
        pointing back to it is always safe. Raises ``ContractNotFound`` if the
        version isn't in the store.
        """
        c = self.get_version(identity)
        view = getattr(c, "view", None)
        akey = _active_key(c.kind.value, c.entity, view)
        # Generation-guard the pointer flip so a concurrent activate/publish can't
        # silently lose it (last-writer-wins on the earlier version).
        for attempt in range(_PUBLISH_RETRIES + 1):
            try:
                ptr = self._store.get(self._set, akey)
                self._store.put(
                    self._set, akey, {"target": identity, "version": c.version},
                    expected_generation=(ptr.generation if ptr else 0),
                )
                break
            except GenerationConflict:
                if attempt >= _PUBLISH_RETRIES:
                    raise
        log.info("contract.activated", identity=identity)
        self.refresh(force=True)
        return c

    def compat_report(self, new: StorageContract) -> dict:
        """Explain what a proposed storage contract changes vs. the active version.

        Returns ``{compatible, first_version, changes:[{field, from, to, breaking,
        reason}]}``. A change is *breaking* when it would strand existing documents
        (re-addresses their primary key, manifest set, or canonical location).
        """
        try:
            prev = self.active_storage(new.entity)
        except ContractNotFound:
            return {"compatible": True, "first_version": True, "changes": []}

        changes: list[dict] = []

        def check(field, a, b, *, breaking, reason):
            if a != b:
                changes.append({"field": field, "from": a, "to": b,
                                "breaking": breaking, "reason": reason})

        check("primary_key", prev.primary_key, new.primary_key, breaking=True,
              reason="changing the primary key re-addresses every existing document")
        check("manifest_set", prev.manifest_set, new.manifest_set, breaking=True,
              reason="moving the manifest set strands existing documents")
        pc, nc = prev.canonical_projection, new.canonical_projection
        check("canonical_projection", f"{pc.set}/{pc.key}", f"{nc.set}/{nc.key}",
              breaking=True, reason="the canonical record location changed")
        # Informational (non-breaking): added projections / searchable coverage.
        check("projection_count", len(prev.projections), len(new.projections),
              breaking=False, reason="projections added/removed (run a backfill to materialise)")
        check("version", prev.version, new.version, breaking=False,
              reason="new contract version")

        breaking = any(c["breaking"] for c in changes)
        return {"compatible": not breaking, "first_version": False, "changes": changes}

    def diff(self, identity_a: str, identity_b: str) -> dict:
        """Field-level diff between two stored contract versions."""
        da = self.get_version(identity_a).model_dump(mode="json")
        db = self.get_version(identity_b).model_dump(mode="json")
        changes = [
            {"field": k, "from": da.get(k), "to": db.get(k)}
            for k in sorted(set(da) | set(db)) if da.get(k) != db.get(k)
        ]
        return {"a": identity_a, "b": identity_b, "changes": changes}

    def _check_storage_compatible(self, new: StorageContract) -> None:
        """Reject an evolution that would strand existing documents."""
        report = self.compat_report(new)
        if not report["compatible"]:
            problems = "; ".join(
                f"{c['field']} {c['from']!r} -> {c['to']!r}"
                for c in report["changes"] if c["breaking"]
            )
            raise ContractValidationError(
                f"incompatible storage change for {new.entity!r} (would strand existing "
                f"documents): {problems}. Publish with force=True to override."
            )

    # --- lookups (served from cache) ------------------------------------

    def active_storage(self, entity: str) -> StorageContract:
        return self._active_of("storage", entity)  # type: ignore[return-value]

    def active_query(self, entity: str) -> QueryContract:
        return self._active_of("query", entity)  # type: ignore[return-value]

    def active_view(self, entity: str, view: str) -> ViewContract:
        return self._active_of("view", entity, view)  # type: ignore[return-value]

    def active_transition(self, entity: str) -> TransitionContract:
        return self._active_of("transition", entity)  # type: ignore[return-value]

    def active_validation(self, entity: str) -> ValidationContract:
        return self._active_of("validation", entity)  # type: ignore[return-value]

    def active_stream(self, entity: str) -> StreamContract:
        return self._active_of("stream", entity)  # type: ignore[return-value]

    def active_ingress(self, entity: str) -> IngressContract:
        return self._active_of("ingress", entity)  # type: ignore[return-value]

    def get_version(self, identity: str) -> Contract:
        self._maybe_refresh()
        with self._lock:
            c = self._by_identity.get(identity)
        if c is None:
            # A pinned old version may have aged out of cache — reload it directly.
            rec = self._store.get(self._set, identity)
            if rec is None:
                raise ContractNotFound(f"contract {identity!r} not found")
            c = parse_contract(rec.bins["doc"] | {"kind": rec.bins["kind"]})
            with self._lock:
                self._by_identity[identity] = c
        return c

    def _active_of(self, kind: str, entity: str, view: Optional[str] = None) -> Contract:
        self._maybe_refresh()
        akey = _active_key(kind, entity, view)
        with self._lock:
            identity = self._active.get(akey)
            if identity is not None:
                c = self._by_identity.get(identity)
                if c is not None:
                    return c
        raise ContractNotFound(
            f"no active {kind} contract for entity={entity!r}"
            + (f" view={view!r}" if view else "")
        )

    # --- refresh --------------------------------------------------------

    def _maybe_refresh(self) -> None:
        if time.monotonic() - self._last_refresh >= self._refresh_seconds:
            self.refresh()

    def refresh(self, *, force: bool = False) -> None:
        with self._lock:
            if not force and time.monotonic() - self._last_refresh < self._refresh_seconds:
                return
            by_identity: dict[str, Contract] = {}
            active: dict[str, str] = {}
            skipped = 0
            for key, rec in self._store.scan(self._set):
                if key.startswith("active:"):
                    active[key] = rec.bins["target"]
                else:
                    doc = dict(rec.bins["doc"])
                    doc["kind"] = rec.bins["kind"]
                    # Isolate a single bad/forward-incompatible contract: skip it
                    # with a warning instead of failing the whole refresh (which
                    # would blank the registry and hide every other contract —
                    # e.g. an old binary reading a contract written by a newer one).
                    try:
                        by_identity[key] = parse_contract(doc)
                    except Exception:  # noqa: BLE001 - one bad record must not nuke the cache
                        skipped += 1
                        log.warning("contract.cache.skip_unparseable", identity=key)
            self._by_identity = by_identity
            self._active = active
            self._last_refresh = time.monotonic()
        log.debug("contract.cache.refreshed", contracts=len(by_identity),
                  active=len(active), skipped=skipped)

    def list_contracts(self) -> dict:
        """Enumerate contracts persisted in the store (the source of truth).

        Refreshes from the store first, so this reflects Aerospike, not just the
        local cache — use it to verify ingestion before removing source files.
        """
        self.refresh(force=True)
        with self._lock:
            return {
                "contracts": sorted(self._by_identity.keys()),
                "active": dict(sorted(self._active.items())),
            }

    def storage_contracts(self) -> list["StorageContract"]:
        """All known storage contracts (any version), deduped by identity."""
        with self._lock:
            seen: dict[str, "StorageContract"] = {}
            for c in self._by_identity.values():
                if isinstance(c, StorageContract):
                    seen[c.identity()] = c
            return list(seen.values())

    def _start_background(self) -> None:
        def _loop() -> None:
            while not self._stop.wait(self._refresh_seconds):
                try:
                    self.refresh(force=True)
                except Exception:  # noqa: BLE001 - never kill the refresher
                    log.exception("contract.cache.refresh_failed")

        self._thread = threading.Thread(target=_loop, name="phronexus-contract-refresh", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1)

    @property
    def cache_age_seconds(self) -> float:
        return time.monotonic() - self._last_refresh
