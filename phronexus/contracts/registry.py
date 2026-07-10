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

import json
import threading
import time
from typing import Optional

import structlog

from phronexus.contracts.loader import Contract, parse_contract
from phronexus.contracts.models import (
    QueryContract,
    StorageContract,
    TransitionContract,
    ValidationContract,
    ViewContract,
)
from phronexus.errors import ContractNotFound
from phronexus.kv.base import KVStore

log = structlog.get_logger(__name__)


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

    def publish(self, contract: Contract, *, activate: bool = True) -> None:
        identity = contract.identity()
        payload = {"kind": contract.kind.value, "doc": contract.model_dump(mode="json")}
        self._store.put(self._set, identity, payload)
        if activate:
            view = getattr(contract, "view", None)
            self._store.put(
                self._set,
                _active_key(contract.kind.value, contract.entity, view),
                {"target": identity, "version": contract.version},
            )
        log.info("contract.published", identity=identity, activated=activate)
        self.refresh(force=True)

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
            for key, rec in self._store.scan(self._set):
                if key.startswith("active:"):
                    active[key] = rec.bins["target"]
                else:
                    doc = dict(rec.bins["doc"])
                    doc["kind"] = rec.bins["kind"]
                    by_identity[key] = parse_contract(doc)
            self._by_identity = by_identity
            self._active = active
            self._last_refresh = time.monotonic()
        log.debug("contract.cache.refreshed", contracts=len(by_identity), active=len(active))

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
