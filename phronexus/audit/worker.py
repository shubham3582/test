"""The audit worker: change feed -> per-document trace records.

Consumes the same committed-document change feed the retention worker uses, and
writes one immutable audit record per commit/delete via :class:`AuditLog`. It
resolves each entity's transition contract to know which field is the saga state,
so the trace can show state at each version (transitions are derived on read).

It runs two ways, sharing this exact logic:
  * **production** — a standalone process (:mod:`phronexus.audit.main`) consuming
    Kafka, next to (not inside) the app; never touches the hot write path.
  * **dev / no services** — inline, via :class:`AuditingSink`, which tees every
    in-process change-feed emit into the worker so a trace exists with zero infra.
"""

from __future__ import annotations

import structlog

from phronexus.audit.log import AuditLog
from phronexus.contracts.registry import ContractRegistry
from phronexus.errors import ContractNotFound
from phronexus.events.base import CommitEvent, EventSink

log = structlog.get_logger("phronexus.audit")


class AuditWorker:
    def __init__(self, audit: AuditLog, registry: ContractRegistry):
        self._audit = audit
        self._registry = registry
        self.stats = {"commit": 0, "delete": 0}

    def _state_field(self, entity: str):
        """The saga state field for an entity, if it has a transition contract."""
        try:
            return self._registry.active_transition(entity).state_field
        except ContractNotFound:
            return None

    def process(self, events: list[CommitEvent]) -> dict[str, int]:
        for ev in events:
            kind = "delete" if ev.op == "delete" else "commit"
            fields = {
                "txn_id": ev.txn_id,
                "version": ev.version,
                "cver": ev.contract_version,
                "ts": ev.ts,
            }
            if ev.document is not None:
                sf = self._state_field(ev.entity)
                if sf is not None and sf in ev.document:
                    fields["state"] = ev.document.get(sf)
            self._audit.record(entity=ev.entity, doc_id=ev.doc_id, kind=kind, **fields)
            self.stats[kind] = self.stats.get(kind, 0) + 1
        return dict(self.stats)


class AuditingSink(EventSink):
    """Wraps a change-feed sink so every emit is also fed to an inline
    :class:`AuditWorker`. Used only for the in-process (no-Kafka) dev mode; with
    Kafka, the standalone worker owns this instead. Auditing is best-effort and
    never breaks the emit it observes."""

    def __init__(self, inner: EventSink, worker: AuditWorker):
        self._inner = inner
        self._worker = worker

    def emit(self, event: CommitEvent) -> None:
        self._inner.emit(event)
        try:
            self._worker.process([event])
        except Exception:  # noqa: BLE001 - auditing must not fail the write path
            log.warning("audit.inline_failed", entity=event.entity, doc_id=event.doc_id)

    def __getattr__(self, name):  # proxy .drain()/.events/etc. to the wrapped sink
        return getattr(self._inner, name)
