"""Contract backfill.

When a storage/query contract evolves (new projection, new searchable field),
existing documents were written under the old version and don't yet have the new
projections/index entries. Backfill re-projects every committed document through
the *active* contract: it reads each canonical document and re-writes it, which
regenerates projections and refreshes the inverted index. It is idempotent and
safe to re-run.

It is **restartable** (a per-entity checkpoint cursor resumes an interrupted run)
and **controllable**: the governance control plane can request pause / cancel and
the job honours it cooperatively between documents, persisting a ``state`` that is
fleet-visible. State record (in the backfill-state set, keyed by entity):
``{cursor, count, done, state, req_action, updated_ts, error}``.
"""

from __future__ import annotations

import time

import structlog

from phronexus.core import Phronexus
from phronexus.manifest.manager import M_STATUS, STATUS_COMMITTED

log = structlog.get_logger(__name__)


class BackfillJob:
    def __init__(self, px: Phronexus, *, state_set: str | None = None):
        self._px = px
        self._state_set = state_set or px.settings.governance.backfill_state_set

    # --- state ---------------------------------------------------------

    def _state(self, entity: str) -> dict:
        rec = self._px.store.get(self._state_set, entity)
        return dict(rec.bins) if rec else {}

    def _write(self, entity: str, **fields) -> None:
        cur = self._state(entity)
        cur.update(fields)
        cur["updated_ts"] = time.time()
        self._px.store.put(self._state_set, entity, cur)

    # --- run -----------------------------------------------------------

    def run(self, entity: str, *, dry_run: bool = False, resume: bool = True) -> int:
        sc = self._px.registry.active_storage(entity)
        st = self._state(entity)

        # Resume an *interrupted* run only; a completed one re-projects fully.
        cursor = None
        if resume and not dry_run and not st.get("done"):
            cursor = st.get("cursor")

        # Deterministic order so the checkpoint cursor is meaningful across runs.
        ids = sorted(
            doc_id for doc_id, rec in self._px.store.scan(sc.manifest_set)
            if rec.bins.get(M_STATUS) == STATUS_COMMITTED
        )
        if not dry_run:
            # Note: we do NOT clear req_action here, so a pause/cancel
            # requested before the run starts is still honoured on the first doc.
            self._write(entity, state="running", error=None)

        count = 0
        for doc_id in ids:
            if cursor is not None and doc_id <= cursor:
                continue  # already done in a prior (interrupted) run
            if not dry_run:
                # Cooperative control point: honour a pause/cancel request.
                action = self._state(entity).get("req_action")
                if action == "pause":
                    self._write(entity, state="paused", req_action=None)
                    log.info("backfill.paused", entity=entity, count=count)
                    return count
                if action == "cancel":
                    self._write(entity, state="cancelled", req_action=None)
                    log.info("backfill.cancelled", entity=entity, count=count)
                    return count
            doc = self._px.get(entity, doc_id)
            if doc is None:
                continue
            if not dry_run:
                self._px.put(entity, doc)  # re-project under the active contract
                self._write(entity, cursor=doc_id, count=count + 1, done=False, state="running")
            count += 1

        if not dry_run:
            self._write(entity, cursor=(ids[-1] if ids else cursor), count=count,
                        done=True, state="completed", req_action=None)
        log.info("backfill.done", entity=entity, documents=count, version=sc.version,
                 dry_run=dry_run, resumed=cursor is not None)
        return count
