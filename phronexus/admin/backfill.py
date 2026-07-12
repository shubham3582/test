"""Contract backfill.

When a storage/query contract evolves (new projection, new searchable field),
existing documents were written under the old version and don't yet have the new
projections/index entries. Backfill re-projects every committed document through
the *active* contract: it reads each canonical document and re-writes it, which
regenerates projections and refreshes the inverted index. It is idempotent and
safe to re-run.

It is also **restartable**: a checkpoint (the last completed doc id) is persisted
per entity, so a crash mid-backfill resumes from where it stopped instead of
rescanning from the top. Documents are processed in sorted id order so the cursor
is meaningful. A fully completed run re-projects everything again on the next
call (the checkpoint only resumes an interrupted run).
"""

from __future__ import annotations

import structlog

from phronexus.core import Phronexus
from phronexus.manifest.manager import M_STATUS, STATUS_COMMITTED

log = structlog.get_logger(__name__)


class BackfillJob:
    def __init__(self, px: Phronexus, *, state_set: str = "_backfill_state"):
        self._px = px
        self._state_set = state_set

    # --- checkpoint ----------------------------------------------------

    def _load_state(self, entity: str) -> tuple[str | None, bool]:
        rec = self._px.store.get(self._state_set, entity)
        if rec is None:
            return None, False
        return rec.bins.get("cursor"), bool(rec.bins.get("done"))

    def _save_state(self, entity: str, cursor: str | None, count: int, *, done: bool) -> None:
        self._px.store.put(self._state_set, entity,
                           {"cursor": cursor, "count": count, "done": done})

    # --- run -----------------------------------------------------------

    def run(self, entity: str, *, dry_run: bool = False, resume: bool = True) -> int:
        sc = self._px.registry.active_storage(entity)

        # Resume an *interrupted* run only; a completed one re-projects fully.
        cursor = None
        if resume and not dry_run:
            saved, done = self._load_state(entity)
            if not done:
                cursor = saved

        # Deterministic order so the checkpoint cursor is meaningful across runs.
        ids = sorted(
            doc_id for doc_id, rec in self._px.store.scan(sc.manifest_set)
            if rec.bins.get(M_STATUS) == STATUS_COMMITTED
        )

        count = 0
        for doc_id in ids:
            if cursor is not None and doc_id <= cursor:
                continue  # already done in a prior (interrupted) run
            doc = self._px.get(entity, doc_id)
            if doc is None:
                continue
            if not dry_run:
                self._px.put(entity, doc)  # re-project under the active contract
                self._save_state(entity, doc_id, count + 1, done=False)  # checkpoint
            count += 1

        if not dry_run:
            self._save_state(entity, ids[-1] if ids else cursor, count, done=True)
        log.info("backfill.done", entity=entity, documents=count, version=sc.version,
                 dry_run=dry_run, resumed=cursor is not None)
        return count
