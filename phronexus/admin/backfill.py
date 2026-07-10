"""Contract backfill.

When a storage/query contract evolves (new projection, new searchable field),
existing documents were written under the old version and don't yet have the new
projections/index entries. Backfill re-projects every committed document through
the *active* contract: it reads each canonical document and re-writes it, which
regenerates projections and refreshes the inverted index. It is idempotent and
safe to re-run.
"""

from __future__ import annotations

import structlog

from phronexus.core import Phronexus
from phronexus.manifest.manager import M_STATUS, STATUS_COMMITTED

log = structlog.get_logger(__name__)


class BackfillJob:
    def __init__(self, px: Phronexus):
        self._px = px

    def run(self, entity: str, *, dry_run: bool = False) -> int:
        sc = self._px.registry.active_storage(entity)
        count = 0
        for doc_id, rec in list(self._px.store.scan(sc.manifest_set)):
            if rec.bins.get(M_STATUS) != STATUS_COMMITTED:
                continue
            doc = self._px.get(entity, doc_id)
            if doc is None:
                continue
            if not dry_run:
                self._px.put(entity, doc)  # re-project under the active contract
            count += 1
        log.info("backfill.done", entity=entity, documents=count, version=sc.version, dry_run=dry_run)
        return count
