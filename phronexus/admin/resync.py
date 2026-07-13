"""Date-scoped store resync between the hot (Aerospike) and cold (Iceberg) tiers.

An operator picks an ``entity``, a ``direction``, and a date window and the job
moves that slice between the two stores, materialising it under the **target's
active storage contract** (its projections/encodings hot-side, its Iceberg table
row shape cold-side — never an ad-hoc format):

  - ``cold-to-hot`` (rehydrate): read the entity's Iceberg rows for the window,
    decode the byte-faithful ``_raw`` document, and re-materialise it into
    Aerospike via :meth:`ManifestManager.restore` — a *silent, coords-preserving*
    write that reproduces the exact as-of view and never loops back to the cold
    tier.
  - ``hot-to-cold`` (re-land): read committed hot documents for the window and
    append them to the entity's Iceberg table via the normal retention row shape.
    Idempotent by ``(_doc_id, _txn)`` so re-runs collapse.

Dates are interpreted the way the entity is modelled: **valid-time** for
bitemporal entities (``valid_time_field``), **commit-time** otherwise.

Like :class:`~phronexus.admin.backfill.BackfillJob` the job is **restartable** (a
per-run checkpoint cursor) and **controllable** (the governance control plane can
request pause/cancel; honoured cooperatively between documents). Run state lives
in the resync-state set keyed by ``entity:direction:window`` so concurrent runs
in different directions/windows do not clobber each other.
"""

from __future__ import annotations

import time

import structlog

from phronexus.core import Phronexus
from phronexus.events.base import CommitEvent
from phronexus.retention.warehouse import build_warehouse, reconcile
from phronexus.retention.worker import RetentionWorker
from phronexus import codec

log = structlog.get_logger(__name__)

DIRECTIONS = ("cold-to-hot", "hot-to-cold")


class ResyncJob:
    def __init__(self, px: Phronexus, *, warehouse=None, worker=None, state_set: str | None = None):
        self._px = px
        self._state_set = state_set or px.settings.governance.resync_state_set
        # The warehouse defaults to whatever the deployment configured (in-memory
        # for demos/tests, a real Iceberg catalog in prod). A test can inject one.
        self._wh = warehouse if warehouse is not None else build_warehouse(px.settings.iceberg)
        self._worker = worker if worker is not None else RetentionWorker(px.registry, self._wh)

    # --- state (mirrors BackfillJob's fleet-visible checkpoint) ---------

    def _skey(self, entity: str, direction: str, window: str) -> str:
        return f"{entity}:{direction}:{window}"

    def _state(self, skey: str) -> dict:
        rec = self._px.store.get(self._state_set, skey)
        return dict(rec.bins) if rec else {}

    def _write(self, skey: str, **fields) -> None:
        cur = self._state(skey)
        cur.update(fields)
        cur["updated_ts"] = time.time()
        self._px.store.put(self._state_set, skey, cur)

    # --- run ------------------------------------------------------------

    def run(self, entity: str, *, direction: str, date_from: int | None = None,
            date_to: int | None = None, dry_run: bool = False, resume: bool = True,
            overwrite: bool = False) -> int:
        if direction not in DIRECTIONS:
            raise ValueError(f"unknown direction {direction!r}; expected one of {DIRECTIONS}")
        window = f"{date_from or '-'}..{date_to or '-'}"
        skey = self._skey(entity, direction, window)

        st = self._state(skey)
        cursor = None
        if resume and not dry_run and not st.get("done"):
            cursor = st.get("cursor")
        if not dry_run:
            self._write(skey, entity=entity, direction=direction, window=window,
                        state="running", error=None)

        try:
            if direction == "cold-to-hot":
                count, last = self._cold_to_hot(entity, date_from, date_to, cursor,
                                                skey, dry_run, overwrite)
            else:
                count, last = self._hot_to_cold(entity, date_from, date_to, cursor,
                                                skey, dry_run)
        except _Controlled as ctl:
            return ctl.count  # state already persisted at the control point

        if not dry_run:
            self._write(skey, cursor=last, count=count, done=True,
                        state="completed", req_action=None)
        log.info("resync.done", entity=entity, direction=direction, window=window,
                 units=count, dry_run=dry_run, resumed=cursor is not None)
        return count

    # --- cold -> hot (rehydrate from Iceberg) ---------------------------

    def _cold_to_hot(self, entity, date_from, date_to, cursor, skey, dry_run, overwrite):
        sc = self._px.registry.active_storage(entity)
        table = sc.iceberg.table
        if not table:
            raise ValueError(f"{entity} has no iceberg.table configured — nothing to rehydrate from")
        rows = [r for r in self._wh.scan(table) if r.get("_op") != "delete" and r.get("_raw") is not None]
        bitemporal = sc.temporal == "bitemporal"

        if bitemporal:
            vfield = sc.valid_time_field
            selected = [r for r in rows if _row_in_window(r, vfield, date_from, date_to)]
        else:
            # Latest-wins target: reconcile the window to the current row per doc.
            lo, hi = _ts_bounds(date_from, date_to)
            selected = reconcile([r for r in rows if _ts_in(r.get("_ts"), lo, hi)])

        # Deterministic order so the checkpoint cursor is meaningful across runs.
        selected.sort(key=lambda r: (str(r.get("_doc_id")), r.get("_version", 0), str(r.get("_txn"))))
        count = 0
        last = cursor
        for r in selected:
            ckey = f"{r.get('_doc_id')}|{r.get('_txn')}"
            if cursor is not None and ckey <= cursor:
                continue
            last = ckey
            if dry_run:
                count += 1  # dry-run reports candidate units in the window
                continue
            self._checkpoint(skey, count)
            doc = dict(codec.unpack(r["_raw"]))
            wrote = self._px.manifest.restore(
                entity, doc, txn_id=str(r.get("_txn")), tx_from=r.get("_ts"),
                valid_from=(int(r[vfield]) if bitemporal and r.get(vfield) is not None else None),
                overwrite=overwrite,
            )
            # Count (and checkpoint) only genuine writes, so a re-run over an
            # already-restored window reports 0 rather than re-attesting old rows.
            if wrote is not None:
                count += 1
                self._write(skey, cursor=ckey, count=count, done=False, state="running")
        return count, last

    # --- hot -> cold (re-land into Iceberg) -----------------------------

    def _hot_to_cold(self, entity, date_from, date_to, cursor, skey, dry_run):
        sc = self._px.registry.active_storage(entity)
        if not (sc.iceberg.enabled and sc.iceberg.table):
            raise ValueError(f"{entity} has no enabled iceberg table — nothing to re-land into")
        units = list(self._px.manifest.resync_units(entity, date_from=date_from, date_to=date_to))
        units.sort(key=lambda u: u.key())
        count = 0
        last = cursor
        for u in units:
            if cursor is not None and u.key() <= cursor:
                continue
            if not dry_run:
                self._checkpoint(skey, count)
                event = CommitEvent(
                    entity=entity, doc_id=u.doc_id, txn_id=u.txn_id,
                    contract_version=u.contract_version, op=u.op,
                    ts=u.tx_from or 0.0, document=u.document, version=u.version,
                )
                self._worker.process([event])
                self._write(skey, cursor=u.key(), count=count + 1, done=False, state="running")
            last = u.key()
            count += 1
        if not dry_run:
            self._flush()
        return count, last

    # --- helpers --------------------------------------------------------

    def _checkpoint(self, skey: str, count: int) -> None:
        """Cooperative pause/cancel control point (honoured between units)."""
        action = self._state(skey).get("req_action")
        if action == "pause":
            self._write(skey, state="paused", req_action=None)
            log.info("resync.paused", skey=skey, count=count)
            raise _Controlled(count)
        if action == "cancel":
            self._write(skey, state="cancelled", req_action=None)
            log.info("resync.cancelled", skey=skey, count=count)
            raise _Controlled(count)

    def _flush(self) -> None:
        flush = getattr(self._wh, "flush", None)
        if callable(flush):
            flush()


class _Controlled(Exception):
    """Internal signal: a pause/cancel was honoured; ``count`` units were done."""

    def __init__(self, count: int):
        super().__init__("resync controlled")
        self.count = count


# --- row-window predicates (cold side) ---------------------------------

def _row_in_window(row: dict, vfield: str | None, lo: int | None, hi: int | None) -> bool:
    if vfield is None:
        return True
    v = row.get(vfield)
    if v is None:
        return False
    v = int(v)
    if lo is not None and v < int(lo):
        return False
    if hi is not None and v > int(hi):
        return False
    return True


def _ts_bounds(date_from: int | None, date_to: int | None):
    import datetime as _dt

    def _start(d: int) -> float:
        day = _dt.datetime.strptime(str(int(d)), "%Y%m%d").replace(tzinfo=_dt.timezone.utc)
        return day.timestamp()

    lo = _start(date_from) if date_from is not None else None
    hi = (_start(date_to) + 86400.0) if date_to is not None else None
    return lo, hi


def _ts_in(ts, lo, hi) -> bool:
    if ts is None:
        return False
    if lo is not None and ts < lo:
        return False
    if hi is not None and ts >= hi:
        return False
    return True
