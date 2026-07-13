# Store resync — rebuilding hot from cold (and cold from hot)

Phronexus keeps a **lossless full copy of every document on both tiers**: the
canonical record in the hot store (Aerospike) and the byte-faithful `_raw`
msgpack blob in the cold store (Iceberg). Because neither tier is lossy relative
to the other, a date-bounded slice of an entity can be rebuilt in **either
direction** with `ResyncJob` (CLI `phronexus resync`, REST `POST /admin/resync`).

Use it to:

- **rehydrate** the hot store after a loss / cluster rebuild / new region, from
  the retention lake (`cold-to-hot`);
- **re-land** a historical window into Iceberg — e.g. seed a newly
  Iceberg-enabled entity, or repair a gap left by a stopped retention worker
  (`hot-to-cold`).

> Resync **moves committed documents between the two stores**. It is not a
> transform: data is always materialised under the **target's active storage
> contract** (see [Format](#format)). For re-projecting existing hot documents
> under an evolved contract, use [backfill](governance.md#8-backfill-status--intervention) instead.

---

## At a glance

| | `cold-to-hot` (rehydrate) | `hot-to-cold` (re-land) |
|---|---|---|
| **Source** | Iceberg table (`_raw` blob) | committed hot documents |
| **Target** | Aerospike (projections + manifest + index) | Iceberg table (retention rows) |
| **Write path** | `ManifestManager.restore` — **silent, coords-preserving** | `RetentionWorker` — normal retention row |
| **Idempotent by** | original `txn` (skips already-present) | `(_doc_id, _txn)` (rows dedup) |
| **Change feed** | **not** emitted (no loop back to cold) | n/a |
| **Non-destructive** | yes — skips a live hot doc unless `--overwrite` | yes — append-only log |

---

## Date scoping

"Specific dates" are interpreted the way the entity is modelled:

- **Bitemporal entities** (`temporal: bitemporal`, `valid_time_field`) window on
  **valid-time** — the business date (COB). Every stored version whose
  `valid_time_field` falls in `[from, to]` is in scope, so a window can span
  several immutable versions of the same document.
- **Non-bitemporal entities** window on **commit-time** (`_ts`), taken as UTC
  days; the upper bound is inclusive of the whole day. For `cold-to-hot` the
  window is reconciled to the current row per document (latest-wins).

Bounds are `YYYYMMDD` integers and either may be omitted for an open-ended range.

```bash
phronexus resync ns_exposure --direction cold-to-hot --from 20260101 --to 20260131
phronexus resync ns_exposure --direction hot-to-cold                 # whole entity
```

---

## Format

There is **no ad-hoc format**. Data is always materialised under the target
tier's **active storage contract**:

- **hot side** — the contract's declared projections and encodings
  (`map` / `msgpack` / `bins` / `spread`), key templates, and inverted index are
  all regenerated exactly as a normal write would produce them
  ([storage-layouts.md](storage-layouts.md));
- **cold side** — the entity's Iceberg table row shape: the document flattened
  into typed columns plus the `_raw` blob and `_version` / `_op` / `_txn` /
  `_expire_at` metadata ([retention-and-journals.md](retention-and-journals.md)).

This keeps reads correct on both sides — a rehydrated document is
indistinguishable from one written through `put`, and a re-landed row is
indistinguishable from one the retention worker would have produced.

---

## Fidelity: `cold-to-hot` is a silent, coords-preserving restore

A rehydrate is **not** a fresh write. `restore` reconstructs the document from
the Iceberg `_raw` blob and re-writes it while:

- **preserving the original coordinates** — the version's `valid_from`,
  transaction-time (`tx_from`), and write identity (`txn`) are reused, so an
  as-of read returns the *identical* version it did before the loss;
- **staging no change-feed event** — nothing is emitted downstream, so a
  rehydrate never loops back into the cold tier and never mints a new version.

It is **idempotent by `txn`**: re-running over an already-restored window writes
nothing (reports `0`). And it is **non-destructive** — if a live hot document
already exists for a doc id it is left untouched unless you pass `--overwrite`.

> `cold-to-hot` restores document *content* and its temporal coordinates. It does
> not replay delete tombstones (rows with `_op = delete` carry no payload and are
> skipped), and version identities are preserved rather than re-minted — so an
> as-of read is byte-identical, though it is a state restore, not a bit-for-bit
> journal replay.

---

## Operations

Modelled on [backfill](governance.md#8-backfill-status--intervention): a resync run is **restartable**
and **controllable**.

- **Checkpointed / restartable** — a per-run cursor in the resync-state set
  (`_resync_state`) resumes an interrupted run instead of rescanning from the
  top. Runs are keyed by `entity:direction:window`, so a `cold-to-hot` and a
  `hot-to-cold` (or two different windows) never clobber each other's progress.
- **Pause / cancel** — the governance control plane can request `pause` or
  `cancel`; the running job honours it cooperatively between documents.
- **Fleet-visible** — run state shows up in `governance.resync_status()` and in
  `fleet()`.
- **Dry-run** — `--dry-run` reports the candidate unit count for the window
  without writing anything.

```python
from phronexus.admin import ResyncJob

n = ResyncJob(px).run(
    "ns_exposure", direction="cold-to-hot",
    date_from=20260101, date_to=20260131,   # valid-time window (bitemporal)
    overwrite=False, dry_run=False,
)
```

Control a run through governance:

```python
px.governance.resync_status()                       # all runs (fleet view)
px.governance.control_resync(actor, run_key, "pause")   # pause | resume | cancel | reset
```

---

## Interfaces

### CLI

```bash
phronexus resync <entity> --direction {cold-to-hot|hot-to-cold} \
    [--from YYYYMMDD] [--to YYYYMMDD] [--overwrite] [--dry-run]
```

Backend + auth come from the environment (`PHRONEXUS_*` / `.env`), same as every
other component — so the process must be configured for **both** tiers
(`PHRONEXUS_BACKEND=aerospike` and `PHRONEXUS_ICEBERG__*`).

### REST — permission `resync:control`

| Method & path | Purpose |
|---|---|
| `POST /admin/resync` | run a resync (`entity`, `direction`, `date_from?`, `date_to?`, `overwrite?`, `dry_run?`) |
| `GET /admin/resync/status[?run_key=]` | fleet-visible run state |
| `POST /admin/resync/control` | `{run_key, action}` — pause / resume / cancel / reset |

Grant `resync:control` to the operator role in `api.auth.roles` (admin has it via
`*`). Every call is recorded in the hash-chained governance history.

> The **API server process must itself be configured for both tiers** —
> `PHRONEXUS_BACKEND=aerospike` **and** `PHRONEXUS_ICEBERG__*` — since the resync
> runs in-process. Without the Iceberg env a resync falls back to an in-memory
> warehouse (useful only for a dry run). The reference `deploy/docker-compose.yml`
> wires this on `phronexus-api` under the `iceberg` profile.

### Management UI

The self-contained console at `/ui` has a **Resync** tab (visible to principals
with `resync:control`): pick an entity, direction, and optional date window, run
it (with dry-run / overwrite toggles), and watch the fleet-visible run table with
pause / resume / cancel / reset controls.

---

## Worked example (Aerospike + Iceberg)

Bitemporal `ns_exposure` (`valid_time_field: cob`), two business dates:

```bash
# hot -> cold: land the whole entity into Iceberg
$ phronexus resync ns_exposure --direction hot-to-cold
{"resynced": 3, "direction": "hot-to-cold", ...}

# ... hot store is lost / rebuilt ...

# cold -> hot: rehydrate from Iceberg (silent, coords-preserving)
$ phronexus resync ns_exposure --direction cold-to-hot
{"resynced": 3, "direction": "cold-to-hot", ...}

# as-of reads return the exact pre-loss versions:
#   NS-1 as_of 20260101 -> 1,000,000
#   NS-1 as_of 20260102 -> 1,500,000

# re-running either direction is idempotent:
$ phronexus resync ns_exposure --direction cold-to-hot
{"resynced": 0, ...}                 # every txn already present
```

For a live deployment (MinIO + Iceberg REST catalog + Aerospike), configure the
resync process with both tiers' env — see
[deployment.md](deployment.md) and the `iceberg` profile in
`deploy/docker-compose.yml`.

---

## Related

- [retention-and-journals.md](retention-and-journals.md) — the cold tier: the
  insert-only log, the `_raw` blob, compaction.
- [bitemporal.md](bitemporal.md) — valid-time / transaction-time and as-of reads.
- [governance.md](governance.md) — backfill, RBAC, hash-chained history.
- [storage-layouts.md](storage-layouts.md) — the hot-side encodings a rehydrate
  regenerates.
