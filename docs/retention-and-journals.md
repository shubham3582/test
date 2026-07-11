# Insert-only retention + binary (msgpack) journals

Two related storage conventions:

1. **The retention lake is an insert-only, idempotent event log** — current state
   is a *derived view*, so replays after a crash reconcile automatically.
2. **Full payloads are stored as msgpack blobs** — a few typed metadata bins for
   lookup plus one bin holding the complete message, decoded byte-faithfully on
   read. This backs both the retention rows and the message / request-response
   journals.

## The envelope convention

Every record follows the same shape:

```
┌───────────────── one stored record ─────────────────┐
│  metadata bins (small, typed, queryable)             │
│    entity, type, status, ts, _doc_id, _version, …    │
│  one blob bin (the full payload, msgpack)            │
│    raw  /  req + resp                                 │
└──────────────────────────────────────────────────────┘
```

`phronexus/codec.py` is the only place that knows the wire format
(`pack`/`unpack`), so it can be wrapped with compression or encryption without
touching callers. msgpack is compact, fast, and cross-language — any consumer can
read the blob with a standard library.

## Insert-only retention log

Each commit becomes **one immutable row**; nothing is mutated in place:

| column | meaning |
|---|---|
| `_doc_id` | the document id |
| `_txn` | **idempotency key** — a replay carries the same value |
| `_version` | **monotonic order** (the manifest generation) — “latest wins” |
| `_op` | `upsert` or `delete` (a tombstone) |
| `_ts`, `_cver`, `_expire_at` | commit time, contract version, retention horizon |
| `_raw` | the exact document as a msgpack blob |

**Current state is a view, not a table.** `reconcile()` keeps the max-`_version`
row per `_doc_id` and drops tombstones — the same as:

```sql
SELECT * FROM (
  SELECT *, row_number() OVER (PARTITION BY _doc_id ORDER BY _version DESC) rn
  FROM warehouse.trades)
WHERE rn = 1 AND _op <> 'delete'
```

```python
from phronexus.retention import reconcile, decode
current = reconcile(warehouse.scan("warehouse.trades"))   # latest per doc, no tombstones
doc     = decode(current[0])                              # exact original from the _raw blob
```

### Why this makes retention self-reconciling

- **Idempotent by construction.** Rows are keyed by `_txn`; appending the same
  event twice is a no-op (in-memory) or produces byte-identical rows the view
  ignores (Iceberg). So the pipeline only needs *at-least-once* delivery to be
  correct — exactly-once falls out of the read.
- **No lost writes.** The change feed is staged into the write transaction
  (durable `_cf_outbox`) and the retention consumer uses **manual offset commit**:
  offsets advance only after the batch is flushed to Iceberg. A crash mid-batch
  replays rather than drops.
- **Deletes never mutate.** A delete is a tombstone row; the view hides the doc,
  and a scheduled compaction physically drops tombstoned/expired rows and dedups
  by `_txn` so the log doesn’t grow without bound. (Compaction is a natural
  [scheduler](ccr-reference.md#the-scheduler--eod-exactly-once-across-replicas)
  job.)

Net: **auto-resume (offset checkpoint) + no loss (manual commit) + automatic
idempotent reconciliation (latest-per-key view).**

### Compaction — keeping the log bounded

The append log grows with every commit, so a **compaction** pass coalesces it:
it deduplicates replay rows by `_txn`, drops fully-tombstoned documents and rows
past their `_expire_at`, and rewrites each table as one Iceberg snapshot (which
also compacts small files). It's idempotent, so run it on a schedule — it pairs
naturally with the [scheduler](ccr-reference.md) as a nightly trigger.

```bash
python -m phronexus.retention.compact              # dedup + drop tombstoned/expired; keep history
python -m phronexus.retention.compact --collapse   # also drop superseded versions (latest per doc only)
```

`keep_history=True` (default) preserves every version of live documents (the
audit tail); `--collapse` keeps only the current version per doc for maximum
shrinkage. Either way the reconciled current-state view is unchanged.

## Binary journals

Enable with `journal.enabled: true` (plus the specific toggles). Both journals are
KVStore-backed, so they work on the in-memory backend and Aerospike unchanged.

### Message journal — full inbound messages

```python
mj = px.message_journal()                      # journal.messages_set
mj.record("trades.events:0:42", envelope,      # any msgpack-able object
          topic="trades.events", partition=0, offset=42, ts=...)
mj.read("trades.events:0:42")
# {"meta": {"topic": "...", "offset": 42, ...}, "message": <full envelope>}
```

Set `journal.journal_messages: true` and the Kafka state-machine runner persists
every raw inbound message (full envelope) before processing — for inspection or
replay.

### Request/response journal — one record per interaction

```python
rj = px.request_journal()                       # journal.interactions_set
rj.record("evt-1", request=req, response=resp,
          entity="trade", type="TradeReceived", status="applied", ts=...)
rj.read("evt-1")
# {"meta": {"entity": "trade", "status": "applied", ...},
#  "request": <decoded>, "response": <decoded>}
```

The request is stored in the `req` blob, the response in `resp`, with
`entity`/`type`/`status`/`ts` (and any extra kwargs) as metadata bins. Set
`journal.journal_requests: true` and **every state-machine `process()` call is
journaled automatically** — over Kafka *and* HTTP (`POST /entities/{entity}/events`
uses the same journaled machine). The stored response includes `emitted_events` —
the **full outbound events** (topic, type, key, payload) the transition sent — so
the journal answers "what came in, and exactly what we sent." Read one over REST:

```
GET /interactions/{event_id}
# {"meta": {"status": "applied", ...}, "request": <event>,
#  "response": {"to_state": "...", "emitted_events": [{topic, type, payload}, ...]}}
```

It's a complete, queryable-by-metadata, byte-faithful audit trail. Journaling is
best-effort: a journal failure logs a warning and never fails the pipeline.

> Metadata bin names must stay ≤15 characters (Aerospike’s limit). The built-in
> ones do; keep any extra `meta` kwargs within that too.

## Config

```yaml
# journal
journal:
  enabled: true
  journal_messages: true        # raw inbound Kafka messages -> _messages
  journal_requests: true        # state-machine req/resp     -> _interactions
  ttl_seconds: 0                # 0 = keep forever
```

Everything else (the insert-only log, manual offset commit, `_version` stamping)
is on by default — no configuration needed.
