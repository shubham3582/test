# Counterparty-Credit-Risk (CCR) on Phronexus — reference implementation

A worked, runnable example of building a real risk pipeline on Phronexus with
**no framework changes** — only contracts, one schedule file, and a demo script.
Everything here lives under [`examples/ccr/`](../examples/ccr).

```
python examples/ccr/run_ccr.py                              # in-memory, no services
PHRONEXUS_BACKEND=aerospike python examples/ccr/run_ccr.py  # same saga, live stack
```

The script defaults to the built-in **in-memory** backend, which is always
available (pure Python, no services, no optional dependencies) — so the example
runs anywhere with no setup. Set `PHRONEXUS_BACKEND=aerospike` (with the usual
`PHRONEXUS_AEROSPIKE__*` / `PHRONEXUS_KAFKA__*` env) to drive the identical saga,
value cube, and exactly-once scheduler against a live Aerospike + Kafka
deployment — the saga state and cube then persist across runs, so re-processing
an already-seen event is a no-op (effectively-once across process restarts).

## Production depth — ten proofs

The goal is **not** to compute risk (the quant engine owns SA-CCR/IMM/XVA); it is
to prove Phronexus reliably **governs and serves the operational data around** one.
`run_ccr.py` narrates all ten; each is pinned by a test (`pytest -m ccr`).

| # | Proof | How |
|---|---|---|
| CCR1 | Trade & counterparty onboarding | validation + reference-DQ (`counterparty`/`currency`/`netting_set` must resolve) + lifecycle sagas → `active`/`published` |
| CCR2 | Contract & reference-data evolution | governed `draft→submit→approve→publish` of `ccr_trade` v2 + `BackfillJob` re-projection; a currency added to reference data is then admitted |
| CCR3 | Netting-set lifecycle | `open→active→amended→closed` saga; trades reference a set (`idx_ns`); a **served** `ns_exposure` aggregate |
| CCR4 | Intraday recalculation | `RecalcRequested` re-drives the saga → a new `exposure_result` version at the same COB, later tx-time |
| CCR5 | Cube projection | `value_cube` point-per-tenor (hot sorted/clipped) **and** `fvcube` transposed (dates → `d<YYYYMMDD>` bins) |
| CCR6 | Late & corrected events | a backdated correction is only visible at/after its tx-time |
| CCR7 | COB reproducibility | reading an exposure as-of a COB is stable under later writes (bitemporal) |
| CCR8 | Hot/cold tier movement | retention lands trades/exposures in the warehouse; hot and cold agree; rows expire past their horizon |
| CCR9 | Lineage | `px.lineage(trade_id)` stitches source event → saga → cube → exposure + producing contract versions (`GET …/lineage`, console **Lineage** view) |
| CCR10 | Recovery from partial failure | fault harness: torn write is invisible + reaped; broker redelivery doesn't double-request; retention replay lands one cold row |

**The consolidated domain** (`examples/ccr/contracts/`): reference data
`counterparty` (+ onboarding lifecycle) and `currency`; the `netting_set` lifecycle
entity; the `ccr_trade` saga (reference-DQ + `idx_cp`/`idx_ns` membership); the
`value_cube` (point) and `fvcube` (transpose) cubes; and the **bitemporal**
`exposure_result` (trade-level) + `ns_exposure` (netting-set-level, served
aggregate). Shared operations live in `examples/ccr/ccr_ops.py`.

## The problem

Modernise CCR trade processing:

1. A **UDM trade** (JSON) is received.
2. Request the **future-value cube** from MFL.
3. Request risk numbers from the **calculator** service.
4. Publish the result to the **display** service over Kafka.
5. Run **EOD** every evening (load market data + reprice today's trades).

This is an asynchronous request/reply **saga**: each downstream service consumes
a request topic, does its work, and replies with an event; the pipeline advances
one step per reply. Phronexus is the orchestrator and the system of record — the
heavy computation still lives in MFL / the calculator grid.

## How it maps onto Phronexus

| Concern | Phronexus mechanism | File |
|---|---|---|
| Trade system of record | storage contract (`ccr_trade`) | `contracts/trade.storage.yaml` |
| UDM shape + data quality | validation contract (JSON Schema + DQ) | `contracts/trade.validation.yaml` |
| The saga | transition contract (state machine) | `contracts/trade.transition.yaml` |
| Outbound request schemas | stream contract (validated at produce time) | `contracts/trade.stream.yaml` |
| Trade lookups | query contract | `contracts/trade.query.yaml` |
| Hot value cube | storage + query contracts (`value_cube`) | `contracts/value_cube.*.yaml` |
| EOD job | schedule definition | `schedules/eod.json` |

### The saga

```
                    (UDM trade JSON)
                          │  TradeReceived
                          ▼
   ┌───────────────┐  RequestValueCube   ┌──────────┐
   │ cube_requested│ ───────────────────▶│   MFL    │
   └───────────────┘                     └────┬─────┘
          ▲  CubeReady                        │ streams the cube
          │                                   ▼
   ┌───────────────┐  RequestCalc        ┌──────────┐   value_cube
   │ calc_requested│ ───────────────────▶│Calculator│   (Aerospike, hot)
   └───────────────┘                     └────┬─────┘
          ▲  CalcComplete                     │
          │                                   ▼
   ┌───────────────┐  DisplayUpdate      ┌──────────┐
   │   published   │ ───────────────────▶│ Display  │
   └───────────────┘                     └──────────┘
```

Each transition **persists the new trade state and enqueues the outbound request
in one store transaction**, then the outbox is relayed to Kafka. Because the
input event is deduped and the write is a generation-CAS, a redelivered reply
never double-fires the next request — the pipeline is *effectively-once* end to
end. `run_ccr.py` demonstrates this: re-processing `TradeReceived` returns
`duplicate` and emits no second cube request.

The transition contract is the whole saga, as config:

```yaml
transitions:
  - {event: TradeReceived, from: null,            to: cube_requested,
     emit: [{topic: "kafka://mfl.cube.requests", type: RequestValueCube}]}
  - {event: CubeReady,     from: cube_requested,  to: calc_requested, guard: "as_of > 0",
     emit: [{topic: "kafka://calc.requests",     type: RequestCalc}]}
  - {event: CalcComplete,  from: calc_requested,  to: published,      guard: "exposure >= 0",
     emit: [{topic: "kafka://ccr.display",        type: DisplayUpdate}]}
  - {event: TradeCancelled, from: "*",            to: cancelled,
     emit: [{topic: "kafka://ccr.display",        type: DisplayUpdate}]}
```

### Delivery & offset commits — why replay is safe

The store-side guarantee above (dedup + generation-CAS in one transaction) is one
half; the Kafka side is the other. Every consumer in the pipeline (the state-machine
runner, and the retention/audit workers) is **manual-commit**:

- `enable.auto.commit: false`, `auto.offset.reset: earliest`.
- The offset is committed **synchronously, only *after* the event is durably
  handled** — for the saga: state persisted **and** the outbox relayed to Kafka;
  for retention: rows flushed to Iceberg. Never before.

That ordering is deliberately **at-least-once** (it closes the loss window — an
offset is never committed for work that isn't durable). A crash, rebalance, or
redelivery between "handled" and "committed" simply **replays the batch** — and
replay is safe because processing is idempotent: the state machine dedups by
`event_id` (a replayed `TradeReceived` returns `duplicate`, no second cube request)
and retention appends are idempotent by `txn_id`. **At-least-once delivery +
idempotent processing = effectively-once end to end.**

### Durability & delivery guarantees ("will we lose a message?")

**No message durably accepted into the pipeline is lost** — at-least-once,
effectively-once end to end — **provided the infrastructure is configured for
durability.** There is no cross-system two-phase commit; the guarantee is
*engineered* from the transactional outbox + `event_id` dedup + commit-after-process
ordering, so it holds exactly as far as these four links:

| Link | Requirement | If not |
|---|---|---|
| Producer → Kafka | `acks=all`, retries on | a message can be lost at the broker *before* Phronexus ever sees it |
| Kafka / MSK | replication ≥ 2, `min.insync.replicas` ≥ 2, no unclean leader election, retention **>** worst consumer lag | committed messages lost on failover, or aged out if a consumer lags past retention |
| Aerospike | Enterprise + **strong-consistency (SC)** namespace, persistent storage, replication | a *memory* namespace (the dev stack) loses everything on restart; a non-SC namespace can drop an acked write on failover |
| Rejects | a `dlq_topic` (`statemachine.dlq_topic`) | a rejected/poison message is acked and **dropped** (by design — but gone) |

> ⚠️ The dev/test stack uses `storage-engine memory` and often no DLQ, so it does
> **not** give you no-loss. Production config does — see [deployment.md](deployment.md#aerospike)
> (Aerospike SC) and [Amazon MSK](deployment.md#amazon-msk).

**Check it, don't assume it.** `phronexus doctor` (or `px.durability_report()`)
inspects the live config — producer `acks`/idempotence, DLQ, and the Aerospike
namespace's `strong-consistency` / `storage-engine` / `replication-factor` — and
reports `pass`/`warn`/`fail` per link, exiting **non-zero if the config can lose
messages** (so a deploy/CI gate can block it). It confirms the *configuration
posture*; it can't see third-party producers on your input topics or broker-side
`min.insync.replicas` — those it lists as "not auto-verified."

**Say it precisely:** *at-least-once, effectively-once end to end — no message
durably accepted is lost, given durable producers, a replicated Kafka/MSK topic
with adequate retention, and Aerospike Enterprise + SC. We may reprocess on failure
(safe, because processing is idempotent), but we don't drop committed messages.*
"Never lose a message" is true only with all four links in place; it is not magic.

> **Enriching a reply with custom code.** When a service reply needs
> post-processing before the next request (call a pricing library, look up
> reference data), attach an `on_transition` hook — it runs inside the same
> transaction and can mutate the candidate document or reject the step. See
> [state-machine.md](state-machine.md#hooks).

### The value cube — hot store, sort & clip

MFL delivers the cube either by **streaming points over Kafka** or by **dropping
the whole cube via S3 in one go**. Either way each `(trade_id, scenario_id,
tenor)` point is written with `px.put("value_cube", …)` and lands in Aerospike
for hot access. Reads use a **sorted, clipped** query — pull a trade's cube,
order by `tenor` (a numeric index), and take the nearest N points:

```python
px.query_page(QueryDoc(
    entity="value_cube",
    where=[{"field": "trade_id",   "op": "eq", "value": trade_id},
           {"field": "scenario_id","op": "eq", "value": "BASE"}],
    sort=[SortKey(field="tenor", order="asc")],
    limit=4,
))
```

`tenor` is declared `numeric` in `value_cube.query.yaml`, so it supports both
range predicates (`tenor <= max`) and ordered sort.

#### Future-value cube — storage layouts

The cube's *physical* shape is a storage-contract choice. All three layouts below
are config only (no cube-specific code); pick by data volume and read pattern.

**1. Point per `(trade, scenario, tenor)`** — the default above. One small record
per point; hot reads sort/clip by `tenor`. Simple; many records per trade.

**2. Transposed — dates as bins** ([`examples/fvcube/`](../examples/fvcube)). One
record per `(trade, scenario)`; the ETL shapes the curve as a `{date: value}` map
and a `spread` projection explodes it so **each date is its own Aerospike bin**
(`d20260712`, `d20260718`, …) for native per-date access:

```yaml
- name: wide
  set: fvc_wide
  key: "{trade_id}:{scenario_id}"
  fields: [as_of, currency, curve]
  encoding: bins
  spread: [{field: curve, prefix: "d"}]     # {date: value} -> bins d<YYYYMMDD>
```

**3. At scale — parts as bins + max per date**
([`examples/fv_paths/`](../examples/fv_paths)). When each date carries **3 parts of
~2000 numbers** (e.g. Monte-Carlo paths), transposing into one record would blow
Aerospike's record-size limit — so put `val_date` in the primary key (**one ~48 KB
record per date**), store each part as its own bin, and a per-date `max` computed
at ingest:

```yaml
primary_key: [trade_id, scenario_id, val_date]   # one record per date
projections:
  - {name: doc, set: fvp_doc, key: "{trade_id}:{scenario_id}:{val_date}", fields: ["*"], encoding: msgpack, canonical: true}
  - name: wide
    set: fvp_wide
    key: "{trade_id}:{scenario_id}:{val_date}"
    fields: [val_date, currency, max_value, part1, part2, part3]
    encoding: bins
    bin_map: {max_value: max, part1: p1, part2: p2, part3: p3}
```

Reads always reconstruct the clean nested document from the canonical (msgpack)
projection; the `bins`/`spread` projections are native, read-optimised physical
views. See [contracts-reference.md](contracts-reference.md#storage) for the
`encoding` / `bin_map` / `spread` fields.

## The scheduler — EOD exactly once across replicas

`schedules/eod.json` fires the EOD job daily at 18:30 New York:

```json
{"name": "ccr-eod", "topic": "kafka://ccr.eod", "daily_at": "18:30",
 "timezone": "America/New_York",
 "payload": {"job": "end-of-day", "steps": ["load_market_data", "reprice_todays_trades", "aggregate_exposure"]}}
```

The scheduler stores its state in Aerospike and claims each occurrence with a
**generation-CAS lease**, so you can run many scheduler replicas for
availability and **an occurrence still fires exactly once** — no duplicate EOD
events. The pattern is *kafka topic → the responsible service → the service
pulls the data it needs via Phronexus*.

```
              tick()             CAS-claim occurrence          relay
  replica A ──────────┐   ┌── _sched_state (gen++) ──┐   ┌─▶ kafka://ccr.eod
  replica B ──────────┴──▶│  winner writes outbox    │──▶│   (published once)
   (both due)             └── loser: GenerationConflict ─┘   loser: no-op
```

Run it standalone with `python -m phronexus.scheduler.runner` (one or more
replicas), manage schedules over REST (`PUT/GET/DELETE /schedules`, `POST
/schedules/tick`; admin), or
embed `px.scheduler()` in your own process. See the exactly-once proof in
`run_ccr.py` step 5 and in `tests/test_scheduler.py`.

## What Phronexus does and does not do here

- **Does:** orchestrate the saga, enforce the UDM contract, keep the trade +
  cube system of record, guarantee effectively-once progression and
  exactly-once scheduling, route requests/replies over Kafka, serve hot cube
  reads.
- **Does not:** compute the value cube or the risk numbers — those stay in MFL
  and the calculator grid. Phronexus coordinates them; it is not the compute
  engine. (If you also need the DAG/compute layer, see
  [integration-dishtayantra.md](integration-dishtayantra.md); for this use case
  the saga above replaces the need for an external orchestrator.)

## Files

```
examples/ccr/
  contracts/
    trade.storage.yaml       trade.validation.yaml   trade.transition.yaml
    trade.stream.yaml        trade.query.yaml
    value_cube.storage.yaml  value_cube.validation.yaml  value_cube.query.yaml
  schedules/eod.json
  run_ccr.py
```

Tests that lock the reference in: `tests/test_ccr_example.py`,
`tests/test_scheduler.py`.
