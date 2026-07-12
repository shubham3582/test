# DishtaYantra vs Phronexus Core — responsibilities by use case

> **They are not competitors — they are two layers.** DishtaYantra is the base
> **compute/orchestration DAG engine**; Phronexus Core is the **durable, governed
> data layer**. This doc maps use cases to the right owner, and — more usefully —
> calls out the **overlap zones** where both *could* do it, so you don't build the
> same thing twice.
>
> *DishtaYantra is characterized from its paper/design (Sinha, 2025; ComputeGraph,
> six node types, `calculate/details`, LMDB zero-copy IPC, URI pub/sub). Confirm
> specifics against the repo — see [integration-dishtayantra.md](integration-dishtayantra.md).*

## The one-line split

- **DishtaYantra** moves and computes data *in flight*: orchestration, scheduling,
  zero-copy transport, multi-language (C++/Rust/Python) numeric nodes.
- **Phronexus** persists and governs data *at rest*: contracts, manifest atomicity,
  indexing, query/views, versioning, retention, lineage.

They meet at two extension points (Phronexus as a DishtaYantra pub/sub backend, and
as a `calculate`/`details` calculator), with the `CommitEvent` change feed as the
shared spine.

## Use-case comparison

| Use case | DishtaYantra | Phronexus Core | Own it in |
|---|---|---|---|
| Multi-step compute pipeline (a DAG) | **native** — that's the engine | not its job | **DishtaYantra** |
| Heavy numeric / multi-language compute (pricing, exposure sim) | **native** (C++/Rust nodes) | delegates it | **DishtaYantra** |
| Zero-copy data hand-off between compute steps | **native** (LMDB IPC) | durable store, not in-flight IPC | **DishtaYantra** |
| Parallel fan-out/fan-in (per counterparty / netting set) | **native** (DAG-affinity scheduling) | — | **DishtaYantra** |
| Durable system of record (trades, counterparties, cubes) | stateless nodes | **native** (manifest, insert-only) | **Phronexus** |
| Schema governance / validation / contract evolution | node config only | **native** (6 contracts + compat gate) | **Phronexus** |
| Indexing + query + serving hot data (by cpty / netting set) | — | **native** (inverted index, `find`, batch) | **Phronexus** |
| Consumer views (allow-list / mask / transform) | — | **native** (view contracts) | **Phronexus** |
| Versioning / point-in-time / as-of reads | — | **native** (insert-only + version in key) | **Phronexus** |
| Long-term retention / lineage / audit | — | **native** (Iceberg/S3 Tables, trace, journals) | **Phronexus** |
| Reference/static data enrichment *in flight* | needs data | **native** (`PhronexusQueryCalculator` node) | **Phronexus, inside a DishtaYantra node** |
| Event-driven triggering (new commit → recompute) | consumes it | **emits it** (`CommitEvent`) | **Phronexus emits → DishtaYantra consumes** |
| Pub/sub transport between DAGs | **native** (SPI) | can back it (`phronexus://`) | **DishtaYantra SPI, Phronexus optional backend** |

## Overlap zones — the judgment calls

These are the only places both *look* capable. Pick one per workload:

**① Orchestration: DAG (DishtaYantra) vs saga/state-machine (Phronexus).**
- Use **DishtaYantra** for a **compute graph across services/languages** with fan-out
  and zero-copy hand-off (the CCR run: price → simulate → net → aggregate).
- Use the **Phronexus state machine** for a **transactional per-entity lifecycle**
  where each step must persist state + emit effectively-once (a trade's saga
  `received → cube_requested → published`). It's not a general orchestrator; it's
  atomic state progression for one entity.
- Rule of thumb: **many services / heavy compute → DishtaYantra; one entity /
  transactional state → Phronexus.** Don't run a big DAG as a Phronexus saga, and
  don't rebuild per-entity transactional state as DAG nodes.

**② Scheduling: DAG run scheduling (DishtaYantra) vs exactly-once scheduler (Phronexus).**
- Use **DishtaYantra** to schedule/kick **DAG runs** (EOD graph, intraday cycles).
- Use the **Phronexus scheduler** for **data-side exactly-once jobs whose lease lives
  in the store** (retention compaction, an occurrence that must fire once across
  replicas tied to store state).
- Don't run two schedulers for the same job — pick the layer that owns the trigger.

**③ Transport / passing data between steps.**
- **In-flight, node-to-node, same run → DishtaYantra LMDB** (zero-copy; never round-trip
  through the store for intermediate payloads).
- **Durable, cross-run, shared, queryable → Phronexus.** If another service or a later
  run needs it, it belongs at rest in Phronexus, not only in LMDB.

**④ Eventing.**
- Phronexus already emits a Kafka/MSK `CommitEvent`; DishtaYantra has its own pub/sub.
  Keep **Phronexus as the source-of-truth event** (a `SubscriptionNode` consumes it);
  use DishtaYantra pub/sub for **intra-DAG** signaling. Don't duplicate the spine.

## How they combine (the intended shape)

```
DishtaYantra DAG:  Subscribe(phronexus://trade) → Price(C++) → Simulate(C++)
                   → Enrich[PhronexusQueryCalculator] → Aggregate → Publish(phronexus://exposure)
                                     │ LMDB zero-copy between nodes │
                   Phronexus is the source, the in-flight enrichment, and the sink;
                   its CommitEvent triggers the next DAG.
```

- **Source / sink:** a node reads (`phronexus://`) or writes (`px.put`) through
  contracts — governed I/O at the DAG's edges.
- **Enrichment:** `PhronexusQueryCalculator` joins in reference/static data mid-DAG
  without leaving the process.
- **Nodes stay stateless;** all durable state and governance live in Phronexus.

## Decision guide

- **Reach for DishtaYantra when** the problem is *compute and coordination*: a graph
  of steps, heavy/multi-language maths, parallelism, low-latency hand-off.
- **Reach for Phronexus when** the problem is *data and governance*: durable state,
  schema/contracts, indexing/query, views, versioning, retention, audit.
- **Use both when** (the common case) a governed store feeds a compute DAG and the
  results flow back under contract — i.e. any real CCR platform.

## Bottom line

Don't frame it as DishtaYantra *or* Phronexus. DishtaYantra is the **run** (compute
+ orchestration); Phronexus is the **record** (governed data + lineage). The only
real decisions are the three overlap zones above — resolve those explicitly and the
two layers compose cleanly, with Phronexus the single source of truth and DishtaYantra
nodes stateless.
