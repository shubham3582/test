# Scaling the state-machine runner (to 500K TPS and beyond)

The state-machine runner (`python -m phronexus.statemachine.runner`) is
**stateless** — all state lives in Aerospike (documents, dedup markers, outbox)
and Kafka (offsets). That makes it scale horizontally in the easy way; the real
capacity planning is the **store and the produce path**, not the consumer.

> **The key reframe:** "one topic" is *not* the constraint. Consumer parallelism
> is capped by **partition count**, and per-event cost is dominated by
> **Aerospike ops**. Scale = partitions × replicas, sized against the store.

---

## First: know your real workload

500K events/s is **not** 500K store ops. Each event (`_process_once`) does roughly:

| Step | Aerospike | Kafka |
|---|---|---|
| dedup check | 1 read | |
| load current state | 1 read (+ canonical record) | |
| stage projections | N writes | |
| outbox (output events) | 1 write | |
| dedup marker | 1 write | |
| inverted index (if `index.in_txn`) | 1 write | |
| change-feed event | 1 write | (relayed) |
| output events + change feed | | ~1–2 produces |

So **500K TPS ≈ ~1.5M Aerospike reads/s + ~2.5M writes/s + ~1M Kafka produces/s.**
Size for *that*, not for 500K.

---

## The levers (in order of impact)

### 1. Partition the input topic heavily

Consumer parallelism ≤ partition count — you can never run more active consumers
than partitions. One topic with **512–1024 partitions** is normal. Size it high
**up front**: increasing partitions later changes the key→partition mapping and
breaks per-entity ordering for in-flight keys.

Partitioning is by **entity key**, which preserves per-entity ordering — but that
guarantee depends on the **upstream producer keying each message by entity id**.
Confirm your producers do this.

### 2. Scale the stateless runner horizontally

State is in Aerospike + Kafka, so run the runner as a Kubernetes `Deployment` with
many replicas in **one consumer group** (`statemachine.consumer_group`). Kafka
spreads partitions across replicas; add replicas up to the partition count.

- **Autoscale on consumer lag** (not CPU).
- Use **cooperative-sticky rebalancing** + **static group membership**
  (`group.instance.id`) so a scale event doesn't stop-the-world at 500K TPS.
- Graceful shutdown already drains + commits offsets on SIGTERM, so rolling
  deploys are safe (replay is idempotent).

### 3. Aerospike is the workhorse — size the cluster for it

~4M ops/s is squarely in Aerospike's range (~1M+ ops/s per node on flash/RAM), but
you provision for it:

- **Node count** for the read+write op rate, with headroom.
- **Namespace RAM** for the working set **plus the dedup set** — at 500K TPS,
  `_sm_dedup` holds `500K × dedup_ttl` markers. With the default 7-day TTL that is
  enormous; rely on `nsup` to expire them and/or **shorten `dedup_ttl`**.
- **Replication factor** ≥ 2 and the durability posture you need (see lever 7).

This is where most of the 500K-TPS budget goes.

### 4. Get the produce/relay path OFF the hot consume loop

At scale, do **not** relay inline. Split publishing into independently-scaled
workers so the consume loop is just consume → transition → commit:

| Setting | Value | Runs instead |
|---|---|---|
| `statemachine.inline_relay` | **false** | `python -m phronexus.statemachine.relay` (outbox → output topics) |
| `changefeed.inline_relay` | **false** | `python -m phronexus.changefeed_relay` (commits → change feed) |
| audit | (Kafka on) | `python -m phronexus.audit.main` (standalone trace consumer) |

The transactional outbox makes this safe: outputs are staged in the same commit as
the state + dedup marker, so a separate relay drains them at-least-once and
consumers dedup.

### 5. Batch and pipeline

`run(..., batch_size=...)` polls up to `batch_size` events and commits offsets
**once per batch** — bigger batches amortize the commit and broker round-trips.
Raise it well above the default of 100 for high throughput, and lean on Aerospike
**batched reads** (dedup + manifest) so a batch is a few round-trips, not 2N.

### 6. Watch the real ceiling: key skew (hot partitions)

Per-entity ordering means **one hot entity = one hot partition = a serial
bottleneck** — no number of replicas fixes it. If one counterparty/book is 30% of
traffic, that partition caps out first. Mitigations:

- Ensure **high-cardinality** keys.
- **Sub-key** hot entities where ordering permits (e.g. include a sub-id).
- Or accept the per-entity throughput ceiling as a design fact and shard the hot
  entity upstream.

Monitor per-partition lag, not just aggregate lag, to catch this.

### 7. Trim per-event overhead + pick a durability tier

- **Journaling**: `journal.journal_requests` / `journal_messages` write an extra
  msgpack record per event — **sample or disable** on the hot path at 500K TPS.
- **Index in-txn**: `index.in_txn=true` adds an index write inside every commit;
  set `false` to update the index post-commit (cheaper hot path, derived index
  rebuilt on crash) if you can tolerate a brief missed-index window.
- **Durability**: native multi-record txns (`aerospike.use_native_txn=true`,
  EE 8.0+) give exactly-once but cost more per commit; the ordered-puts path
  (`require_atomic=false`) is cheaper and at-least-once — safe because the
  transition/outbox path is idempotent (event-id dedup + generation CAS).

---

## Config knobs at a glance

| Knob | Default | For 500K TPS |
|---|---|---|
| input topic partitions | — | **512–1024+**, set high once |
| runner replicas | 1 | scale in one `consumer_group`, HPA on lag |
| `statemachine.inline_relay` | true | **false** → dedicated relay workers |
| `changefeed.inline_relay` | true | **false** → dedicated change-feed relay |
| `run(batch_size=…)` | 100 | raise (amortize commit/round-trips) |
| `statemachine.dedup_ttl` | 604800 (7d) | size Aerospike RAM for it; shorten if possible |
| `index.in_txn` | true | consider **false** (post-commit index) |
| `journal.journal_requests` | false | keep off / sample on the hot path |
| `aerospike.use_native_txn` | true | true = exactly-once (EE 8.0+); false = at-least-once, cheaper |
| `statemachine.require_atomic` | true | false only if you accept best-effort |
| Aerospike cluster | — | sized for ~4M ops/s + dedup-set RAM + RF≥2 |

---

## A concrete 500K-TPS shape

- **Input topic:** 768 partitions; producers keyed by entity id.
- **Runner:** ~60–120 pods in one consumer group, cooperative-sticky + static
  membership, autoscaled on consumer lag.
- **Aerospike:** cluster sized for ~4M ops/s, RF=2, RAM budgeted for the working
  set + `_sm_dedup`.
- **Produce path:** separate pools of **outbox-relay** and **change-feed-relay**
  workers (`inline_relay=false` on both); standalone **audit** worker.
- **Topics per entity type (optional):** split so a spike in one workload doesn't
  starve others, and each can be partitioned/scaled independently.
- **Hot-path trims:** `journal_requests=false`, larger `batch_size`, shorter
  `dedup_ttl`, consider `index.in_txn=false`.

**Bottom line:** the stateless runner scales linearly with **partitions × replicas**
— trivially. Your 500K-TPS engineering is: (a) enough partitions, (b) get the
produce path off the consume loop, (c) size Aerospike (including the dedup set),
and (d) design keys to avoid a hot partition.

---

## What Phronexus gives you for free at scale

- **Effectively-once** under this fan-out: event-id dedup + idempotent writes
  (doc-id + generation CAS) mean rebalances, retries, and redeploys replay safely.
- **Durable-first outbox:** outputs commit atomically with state, so decoupling the
  relay never loses an event.
- **Poison isolation:** a structurally-broken message is quarantined at the
  transport boundary (it can't wedge a partition), and a schema-invalid message is
  rejected at ingress and dead-lettered — neither stalls the pipeline.

See [state-machine.md](state-machine.md) for the atomicity model and
[deployment.md](deployment.md) for the production topology.
