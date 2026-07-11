# Phronexus as a Transactional State Machine

**Status:** implemented prototype (in-memory + Aerospike backends) · **Shape:**
hybrid — an autonomous Kafka-driven service *and* an embedded DishtaYantra node,
sharing one engine.

Phronexus can run as an autonomous, event-driven state machine:

```
consume(event) ─▶ [ store new state + outbox events + dedup marker ]atomic ─▶ relay ─▶ emit
```

It takes a domain event from Kafka, loads the entity's current state, evaluates a
metadata-driven transition, and **atomically** persists the new state, enqueues
the resulting output events, and records a dedup marker — then a relay publishes
the outbox to Kafka.

## The atomicity guarantee (read this first)

There is **no native distributed commit** across Kafka offsets + Aerospike +
Kafka output. Anyone claiming "atomic consume-process-produce across Kafka and an
external store" means Kafka-only EOS (state must live in Kafka) — which does not
cover our Aerospike write. So Phronexus uses the **transactional outbox**:

- **Atomic where it counts** — new state (projections + manifest), the output
  events, and the input-dedup marker are written in **one Aerospike native
  transaction**. All-or-nothing.
- **At-least-once emission** — a relay drains the outbox to Kafka after commit. A
  crash between commit and publish just means the relay re-publishes.
- **Effectively-once end to end** — idempotent writes (`doc_id` + generation CAS),
  `event_id` dedup on input, and idempotent downstream consumers.

The honest guarantee is **"atomic durable transition + effectively-once
emission,"** not literal 2PC — which is the strongest this class of system offers.

## The transactional loop

```
 Kafka in ─▶ consume(event, key=doc_id)
               │
               ▼   ┌──────────── ONE Aerospike native transaction ───────────┐
           dedup check: event_id already applied? ── yes ─▶ skip (idempotent) │
               │ no                                                           │
           load state (manifest.read by key)                                 │
           evaluate transition (event + from_state → to_state, guard)        │
           stage_write: projections + manifest (new state)                   │
           put output events → _sm_outbox                                    │
           put dedup marker  → _sm_dedup (event_id, TTL)                     │
               └──────────────────── commit (all-or-nothing) ───────────────┘
               │
           post_write: inverted-index update + change-feed CommitEvent
           drain _sm_outbox ─▶ OutputPublisher (Kafka)   at-least-once
           commit input offset       (safe: dedup makes reprocessing a no-op)
```

**Ordering & concurrency.** Partition input by `doc_id` (Kafka key), so all events
for an entity are ordered on a single consumer — no concurrent transitions on a
key; the manifest generation-CAS is the backstop. Scale horizontally by partition.

## The transition contract (metadata, not code)

A fourth contract kind. Onboarding a new lifecycle is a config file, like every
other entity concern.

```yaml
kind: transition
entity: trade
version: 1
state_field: status
inputs: ["kafka://trades.events"]
transitions:
  - event: TradeBooked     from: null       to: booked     emit: [{topic: "kafka://trades.booked"}]
  - event: TradeConfirmed  from: booked     to: confirmed  emit: [{topic: "kafka://settlement.requests"}]
  - event: TradeSettled    from: confirmed  to: settled    guard: "notional > 0"
                                                           emit: [{topic: "kafka://trades.settled"}]
  - event: TradeCancelled  from: "*"        to: cancelled  emit: [{topic: "kafka://trades.cancelled"}]
```

- `from: null` matches "no existing document" (creation); `from: "*"` matches any
  state.
- `guard` is a **sandboxed** boolean expression over the candidate document —
  restricted AST (comparisons / and-or-not / names / literals), no calls, no
  imports, no attribute access.
- The field is named `event` (not `on`) because YAML 1.1 parses a bare `on:` key
  as boolean `True`.

## Two faces, one engine

| Face | Entry | Use |
|---|---|---|
| **Autonomous service** | `python -m phronexus.statemachine.runner` | consume Kafka → transition → emit Kafka (async) |
| **Synchronous REST** | `POST /entities/{entity}/events` | submit an event, get accept/reject in the response |
| **DishtaYantra node** | `PhronexusStateMachineNode.calculate(data)` | embed the same transition as a DAG `CalculationNode` |

All three call the same `StateMachine.process(event)`. The REST face returns the
decision synchronously — `200` applied/duplicate, `409` no valid transition,
`422` guard failure — while output events still fan out asynchronously through
the outbox → Kafka. As a service Phronexus is a
loosely-coupled peer (topics are the only contract, durable + replayable +
auditable); as a node it rides a DishtaYantra DAG with in-process latency. Pick
per flow — for settlement/lifecycle correctness the service face usually wins;
for microsecond enrichment the node face does.

## Extensibility: hooks and output routing

**Custom code between consume and commit** — register `ProcessingHook`s (they run
for all three faces because they live in `process()`):

| Hook | When | Use |
|---|---|---|
| `on_event(event)` | after consume, before the transition | enrich / validate / transform; return a modified event, or `None` to **drop** (ack + skip, not retried) |
| `on_transition(ctx)` | transition matched, **before commit** | mutate `ctx.new_doc` (enrichment), or raise `TransitionRejected` to reject (retryable) |
| `on_committed(event, result)` | after commit, around the relay | audit, metrics, side effects |

Rule: any I/O (reference-data lookup, HTTP enrichment) goes in `on_event` /
`on_transition` — *before* the transaction — to keep the store transaction tight.

```python
sm = px.state_machine(hooks=[EnrichHook(), ValidateHook()])
```

**Flexible input/output topologies** — input (`InputSource`) and output
(`OutputPublisher`) are pluggable, and emit targets are URIs, so `RoutingOutputPublisher`
dispatches by scheme:

| `emit` target | Goes to |
|---|---|
| `kafka://topic` | Kafka / MSK |
| `http(s)://host/path` | `HttpOutputPublisher` (POST, TLS/mTLS) |
| `null://` or empty `emit` | dropped — **consumer-only** |

A single transition can mix them (some events to Kafka, some to a webhook, some
silent). Failed HTTP/Kafka publishes stay in the outbox for retry (at-least-once).

**Inline vs. standalone relay.** By default `process()` drains the outbox inline
after commit — simplest, but a slow broker/webhook adds latency to the sync REST
response or the runner. Set `statemachine.inline_relay: false` and run a
standalone relay so the write path returns as soon as the transaction commits:

```bash
python -m phronexus.statemachine.relay      # drains the outbox -> publishers
```

The transition is durable the moment it commits; the relay publishes
at-least-once and can run with multiple replicas (rows are removed only after a
successful publish; consumers dedup on event id).

**Event-schema validation (produce-time).** A `stream` contract registers a JSON
Schema per emitted event type. Before the commit, each output event's payload is
validated against its schema; under `enforce` a failure **rejects the transition**
(nothing committed, nothing published), so malformed events never reach the wire
— a schema-registry substitute without an external registry.

## What it reuses vs. adds

**Reuses:** manifest + native transaction (the atomic core), the `_outbox`/change-
feed pattern, generation-CAS idempotency, the contract registry + hot reload.

**Adds:** the `transition` contract kind, a sandboxed guard evaluator, the
processor (`StateMachine`), input sources / output publishers (memory + Kafka), a
dedup-marker set, and the two entry points. `ManifestManager` was refactored into
`stage_write` (stages into a caller's transaction) + `post_write` (post-commit
side effects) so the write composes into the larger state-machine transaction.

## Prototype status

Implemented on the in-memory backend with 10 tests covering: creation, full
lifecycle, wrong-state rejection (atomic — no partial writes), guard rejection,
input dedup, wildcard transitions, outbox-drains-after-commit, change-feed flow,
the node face, and guard sandboxing. Aerospike native transactions are wired for
production; Kafka I/O is behind the optional `[kafka]` extra.

## Open items for production

- Dead-letter routing for rejected/unmatched events (currently returned to the
  caller).
- Outbox relay as a separate always-on process (vs. inline drain) + backpressure.
- Payload templating in `emit` (currently the new state document is the payload).
- Per-key watermark as an alternative to per-`event_id` dedup markers at very high
  cardinality.
