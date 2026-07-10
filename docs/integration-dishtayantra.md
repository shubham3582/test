# Integrating Phronexus Core with DishtaYantra

**Status:** design (pre-implementation) · **Direction:** DishtaYantra is the base DAG
engine; Phronexus Core integrates into it as library-level components.

> **Scope note.** This design is derived from the DishtaYantra paper's described API
> — `ComputeGraph`, the six node types, the `calculate(data)/details()` calculator
> interface, and the URI-addressed pub/sub SPI (*Sinha, 2025;
> `github.com/ajsinha/dishtayantra`*). Exact method signatures must be confirmed
> against the repository; every dependency on it is flagged in
> [§7 Open items](#7-open-items-to-confirm-against-the-repo).

---

## 1. Integration thesis

DishtaYantra owns **orchestration, scheduling, transport (LMDB zero-copy IPC), and
multi-language compute**. Phronexus owns **durable contract-driven state, indexing,
query/views, and long-term retention**. They meet at exactly two extension points
DishtaYantra already defines, so Phronexus enters as a **library adapter, not a fork**:

| DishtaYantra extension point | Phronexus role | Direction |
|---|---|---|
| **Pub/Sub backend** (`phronexus://` URI) | write via the manifest pattern / stream the change-feed | both ways |
| **Calculator** (`calculate`/`details`) | read / query / view **enrichment** inside a `CalculationNode` | DAG → store → DAG |

The Phronexus change-feed (the existing `CommitEvent`) is the **shared spine** — it
keeps Phronexus the single source of truth and DishtaYantra nodes stateless.

---

## 2. Data flow

```
        ┌──────────────── DishtaYantra ComputeGraph (the base) ────────────────┐
        │                                                                       │
 Subscription        Calculation            Calculation           Publication   │
   Node          →   Node (C++/Rust)    →   Node                →   Node         │
 (kafka://…)         heavy numeric          [PhronexusQueryCalc]    [phronexus://trade]
        │                 ▲                       │ enrich              │ write   │
        │        LMDB zero-copy between nodes     │ get/query/view      │ px.put  │
        └─────────────────┼───────────────────────┼─────────────────────┼────────┘
                          │                        ▼                     ▼
                          │                 ┌──────────────── Phronexus Core ─────────────┐
                          │                 │  contracts · manifest · inverted idx · views │
                          └─(phronexus:// subscribe: CommitEvent → new DAG)◄── change-feed
```

- **Enrichment** (`PhronexusQueryCalculator`): a record flowing through the DAG is
  joined with looked-up / queried Phronexus data (e.g. attach reference/static data
  to a trade in flight).
- **Sink** (`phronexus://` publisher): a terminal node commits the record through the
  manifest write path.
- **Source** (`phronexus://` subscriber): a `SubscriptionNode` consumes Phronexus
  `CommitEvent`s to trigger downstream DAGs (retention, derived analytics).

---

## 3. Adapter surface

The surface is small because Phronexus already exposes the verbs
(`put/get/query/query_pattern/view/delete`, the `CommitEvent` sink, and
`PhronexusClient`). New code is limited to two adapters, a per-worker bootstrap, a
change-feed consumer, and a batch `put`.

```python
# A) Phronexus as a DishtaYantra pub/sub backend  (Infrastructure layer)
#    URI:  phronexus://<entity>[?view=<v>&mode=upsert|delete]
class PhronexusPubSubBackend(DishtaPubSubBackend):     # implements their SPI
    def publish(self, topic, message):                 # topic -> entity
        return get_phronexus().put(entity_of(topic), message)          # -> doc_id

    def subscribe(self, topic, handler):               # drives the change-feed
        for ev in change_feed(entity_of(topic)):       # Phronexus CommitEvent
            handler(ev.document, meta=ev)              # deliver into a DAG node


# B) Phronexus as a Calculator  (Integration layer) — read / enrichment
class PhronexusQueryCalculator(Calculator):            # calculate / details
    # config: {entity, pattern|where, view?, mode: enrich|replace, key_from}
    def calculate(self, data):
        hits = (px.query_pattern(entity, pattern, **bind(data)) if pattern
                else px.query({"entity": entity, "where": where}))
        return merge(data, hits) if mode == "enrich" else hits

    def details(self):     # calculator metadata + runtime stats for their dashboard
        ...


# C) Per-worker bootstrap (GIL-free multiprocessing: one instance per process)
def get_phronexus() -> Phronexus:                      # lazy singleton per worker
    global _PX
    if _PX is None:
        _PX = Phronexus(Settings())                    # its own Aerospike/Kafka conns
    return _PX
```

---

## 4. The four decisions that actually matter

### ① Process & connection model — the most important
DishtaYantra workers are separate interpreters (GIL-free multiprocessing). Aerospike
clients, the contract cache, and the Kafka producer are **not fork-safe or
shareable**. Therefore:

- **One Phronexus instance per worker process**, created lazily in a worker-init
  hook; each keeps its own connections and refreshes its own contract cache (the
  5-minute cadence is fine per worker).
- Use DishtaYantra's **DAG-affinity scheduling** to pin Phronexus-writing nodes to
  workers that hold warm Phronexus/Aerospike connections.

### ② Zero-copy boundary
In-DAG payloads move zero-copy over LMDB; Phronexus is a **terminal sink /
enrichment**, so it does not break that — it serializes only at its *own* Aerospike
boundary, which is separate. To keep the hot path fast:

- The Phronexus sink consumes the LMDB-mapped record **without re-serializing**.
- Add a **batch `put`** so a node handling N records amortizes manifest/transaction
  overhead.
- *Open item:* confirm the on-wire payload format over LMDB (dict / msgpack / Arrow).
  If Arrow record-batches, the sink does columnar batched writes.

### ③ Two event systems, one spine
Phronexus already emits a Kafka change-feed; DishtaYantra has its own pub/sub
(including `lmdb://`, `inmemory://`). Avoid double sources of truth: the
`phronexus://` **subscriber** adapts `CommitEvent → DishtaYantra message`, so
same-host DAGs can consume commits over `lmdb://`/`inmemory://` while cross-host
still uses Kafka. Phronexus stays the source of truth; DAG nodes stay stateless.

### ④ Contracts vs. node config — both are config-driven, so exploit it
Cross-reference by `entity`: a `phronexus://trade` node is valid only if an **active
storage contract** for `trade` exists.

- Add a validator at DAG-load time.
- Add a generator that emits a `phronexus://` publisher stub from a Phronexus storage
  contract.
- Optionally, a single onboarding manifest carrying both the DAG wiring and the
  storage/query/view contracts.

---

## 5. Cross-cutting concerns

- **Deployment modes.** *Embedded* (Phronexus core imported into DishtaYantra workers
  — lowest latency, matches the zero-copy ethos; the primary mode for "as a library")
  vs *remote* (`PhronexusClient` over REST/mTLS — team isolation, adds a network hop).
  Both sit behind `get_phronexus()`.
- **Fault tolerance.** Phronexus raises typed errors; map them onto DishtaYantra node
  fault-isolation / auto-recovery. Retries are safe because writes are **idempotent by
  `doc_id`** (manifest + generation-CAS).
- **Observability.** Propagate OpenTelemetry trace context in record metadata across
  the boundary (DAG node span → Phronexus write span). Both already emit OTel +
  Prometheus.
- **Security.** Embedded mode is in-process (no auth hop); remote mode reuses
  Phronexus mTLS / API-key. LMDB zero-copy is **same-host only** — cross-host stays on
  Kafka/etc.

---

## 6. Phased rollout

| Phase | Deliverable |
|---|---|
| **A** | `phronexus://` publisher (write sink) + per-worker bootstrap + batch `put` — smallest useful slice |
| **B** | `PhronexusQueryCalculator` (read / enrichment) |
| **C** | `phronexus://` subscriber (change-feed → DAG) |
| **D** | contract/config bridge + DAG-load validation + OTel context propagation |

---

## 7. Open items to confirm against the repo

- Calculator & pub/sub backend SPI signatures.
- Worker-init hook API (for the per-process Phronexus singleton).
- On-wire payload format over LMDB (dict / msgpack / Arrow) — determines the
  zero-copy batch-write design.
- License fit (the paper is distributed MIT-style, "AS IS").
- Whether embedded or remote is the default deployment mode.

---

## 8. Why the fit is clean

Phronexus and DishtaYantra are **complementary, not overlapping**: a compute/pipeline
orchestrator in front of a contract-driven operational store. They already share a
transport (Kafka) and a domain (trades / FX / repos; enterprise messaging). The
integration is therefore an **adapter effort**, not a rewrite — and it reuses the
verbs Phronexus already ships.
