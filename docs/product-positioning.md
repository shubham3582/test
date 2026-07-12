# Phronexus — Positioning & Scope Memo

> Internal strategy note (not user documentation). A candid read on what this is
> as a *product*, where the moat is, and how to keep scope from eating the team.

## Positioning (one sentence)

**A contract-governed data & lifecycle layer for regulated, low-latency domains** —
onboard a new business entity (trade, FX, CCR) by publishing versioned contracts,
and get atomic multi-shape storage, validation/lineage, consumer views, and
retention *by declaration, not code*. Aerospike (hot) and Iceberg/S3 Tables (cold)
are implementation details, not the pitch.

- **Who it's for:** a capital-markets / risk platform team that needs governance +
  latency + auditability and already runs (or will adopt) Aerospike Enterprise.
- **The job it does:** collapse "stand up a governed, indexed, versioned,
  audited store + change feed + saga for a new entity" from a project into a config change.
- **How it's delivered:** *two shapes from one core* — an **embeddable library**
  (`pip install`, in-process, no services) **and** a **running service** (REST + console
  + workers). Same contracts, same engine. This dual shape is a strategic asset (below).
- **What it is *not* (say this out loud):** a general-purpose database, a data lake,
  or a workflow engine. It orchestrates those; it should not try to *be* them.

## The moat is the contract model — not the plumbing

Anyone can wrap Aerospike. The durable IP is the **six-contract vocabulary plus the
evolution discipline**: compatibility gates, versioned active-pointers, backfill,
and the manifest pattern that makes a multi-record write atomically visible on a
KV store. That governance-as-config story is the thing to invest in, harden, and
market. Everything else is a cost center to be minimized.

## Two shapes: embeddable library and running service

The same core ships as a **library** (`phronexus-core` wheel; hold a `Phronexus`
object and call the verbs) and as a **service** (the REST API, console, and the
retention/audit/scheduler workers). This is one of the stronger, and currently
under-marketed, product levers.

**Why the library shape is a wedge:**
- **Near-zero adoption barrier.** `pip install`, in-memory backend, no services to
  stand up — a team can embed it into an existing app or a DishtaYantra DAG node in
  an afternoon. Adoption is incremental, not a platform migration.
- **It goes where the compute is.** As a `CalculationNode` / pub-sub backend it runs
  *inside* the host's process model (GIL-free multiprocessing: one `Phronexus` per
  worker, its own connections) — the store meets the data in flight instead of a
  network hop away.
- **Air-gap / regulated friendly.** A wheel + an offline bundle (`--no-index`) is a
  distribution model banks accept; a hosted service often isn't.
- **Same governance travels with it.** Contracts live in the store, so an embedded
  library and the central service read the *same* source of truth — you don't lose
  governance by embedding.

**The tension to manage (this is the important part):**
- Governance is strongest as a **service with a central control plane** (one contract
  registry, one place to enforce compatibility, audit, RBAC). A library scattered
  across many host apps risks **decentralized drift** — many processes, many
  connection pools, many versions of the wheel, no single throat to choke.
- Resolution: **the library is the data/compute plane; the service is the control
  plane.** Contracts, policy, and audit are owned centrally (the store + the API);
  the library is a *client* of that governance, not a fork of it. Make that boundary
  explicit so "embed anywhere" doesn't become "govern nowhere."

**Product implications:**
- The **in-memory backend and the wheel/offline packaging are first-class product
  surface**, not test scaffolding — they are the on-ramp.
- The **API-reference (verbs) is the library's product**; SDK/REST parity matters
  more than it looks (an embedder shouldn't hit a "Python-only" wall).
- Version/dependency hygiene becomes a real obligation: pin ranges, a clean extras
  matrix, and reproducible offline bundles (already started) are table stakes when
  other teams depend on your wheel.

## Scope buckets

| Capability | Bucket | Why |
|---|---|---|
| 6-kind contracts + versioning/evolution/backfill/compat gate | **MOAT — invest** | the actual product; hardest to copy; the regulated-finance wedge |
| Manifest atomic-visibility pattern | **MOAT — invest** | correctness core; the reason it can sit on a KV store |
| Validation (JSON Schema + DQ + cross-entity references) | **MOAT — invest** | governance at the write boundary is the buyer's pain |
| Consumer views (allow-list / mask / transform) | **MOAT — invest** | data-sharing governance; cheap to own, high value |
| Audit / trace + interaction journals + lineage | **MOAT — invest** | "prove what happened" is table stakes in regulated shops |
| State machine / saga | **KEEP THIN** | own the *contract* for transitions; consider delegating execution to a real engine (Temporal/Step Functions) rather than growing an orchestrator |
| Distributed scheduler | **KEEP THIN or DEFER** | valuable but generic; EventBridge/K8s CronJob may suffice; don't over-build |
| Inverted index + query engine | **RECONSIDER vs NATIVE** | you're building a query planner; evaluate Aerospike secondary indexes / expressions before deepening this |
| Batch read/write | **KEEP THIN** | thin wrappers over native batch — correct scope already |
| Change feed | **EXPOSE NATIVE** | it's Kafka/MSK; don't reinvent CDC, just relay `CommitEvent` cleanly |
| Retention to Iceberg / S3 Tables | **EXPOSE NATIVE** | it's a lake; be a well-behaved producer, not a lake |
| Native-Aerospike accessor (escape hatch) | **EXPOSE NATIVE — keep** | the right instinct; lets users reach past the abstraction safely |
| REST API + remote SDK | **KEEP** | necessary product surface; but SDK should reach parity or defer to REST |
| Management console UI | **NICE-TO-HAVE / minimal** | great for demos and ops; not a moat — don't let it grow into an app |
| OTel metrics / structured logging | **KEEP** | operability is required to be taken seriously in prod |
| DishtaYantra adapters | **KEEP THIN** | integration surface; a thin SPI, swap their base classes when confirmed |
| In-memory backend + wheel / offline (air-gap) packaging | **KEEP — the on-ramp** | this *is* the library adoption wedge; treat as product surface, keep reproducible |
| Library verbs + SDK/REST parity | **KEEP — the library's product** | embedders live here; parity avoids a "Python-only" wall |
| Central control plane (contract registry, policy, audit, RBAC) | **MOAT — invest** | the service side that keeps "embed anywhere" from becoming "govern nowhere" |

Rule of thumb: **MOAT rows get depth; everything else gets a clean interface and
the smallest possible implementation.** When in doubt, expose the native thing.

## The two risks that decide product vs. prototype

1. **Build-vs-buy creep.** Re-implementing indexing, query planning, transactions,
   and orchestration means owning database- and workflow-grade semantics with one
   team. The prototype already shows the tax (edge-case bugs surface fast). Mitigation:
   stay thin outside the moat; prefer native Aerospike/Kafka/Iceberg; delegate
   execution engines.
2. **Breadth over depth.** Ten features at 70% beats nothing, but ships nothing to
   production. Mitigation: pick **one flagship workload (CCR)** and take it to true
   production depth — SLOs, failure injection, backfill at scale, ops runbooks —
   before adding capability #11.

## Recommended sequencing

1. **Name the wedge** and rewrite the top-level pitch around governance + latency +
   audit (not "framework over Aerospike").
2. **Harden the moat**: contract evolution, compatibility, backfill, manifest under
   failure — with tests and chaos, not more features.
3. **Take CCR to production depth** on real MSK + S3 Tables; let that pull the roadmap.
4. **Draw the thin/native line** explicitly (this table) and hold it in code review.
5. Revisit the query engine vs. native secondary indexes before it grows further.
6. **Lead adoption with the library** (wheel + in-memory + a DishtaYantra node), but
   make the **library-as-client / service-as-control-plane** boundary explicit so
   governance stays centralized as embedding spreads.

## Bottom line

Strong core idea (contracts + manifest visibility), an excellent no-services
on-ramp, a clear high-value domain, and a genuinely differentiated **dual shape**
(embeddable library + governing service) that most competitors can't easily copy.
It becomes a *product* — not an impressive prototype — on two disciplines:
**ruthless scope** (be a thin governance layer, not a half-built database) and
**production depth on one workload** over feature breadth — while leading adoption
with the library and holding governance in the central control plane.
The bugs found in prototyping aren't the concern; the pattern they reveal —
database-grade semantics owned by one team — is what to design around.
