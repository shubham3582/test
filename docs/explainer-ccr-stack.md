# The Data Stack Behind Counterparty Credit Risk
### Kafka, Aerospike, and Iceberg — and why they matter for CCR

*A briefing document for an explainer video. Written to be read aloud: plain
language first, precise detail second, with analogies you can lean on.*

---

## Part 1 — The problem: CCR is a data problem, not just a maths problem

**Counterparty Credit Risk (CCR)** answers one deceptively simple question: *if a
bank's trading counterparty defaulted right now, how much money is at risk?* A big
bank trades with thousands of counterparties across millions of trades, so it must
recompute this exposure continuously, all day, as markets move.

Everyone assumes the hard part is the *maths* — the quant models that price trades
and simulate future exposure. That part is genuinely hard. But there's a second,
quieter problem that decides whether the whole system actually works in production:
the **data problem**.

To calculate exposure you have to:

- **Ingest** a torrent of live events — new trades, amendments, market data,
  netting agreements — from dozens of systems that don't agree on formats.
- **Serve** the latest state of every trade and exposure to a fleet of calculators
  in *milliseconds*, because the whole risk cube gets recomputed many times a day.
- **Remember** everything, forever, in a way a regulator can reproduce — "show me
  exactly what you knew at 9am yesterday, and prove nobody changed it since."

No single database is good at all three. So modern risk platforms use **three
specialized technologies**, each doing the one job it's best at: **Kafka** to move
the events, **Aerospike** to serve the now, and **Iceberg** to remember forever.
Think of them as the **nervous system**, the **short-term memory**, and the
**long-term memory** of the risk platform.

---

## Part 2 — The three technologies, in plain language

### Kafka (here, Redpanda) — the nervous system

**What it is:** a distributed, durable **event log**. Systems publish events to
named "topics"; other systems subscribe and read them, at their own pace, without
the publisher ever knowing who's listening.

**The analogy:** a conveyor belt in a factory, or the body's nervous system. Every
time something happens — a trade is booked, an exposure is recalculated — a signal
travels down the belt. Anyone who cares can tap the belt and react. Nobody has to
call anybody directly.

**Why it matters here:**
- **Decoupling.** The trade-capture system doesn't need to know the risk engine
  exists. It just says "trade booked" onto the belt. This lets a bank wire dozens
  of systems together without a tangle of point-to-point connections.
- **Durability & replay.** The belt keeps the events. If a calculator crashes, it
  restarts and *replays* from where it left off — nothing is lost.
- **Back-pressure.** A slow consumer doesn't slow down a fast producer; the log
  absorbs the difference.

*(Redpanda is a drop-in, Kafka-compatible engine — same idea, lighter to run. In
this platform it carries the "change feed": every committed document emits an event
onto the belt for downstream consumers.)*

### Aerospike — the short-term memory (the hot store)

**What it is:** a **key-value database built for speed and scale** — sub-millisecond
reads and writes, holding data in memory (RAM) and on fast flash, across many
machines, without slowing down as it grows.

**The analogy:** a trader's desk, or your brain's working memory. It holds *what's
true right now* and hands it to you instantly. You don't file-search for it; it's
already at your fingertips.

**Why it matters here:**
- **Serving speed.** The risk cube is recomputed constantly, and every computation
  needs the *current* state of trades and exposures — right now, at massive fan-out.
  Aerospike answers those reads in microseconds.
- **Scale without slowdown.** Millions of trades, thousands of counterparties — it
  stays fast because it's designed to spread data evenly across a cluster.
- **The "hot cube."** The freshly-computed exposure numbers that the desk and
  downstream services hit thousands of times a second live here, ready to serve.

**The catch:** hot memory is expensive, and by itself it only tells you *the
present*. It doesn't naturally keep a perfect, cheap, immutable history. That's the
third technology's job.

### Iceberg (on object storage like S3 / MinIO) — the long-term memory

**What it is:** an open **table format** that turns a pile of files in cheap cloud
storage into a proper, queryable, versioned table — with schema evolution,
time-travel, and transactional guarantees.

**The analogy:** the national archive, or a library with a perfect card catalogue.
Cheap, vast, permanent, and organized so you can pull *exactly* the record you want
from any point in history.

**Why it matters here:**
- **Cheap history at scale.** You cannot keep years of every trade version in
  expensive RAM. You move the cold, older data down to object storage — pennies per
  gigabyte — while keeping it fully queryable.
- **Reproducibility & audit.** Regulators demand you reproduce a past risk report
  *exactly*. Iceberg's snapshots and time-travel let you query the table "as of"
  a past moment.
- **Analytics.** Data scientists and model-validation teams can run big historical
  queries over Iceberg with standard tools, without ever touching the hot serving
  path.

---

## Part 3 — How they work together: the hot/cold data plane

Here's the key insight: **these three aren't competitors — they're a pipeline.**
Data flows through them in a lifecycle:

```
   events in            serve the "now"           age out the "past"
  ───────────▶  KAFKA  ──────────▶  AEROSPIKE  ──────────▶  ICEBERG
  (nervous system)    (hot memory)                (long-term memory)
        ▲                    │                            │
        └──── change feed ───┘  every commit emits an event that
                               fans out to consumers AND to retention
```

1. **Kafka** carries every inbound event *in* and every change *out*.
2. **Aerospike** holds the live, hot state and serves it at speed. Every write it
   commits also drops a "this changed" event back onto Kafka.
3. A **retention** process reads that change feed and writes the history down into
   **Iceberg** — an insert-only, replay-safe log. Hot data that's gone cold is
   tiered down, freeing expensive memory while keeping the full record.

So the *same* event that updates the hot store also feeds the permanent archive.
Nothing is bolted on after the fact; history is a **byproduct of normal operation**.

---

## Part 4 — A day in the life of a trade (a concrete walkthrough)

Follow one trade — call it **CCR-T-1**, a \$25 million interest-rate swap with the
counterparty **Goldman Sachs**, in netting set **NS-GS-USD**:

1. **A "TradeReceived" event lands on Kafka.** The trade-capture system published
   it; the risk platform is subscribed.
2. **It's validated and stored in Aerospike.** The counterparty is checked against
   reference data ("is GS actually onboarded?"), the trade shape is validated, and
   it's committed to the hot store as `state: active`. That commit emits a change
   event back onto Kafka.
3. **Calculators pick it up and price it.** They read the current trade state from
   Aerospike (fast), compute an exposure — say **\$3.1 million** — and write the
   result back. All served from hot memory.
4. **The market moves; recalculation happens intraday.** A new exposure version —
   **\$3.4 million** — is written *for the same business day*, stamped with a later
   timestamp. Aerospike now serves the new number, but the platform can still answer
   "what did we think this morning?" (\$3.1M) — this is **bitemporal** storage:
   every fact carries both a *business date* and a *system time*.
5. **Netting.** The platform sums the exposures of all trades facing GS in that
   netting set to produce a single **netted exposure** — the number that actually
   matters for credit limits.
6. **End of day, the data tiers down.** The day's trade versions and exposures are
   written into **Iceberg**. Months later, an auditor can ask "show me GS exposure
   as of that date" — and get the exact, immutable answer.

Every step was an event on Kafka, served from Aerospike, and archived to Iceberg.

---

## Part 5 — Why this specifically matters for CCR

CCR has a few brutal requirements that map *directly* onto the three technologies:

- **Intraday, continuous recalculation** → needs Kafka (a live event stream) and
  Aerospike (fast serving to a fleet of calculators).
- **COB / "close of business" reproducibility** → needs bitemporal storage: any
  report can be reproduced *as of* a past (business date, system time). Later
  corrections never silently rewrite the past.
- **Regulatory audit & lineage** → needs Iceberg's immutable, time-travel history,
  plus a tamper-evident trail from source event → calculation → result.
- **Netting across thousands of trades** → needs fast fan-out reads and an index to
  find "all trades in this netting set" instantly (Aerospike + an inverted index).
- **Massive scale, low latency** → the reason a general-purpose database won't do,
  and Aerospike will.

Miss any one of these and the risk number is either *too slow to be useful*, or
*impossible to defend to a regulator*. The three-technology stack exists precisely
because CCR needs all of them at once.

---

## Part 6 — Where Phronexus fits

There's a clean division of labor worth stating plainly:

> **The quant engine computes the risk. Phronexus governs and serves the data.**

Phronexus is the layer that **orchestrates these three technologies so teams don't
have to wire them together by hand**. It provides:

- **Contract-driven onboarding** — you describe an entity (a trade, an exposure) in
  a small config file, and Phronexus handles the storage layout in Aerospike, the
  event schemas on Kafka, and the retention into Iceberg. No bespoke plumbing code.
- **Governed change** — schemas and message formats are versioned and go through a
  draft → approve → publish workflow, with a hash-chained audit history.
- **Message validation on both edges** — inbound messages are checked before they
  touch state; outbound messages are shaped and validated before they're published.
- **Bitemporal, reproducible reads** — the "as of" guarantees CCR lives or dies on.

In short: the maths team owns the model; Phronexus makes the operational data around
it **fast, governed, reproducible, and auditable** — using Kafka, Aerospike, and
Iceberg as its engine.

---

## Key terms (glossary for the narration)

- **CCR (Counterparty Credit Risk):** the risk that a trading counterparty defaults
  before settling what they owe; measured as *exposure*.
- **Exposure:** how much you'd lose if a counterparty defaulted now.
- **Netting set:** a group of trades with one counterparty whose exposures legally
  offset, so only the *net* amount is at risk.
- **COB (Close of Business):** the official daily cut-off; reports are produced
  "as of" a COB and must be reproducible.
- **Bitemporal:** every fact is stamped with two clocks — the *business date* it
  applies to and the *system time* it was recorded — so the past is never rewritten.
- **Change feed:** the stream of "this record changed" events every commit emits.
- **Hot / cold tiering:** keeping recent data in fast, costly memory (Aerospike) and
  aging older data down to cheap, permanent storage (Iceberg).

---

## Soundbites (for the video's hooks)

- "Everyone thinks risk is a maths problem. In production, it's a data problem."
- "Kafka is the nervous system, Aerospike is the short-term memory, Iceberg is the
  long-term memory."
- "The same event that updates the present also writes the past — history is a
  byproduct of normal operation, not an afterthought."
- "A regulator doesn't ask 'what do you think now?' They ask 'prove what you knew
  then.' Bitemporal storage is how you answer."
- "The quant engine computes the risk. The data platform makes that number fast,
  governed, and defensible."

---

## One-paragraph summary (if the video needs a cold open)

> Counterparty credit risk is really a data problem: a bank must ingest a firehose
> of trade events, serve the latest risk numbers to an army of calculators in
> milliseconds, and remember everything forever in a way a regulator can reproduce.
> No single database does all three, so the modern stack uses three specialists —
> **Kafka** to move the events, **Aerospike** to serve the now, and **Iceberg** to
> remember forever. Phronexus ties them together with versioned contracts and
> bitemporal, governed data, so the risk team can focus on the maths while the
> platform keeps the numbers fast, reproducible, and auditable.
