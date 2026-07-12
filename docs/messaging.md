# Messaging: creating & validating messages with JSON Schema

Phronexus uses JSON Schema on **both** edges of the state machine:

- **`ingress`** — validates messages *arriving* (before a transition runs).
- **`stream`** — validates messages *leaving* (at produce time, before publish).

Both live in the one versioned, governed schema store (authored via the
draft→approve→publish flow, same as any contract — see [governance.md](governance.md)).

> **Mental model.** The JSON Schema does **not** generate a message — it *defines
> and validates* its shape. A message is **created** by shaping fields from a
> document, then **validated** against the schema. Schema = the contract; shaping
> = the constructor.

---

## What "validate" does

Validation checks a payload against its JSON Schema (Draft 2020-12) and returns a
`ValidationReport(ok, errors, warnings)`. What happens on a failure depends on the
contract's **`mode`** and on **which edge** the check runs at.

**Modes** (set per contract):

| `mode` | Effect |
|---|---|
| `enforce` (default) | a violation **blocks** — see per-edge outcome below |
| `warn_only` | violations are downgraded to **warnings**; never blocks (logged + counted) |
| `off` | validation is **skipped** entirely |

**Outcome by edge** (under `enforce`):

| Edge | Contract | On failure |
|---|---|---|
| Inbound message | `ingress` | the message is **rejected at the boundary** before any transition — nothing touches state, and the runner **dead-letters** it |
| Outbound message | `stream` | the transition is **rejected** — nothing is committed and nothing is published (the malformed message never reaches the wire) |
| Stored document | `validation` | the **write is blocked** (`ValidationError`); the state change is aborted |

In every case the check is **standalone-callable** without side effects, returning
a report you can inspect:

```python
rep = px.validate_event("ccr_trade", "DisplayUpdate", msg)   # outbound message
rep = px.validator.validate_inbound("ccr_trade", "TradeReceived", payload)  # inbound message
rep = px.validate("ccr_trade", document)                     # stored document (schema + DQ)
rep.ok          # bool — did it pass?
rep.errors      # list[str] — schema/DQ failures (empty if ok)
rep.warnings    # list[str] — warn_only downgrades and DQ warnings
```

An event type with **no** registered schema passes (validation is opt-in per
type). `validate` is pure: it never writes, publishes, or mutates state — it only
tells you whether a payload conforms.

---

## Creating an outbound message

### 1. Declarative — the recommended, governed way

Define the message shape once with a `stream` contract, and let a transition
`emit` create it. The state machine builds the payload and validates it
automatically — no code.

```yaml
# stream contract — the message's JSON Schema
kind: stream
entity: ccr_trade
version: 1
mode: enforce
events:
  - type: DisplayUpdate
    json_schema:
      type: object
      required: [trade_id, counterparty, status]
      properties:
        status: {enum: [published, cancelled]}
      additionalProperties: false
```

```yaml
# transition emit — CREATE the message by shaping SPECIFIC fields of the document
transitions:
  - event: CalcComplete
    from: calc_requested
    to: published
    emit:
      - topic: kafka://ccr.display
        type: DisplayUpdate
        fields: [trade_id, counterparty, status]   # pick exactly what the consumer needs
        transform: {counterparty: upper}           # optional per-field transform
        # rename: {trade_id: id}                    # optional output-key rename
```

At produce time the engine runs `fields → transform → rename` to build the
payload, validates it against the `DisplayUpdate` schema, and publishes. A message
that doesn't conform **rejects the transition** — nothing is committed or
published. Omitting `fields`/`transform`/`rename` emits the whole document.

**Keep shaped keys and the schema in sync.** If you `rename` a field, the schema
must require the *renamed* key. Renaming `trade_id → id` while the schema still
requires `trade_id` fails validation.

### 2. Programmatic — as a library

Build and validate a message yourself:

```python
from phronexus import Phronexus, Settings
from phronexus.contracts.models import EmitSpec
from phronexus.statemachine.emit import build_emit_payload

px = Phronexus(Settings(backend="memory"))
# ... contracts published, document written ...

doc  = px.get("ccr_trade", "CCR-T-1")
spec = EmitSpec(topic="kafka://ccr.display", type="DisplayUpdate",
                fields=["trade_id", "counterparty", "status"],
                transform={"counterparty": "upper"})

msg = build_emit_payload(spec, doc)                       # -> create the message
#   {'trade_id': 'CCR-T-1', 'counterparty': 'GS', 'status': 'published'}

report = px.validate_event("ccr_trade", "DisplayUpdate", msg)   # -> validate it
assert report.ok
# then publish msg via your OutputPublisher (Kafka / HTTP / …)
```

### 3. Fetch the schema first (know the shape)

```python
# outbound (stream) schema for one event type
px.registry.active_stream("ccr_trade").schema_for("DisplayUpdate")
#   {'type': 'object', 'required': ['trade_id', 'counterparty', 'status'], ...}

# document (validation) schema, versioned — the schema-registry surface
px.schema("ccr_trade")                    # active
px.schema("ccr_trade", version=1)         # a specific version
```

Over REST: `GET /entities/{entity}/schema[?version=]` for the document schema;
the stream/ingress schemas are visible in the UI under **Contracts → stream /
ingress**, or via `GET /contracts/{identity}`.

---

## Validating an inbound message

The mirror image. An `ingress` contract registers a JSON Schema per **inbound**
event type; the message is validated at the state-machine boundary before the
transition runs. A failure is rejected at ingress and dead-lettered — it never
drives a state change.

```yaml
kind: ingress
entity: ccr_trade
version: 1
mode: enforce
events:
  - type: TradeReceived
    json_schema:
      type: object
      required: [trade_id, counterparty, netting_set_id, notional, currency]
      properties:
        notional: {type: number, exclusiveMinimum: 0}
      additionalProperties: true
```

```python
# standalone (dry-run) inbound validation
px.validator.validate_inbound("ccr_trade", "TradeReceived", payload)   # -> ValidationReport
```

An inbound event type with **no** registered schema passes (opt-in per type).

---

## The two-stage gauntlet

A message runs a full gauntlet from wire to state and back:

```
decode (transport; poison → DLQ)
   → ingress schema (inbound)      ← this doc
      → transition + guard
         → stream schema (outbound; shaped payload)   ← this doc
            → commit + publish
```

- `ingress` / `stream` — **message** schemas (per event type), on the edges.
- `validation` — the **document** schema + data-quality checks, at the write
  boundary (after the event is merged into state). See
  [contracts-reference.md](contracts-reference.md#validation).

---

## Reference

- Field-by-field: [`stream`](contracts-reference.md#stream) ·
  [`ingress`](contracts-reference.md#ingress) ·
  [`transition` / EmitSpec](contracts-reference.md#transition)
- Pipeline in depth: [state-machine.md](state-machine.md#inbound-message-validation)
- Worked example: [`examples/bond/`](../examples/bond) (shaped emit + ingress) and
  [`examples/ccr/`](../examples/ccr)
