# Phronexus as a library (Aerospike + Iceberg)

The whole API — operational store **and** retention — embedded in another app
(e.g. a [DishtaYantra](../../docs/integration-dishtayantra.md) DAG), no server, no
services required.

```bash
python examples/library_embed/run_embed.py                              # in-memory + in-process Iceberg
PHRONEXUS_BACKEND=aerospike python examples/library_embed/run_embed.py   # real Aerospike
```

What it exercises, all as plain library calls:

1. **Operational store (the Aerospike face)** — `put` / `get` / `query` / `find`.
2. **State machine as a calculator node** — `PhronexusStateMachineNode.calculate(record)`
   maps a DAG record to a transactional transition. Every event's **request +
   response (incl. the full emitted events)** is captured in the interaction
   journal; read it with `px.request_journal().read(event_id)` or
   `GET /interactions/{event_id}`.
3. **Query/enrichment calculator** — `PhronexusQueryCalculator` joins a DAG record
   with looked-up store data (`key_from` / `pattern` / `where`, `enrich` | `replace`).
4. **Retention → Iceberg** — a `RetentionWorker` drains the change feed into a
   warehouse in-process (`InMemoryWarehouse`); set `iceberg.backend = iceberg` to
   target a real Iceberg catalog + S3 with the same code.

The adapters live in `phronexus/statemachine/node.py`; both the operational and
retention paths are ordinary Python objects, so an embedding process just holds a
`Phronexus` (its own Aerospike/Kafka connections — one per worker) and calls into it.
