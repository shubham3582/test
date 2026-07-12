# Phronexus Core — Documentation

| Doc | What it covers |
|---|---|
| [architecture.md](architecture.md) | The mental model, components, the manifest write path, and the state machine — with diagrams. Start here. |
| [building-on-phronexus.md](building-on-phronexus.md) | Developer guide: onboard a new entity by config end-to-end, extend with hooks, use the REST API/SDK, evolve contracts, test. |
| [api-reference.md](api-reference.md) | The verbs at a glance: Python (`Phronexus`), REST endpoints, and the remote SDK. |
| [contracts-reference.md](contracts-reference.md) | Field-by-field reference for all six contract kinds (storage, query, view, validation, transition, stream). |
| [storage-layouts.md](storage-layouts.md) | Physical storage options: `map`/`msgpack`/`bins` encoding, `bin_map`, `spread`, `native_txn`, and batch reads — with worked layouts. |
| [state-machine.md](state-machine.md) | The transactional state machine in depth: atomicity model, hooks, output routing, the three faces. |
| [ccr-reference.md](ccr-reference.md) | End-to-end reference: Counterparty-Credit-Risk saga, the hot value cube, and the exactly-once scheduler — all as config. |
| [retention-and-journals.md](retention-and-journals.md) | Insert-only, idempotent retention log (self-reconciling on replay) and the binary msgpack journals for messages and request/response pairs. |
| [deployment.md](deployment.md) | Production: Aerospike / MSK / S3 Tables, TLS/mTLS, auth, observability, operations, scaling. |
| [logging.md](logging.md) | The one place to change log format/level/fields for the whole platform (structlog). |
| [packaging.md](packaging.md) | Build the wheel, consume it as a library (extras matrix), and produce an offline/air-gapped install bundle. |
| [integration-dishtayantra.md](integration-dishtayantra.md) | Integrating Phronexus into a DishtaYantra DAG as library components. |
| [product-positioning.md](product-positioning.md) | *Internal strategy note* — positioning, the moat vs. commodity, and a moat/keep-thin/expose-native/cut scope table. |

## Quick links

- **Run the worked example:** `python examples/bond/run_bond.py`
- **Run the CCR reference:** `python examples/ccr/run_ccr.py` (in-memory by default; `PHRONEXUS_BACKEND=aerospike` runs it against a live stack)
- **Contracts for the example:** [`examples/bond/`](../examples/bond), [`examples/ccr/`](../examples/ccr)
- **Config templates:** [`config/`](../config)
- **Tests (usage patterns):** [`tests/`](../tests)
