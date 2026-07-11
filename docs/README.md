# Phronexus Core — Documentation

| Doc | What it covers |
|---|---|
| [architecture.md](architecture.md) | The mental model, components, the manifest write path, and the state machine — with diagrams. Start here. |
| [building-on-phronexus.md](building-on-phronexus.md) | Developer guide: onboard a new entity by config end-to-end, extend with hooks, use the REST API/SDK, evolve contracts, test. |
| [contracts-reference.md](contracts-reference.md) | Field-by-field reference for all five contract kinds (storage, query, view, validation, transition). |
| [state-machine.md](state-machine.md) | The transactional state machine in depth: atomicity model, hooks, output routing, the three faces. |
| [deployment.md](deployment.md) | Production: Aerospike / MSK / S3 Tables, TLS/mTLS, auth, observability, operations, scaling. |
| [integration-dishtayantra.md](integration-dishtayantra.md) | Integrating Phronexus into a DishtaYantra DAG as library components. |

## Quick links

- **Run the worked example:** `python examples/bond/run_bond.py`
- **Contracts for the example:** [`examples/bond/`](../examples/bond)
- **Config templates:** [`config/`](../config)
- **Tests (usage patterns):** [`tests/`](../tests)
