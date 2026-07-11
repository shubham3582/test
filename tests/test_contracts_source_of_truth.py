from __future__ import annotations

import json

from phronexus.cli import main as cli_main
from phronexus.contracts.registry import ContractRegistry


def test_fresh_registry_reads_from_store_not_files(px):
    # px ingested contracts_examples into its store. A brand-new registry over the
    # SAME store (i.e. a new process pointed at the same Aerospike) sees them —
    # with no access to the source files.
    fresh = ContractRegistry(px.store, contracts_set=px.settings.aerospike.contracts_set)
    assert fresh.active_storage("trade").version == 1
    assert "storage:trade:v1" in fresh.list_contracts()["contracts"]
    fresh.close()


def test_list_and_get_contract(px):
    listing = px.list_contracts()
    assert "storage:trade:v1" in listing["contracts"]
    assert listing["active"]["active:storage:trade"] == "storage:trade:v1"
    doc = px.get_contract("storage:trade:v1")
    assert doc["entity"] == "trade" and doc["kind"] == "storage"


def test_cli_ingest_publishes_directory(monkeypatch, capsys):
    monkeypatch.setenv("PHRONEXUS_BACKEND", "memory")
    monkeypatch.setenv("PHRONEXUS_OBSERVABILITY__LOG_LEVEL", "ERROR")
    rc = cli_main(["ingest", "examples/bond"])
    out = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert "storage:bond:v1" in out["ingested"] and out["count"] >= 5
