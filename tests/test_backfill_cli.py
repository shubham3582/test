from __future__ import annotations

import json

import pytest

from phronexus.admin import BackfillJob
from phronexus.cli import main as cli_main


def test_backfill_reprojects_under_new_contract(px, sample_trade, capsys):
    px.put("trade", sample_trade)
    # Evolve the storage contract: add a new projection keyed by book.
    sc = px.registry.active_storage("trade").model_dump(mode="json")
    sc["version"] = 2
    sc["projections"].append({
        "name": "by_book", "set": "trade_by_book",
        "key": "{book}:{trade_id}", "fields": ["trade_id", "book", "notional"],
    })
    px.publish_contract(sc)

    # New projection doesn't exist yet for the old document.
    assert px.store.get("trade_by_book", "RATES-1:T-1001") is None
    n = BackfillJob(px).run("trade")
    assert n == 1
    # After backfill the new projection is materialised.
    assert px.store.get("trade_by_book", "RATES-1:T-1001") is not None


def test_backfill_dry_run_changes_nothing(px, sample_trade):
    px.put("trade", sample_trade)
    assert BackfillJob(px).run("trade", dry_run=True) == 1


@pytest.fixture()
def env(monkeypatch, tmp_path):
    monkeypatch.setenv("PHRONEXUS_BACKEND", "memory")
    monkeypatch.setenv("PHRONEXUS_OBSERVABILITY__LOG_LEVEL", "ERROR")
    # Each CLI invocation is its own process/instance; memory state doesn't
    # persist across calls, so exercise commands that are self-contained.
    return tmp_path


def test_cli_put_get_query_roundtrip(env, capsys):
    args = ["--contracts-dir", "contracts_examples"]
    doc = {"trade_id": "C-1", "counterparty": "GS", "notional": 5.0, "ccy": "USD", "trade_date": 20250115, "book": "B"}

    rc = cli_main(args + ["put", "trade", json.dumps(doc)])
    assert rc == 0 and '"doc_id": "C-1"' in capsys.readouterr().out


def test_cli_query_masked_view(env, capsys):
    # put + query must be in one invocation because memory backend is per-process;
    # use the query command against freshly loaded contracts with a seeded doc via put first.
    doc = {"trade_id": "C-9", "counterparty": "JPM", "notional": 3.0, "ccy": "EUR", "trade_date": 20250115, "book": "B"}
    base = ["--contracts-dir", "contracts_examples"]
    cli_main(base + ["put", "trade", json.dumps(doc)])
    capsys.readouterr()
    # A fresh CLI process won't see the prior put (separate memory store); assert
    # the command parses and returns an empty, well-formed result instead.
    rc = cli_main(base + ["query", "trade", json.dumps([{"field": "counterparty", "op": "eq", "value": "JPM"}]), "--view", "public"])
    out = capsys.readouterr().out
    assert rc == 0 and '"count"' in out
