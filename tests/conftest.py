from __future__ import annotations

import pytest

from phronexus import Phronexus, Settings


@pytest.fixture()
def px() -> Phronexus:
    settings = Settings(backend="memory")
    settings.observability.log_level = "WARNING"
    p = Phronexus(settings)
    p.load_contract_dir("contracts_examples")
    yield p
    p.close()


@pytest.fixture()
def sample_trade() -> dict:
    return {
        "trade_id": "T-1001",
        "counterparty": "GS",
        "notional": 1_000_000.005,
        "ccy": "USD",
        "trade_date": 20250115,
        "book": "RATES-1",
    }
