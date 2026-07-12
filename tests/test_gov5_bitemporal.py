"""GOV5 — bitemporal reproducibility.

A read as-of (valid_time, tx_time) is stable: later writes never change a past
as-of result, and a backdated correction is only visible at/after the tx-time it
was recorded. The valid-time defaults to the environment COB.
"""

from __future__ import annotations

import time

import pytest

from phronexus import Phronexus, Settings

CONTRACT = {
    "kind": "storage", "entity": "price", "version": 1, "primary_key": ["sym"],
    "manifest_set": "price_manifest", "temporal": "bitemporal", "valid_time_field": "as_of",
    "projections": [{"name": "main", "set": "price_main", "key": "{sym}",
                     "fields": ["*"], "canonical": True}],
}


def _px():
    s = Settings(backend="memory")
    s.observability.log_level = "WARNING"
    s.audit.enabled = False
    px = Phronexus(s)
    px.publish_contract(CONTRACT)
    return px


@pytest.mark.governance
def test_valid_time_asof_selects_effective_version():
    px = _px()
    px.put("price", {"sym": "AAPL", "as_of": 20250110, "px": 100.0})
    px.put("price", {"sym": "AAPL", "as_of": 20250115, "px": 110.0})

    assert px.get("price", "AAPL", as_of=20250112)["px"] == 100.0   # 110 not yet effective
    assert px.get("price", "AAPL", as_of=20250116)["px"] == 110.0   # latest effective
    assert px.get("price", "AAPL", as_of=20250101) is None          # nothing effective yet


@pytest.mark.governance
def test_past_asof_is_invariant_under_later_writes():
    px = _px()
    px.put("price", {"sym": "MSFT", "as_of": 20250110, "px": 50.0})
    tt = time.time()
    time.sleep(0.01)
    px.put("price", {"sym": "MSFT", "as_of": 20250110, "px": 55.0})  # a later correction

    # As known at tx-time tt (before the correction) the value is still 50 ...
    assert px.get("price", "MSFT", as_of=20250110, tx_as_of=tt)["px"] == 50.0
    # ... and the current view reflects the correction.
    assert px.get("price", "MSFT", as_of=20250110)["px"] == 55.0


@pytest.mark.governance
def test_backdated_correction_visible_only_after_its_tx_time():
    px = _px()
    px.put("price", {"sym": "IBM", "as_of": 20250115, "px": 200.0})
    tt = time.time()
    time.sleep(0.01)
    px.put("price", {"sym": "IBM", "as_of": 20250110, "px": 190.0})  # backdated correction

    # At tx-time tt nothing was effective for valid-date 20250112 -> None.
    assert px.get("price", "IBM", as_of=20250112, tx_as_of=tt) is None
    # Now the backdated correction is visible for that valid-date.
    assert px.get("price", "IBM", as_of=20250112)["px"] == 190.0


@pytest.mark.governance
def test_read_defaults_asof_to_cob():
    px = _px()
    px.governance.set_cob("ops", 20250112)
    px.put("price", {"sym": "GOOG", "as_of": 20250110, "px": 10.0})
    px.put("price", {"sym": "GOOG", "as_of": 20250120, "px": 20.0})
    # No explicit as_of -> defaults to COB 20250112 -> the 20250110 version.
    assert px.get("price", "GOOG")["px"] == 10.0
