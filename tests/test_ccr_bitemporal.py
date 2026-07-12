"""CCR6/CCR7 — late & corrected events and COB reproducibility on the exposure.

exposure_result is bitemporal (valid_time_field=cob). A read as-of a COB is
reproducible: later writes never change a past COB's value, and a backdated
correction is only visible at/after the tx-time it was recorded.
"""

from __future__ import annotations

import time

import pytest

from ccrsupport import build_ccr


def _exposure(px, trade_id, cob, exposure):
    px.put("exposure_result", {"trade_id": trade_id, "cob": cob,
                               "exposure": exposure, "currency": "USD"})


# --- CCR7 -----------------------------------------------------------------

@pytest.mark.ccr
def test_exposure_asof_cob_selects_that_cob():
    px = build_ccr()
    _exposure(px, "T", 20260711, 100.0)
    _exposure(px, "T", 20260712, 150.0)
    assert px.get("exposure_result", "T", as_of=20260711)["exposure"] == 100.0
    assert px.get("exposure_result", "T", as_of=20260712)["exposure"] == 150.0
    px.close()


@pytest.mark.ccr
def test_past_cob_read_unchanged_by_later_correction():
    px = build_ccr()
    _exposure(px, "T2", 20260711, 100.0)
    tt = time.time()
    time.sleep(0.01)
    _exposure(px, "T2", 20260711, 120.0)  # a correction to the same COB, later
    # As known at tx-time tt the value is still 100 ...
    assert px.get("exposure_result", "T2", as_of=20260711, tx_as_of=tt)["exposure"] == 100.0
    # ... and the current view reflects the correction.
    assert px.get("exposure_result", "T2", as_of=20260711)["exposure"] == 120.0
    px.close()


@pytest.mark.ccr
def test_read_defaults_to_environment_cob():
    px = build_ccr()
    px.governance.set_cob("ops", 20260711)
    _exposure(px, "T4", 20260711, 50.0)
    _exposure(px, "T4", 20260712, 80.0)
    assert px.get("exposure_result", "T4")["exposure"] == 50.0  # defaults to COB
    px.close()


# --- CCR6 -----------------------------------------------------------------

@pytest.mark.ccr
def test_backdated_correction_visible_only_after_its_tx_time():
    px = build_ccr()
    _exposure(px, "T3", 20260712, 200.0)
    tt = time.time()
    time.sleep(0.01)
    _exposure(px, "T3", 20260711, 190.0)  # a LATE, backdated correction
    # At tx-time tt nothing was effective for COB 20260711 -> None.
    assert px.get("exposure_result", "T3", as_of=20260711, tx_as_of=tt) is None
    # Now the backdated correction is visible for that COB.
    assert px.get("exposure_result", "T3", as_of=20260711)["exposure"] == 190.0
    px.close()
