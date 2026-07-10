from __future__ import annotations

import pytest

from phronexus.errors import ValidationError

GOOD = {"trade_id": "T-1", "counterparty": "GS", "notional": 1_000_000.0, "ccy": "USD", "trade_date": 20250115, "book": "R"}


def test_valid_document_passes(px):
    report = px.validate("trade", GOOD)
    assert report.ok and report.errors == []
    assert px.put("trade", GOOD) == "T-1"


def test_schema_missing_required_field_rejected(px):
    bad = {k: v for k, v in GOOD.items() if k != "ccy"}          # drop required ccy
    report = px.validate("trade", bad)
    assert not report.ok and any("ccy" in e for e in report.errors)
    with pytest.raises(ValidationError):
        px.put("trade", bad)
    assert px.get("trade", "T-1") is None                         # nothing written


def test_schema_wrong_type_rejected(px):
    report = px.validate("trade", {**GOOD, "notional": "not-a-number"})
    assert not report.ok and any("notional" in e for e in report.errors)


def test_dq_expression_check(px):
    report = px.validate("trade", {**GOOD, "notional": -5})
    assert not report.ok and any("notional_positive" in e for e in report.errors)


def test_dq_allow_list_check(px):
    report = px.validate("trade", {**GOOD, "ccy": "ZZZ"})
    assert not report.ok and any("ccy_supported" in e for e in report.errors)


def test_dq_warning_does_not_block(px):
    # notional_reasonable is a warn-severity check; exceeding it warns, not errors.
    doc = {**GOOD, "trade_id": "T-big", "notional": 5e12}
    report = px.validate("trade", doc)
    assert report.ok and any("notional" in w for w in report.warnings)
    assert px.put("trade", doc) == "T-big"                        # still written


def test_warn_only_mode_downgrades_errors(px):
    vc = px.registry.active_validation("trade").model_dump(mode="json", by_alias=True)
    vc["version"] = 2
    vc["mode"] = "warn_only"
    px.publish_contract(vc)
    report = px.validate("trade", {**GOOD, "ccy": "ZZZ"})          # would be an error under enforce
    assert report.ok and report.warnings                          # downgraded, not blocking


def test_entity_without_validation_contract_is_ok(px):
    # fx_spot has no validation contract.
    report = px.validate("fx_spot", {"deal_id": "F1"})
    assert report.ok


def test_state_machine_rejects_invalid_event(px):
    from phronexus.statemachine import InputEvent
    sm = px.state_machine()
    # Book with an unsupported ccy -> DQ error -> rejected, nothing committed.
    ev = InputEvent(entity="trade", event_type="TradeBooked", key="T-1",
                    payload={**GOOD, "ccy": "ZZZ"}, event_id="e1")
    r = sm.process(ev)
    assert r.status == "rejected" and "validation" in r.reason
    assert px.get("trade", "T-1") is None
