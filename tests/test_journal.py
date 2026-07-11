"""Binary journals: msgpack round-trip + request/response persistence."""

from __future__ import annotations

import pytest

from phronexus import Phronexus, Settings, codec
from phronexus.journal import MessageJournal
from phronexus.statemachine import InputEvent, MemoryOutputPublisher


@pytest.fixture()
def px() -> Phronexus:
    settings = Settings(backend="memory")
    settings.observability.log_level = "WARNING"
    settings.journal.enabled = True
    settings.journal.journal_requests = True
    p = Phronexus(settings)
    p.load_contract_dir("contracts_examples")
    yield p
    p.close()


# --- codec ----------------------------------------------------------------

def test_codec_round_trip():
    obj = {"trade_id": "T-1", "qty": 5, "nested": {"a": [1, 2, 3]}, "b": b"\x00\x01", "f": 1.5}
    blob = codec.pack(obj)
    assert isinstance(blob, (bytes, bytearray))
    assert codec.unpack(blob) == obj


# --- message journal ------------------------------------------------------

def test_message_journal_full_envelope(px):
    mj = MessageJournal(px.store, set_name="_messages")
    envelope = {"topic": "trades.events", "key": "T-1", "headers": {"src": "gw"},
                "value": {"event_type": "TradeReceived", "payload": {"x": 1}}}
    mj.record("trades.events:0:42", envelope, topic="trades.events", partition=0, offset=42, ts=123.0)

    got = mj.read("trades.events:0:42")
    assert got["message"] == envelope                       # full msgpack round-trip
    assert got["meta"]["topic"] == "trades.events"
    assert got["meta"]["offset"] == 42
    assert codec.RAW_BIN not in got["meta"]                 # blob stripped from meta
    assert mj.read("missing") is None


# --- request journal ------------------------------------------------------

def test_request_journal_req_and_resp(px):
    rj = px.request_journal()
    req = {"event_type": "TradeReceived", "payload": {"notional": 1_000_000}}
    resp = {"status": "applied", "to_state": "booked"}
    rj.record("evt-1", request=req, response=resp, entity="trade",
              type="TradeReceived", status="applied", ts=1.0)

    got = rj.read("evt-1")
    assert got["request"] == req
    assert got["response"] == resp
    assert got["meta"]["entity"] == "trade" and got["meta"]["status"] == "applied"
    # Payloads are stored as opaque msgpack blobs, not plaintext bins.
    raw = px.store.get("_interactions", "evt-1").bins
    assert isinstance(raw["req"], (bytes, bytearray)) and isinstance(raw["resp"], (bytes, bytearray))


# --- state-machine integration -------------------------------------------

def test_state_machine_journals_each_request(px):
    sm = px.state_machine(output=MemoryOutputPublisher())
    trade = {"trade_id": "T-9", "counterparty": "GS", "notional": 5e6,
             "ccy": "USD", "trade_date": 20250115, "book": "R"}
    sm.process(InputEvent(entity="trade", event_type="TradeBooked", key="T-9",
                          payload=trade, event_id="evt-book-1"))

    rj = px.request_journal()
    rec = rj.read("evt-book-1")
    assert rec is not None
    assert rec["request"]["event_type"] == "TradeBooked"
    assert rec["request"]["payload"]["trade_id"] == "T-9"
    assert rec["response"]["status"] in {"applied", "rejected", "duplicate", "dropped"}
    assert rec["meta"]["entity"] == "trade"
