"""G11 — the native-transaction precondition is honest, never silently degraded.

The saga's exactly-once guarantee needs atomic multi-record transactions. On a
backend that can't provide them the state machine fails fast (unless the operator
opts into best-effort), and `phronexus doctor` reports it as a warning so the
posture is visible rather than a silent downgrade.
"""

from __future__ import annotations

import pytest

from phronexus import Settings
from phronexus.errors import ConfigError
from phronexus.kv.memory import InMemoryKV
from tests.harness import SequentialKV, phronexus_with


def _atomic_check(report):
    return next(c for c in report["checks"] if c["link"] == "store.atomic_writes")


def test_state_machine_fails_fast_without_atomic_txn():
    px = phronexus_with(SequentialKV())  # non-atomic; require_atomic defaults True
    with pytest.raises(ConfigError):
        px.state_machine()
    px.close()


def test_state_machine_best_effort_opt_in():
    s = Settings(backend="memory")
    s.observability.log_level = "WARNING"
    s.statemachine.require_atomic = False
    px = phronexus_with(SequentialKV(), settings=s)
    assert px.state_machine() is not None  # allowed in best-effort mode
    px.close()


def test_doctor_flags_non_atomic_store():
    px = phronexus_with(SequentialKV())
    report = px.durability_report()
    assert _atomic_check(report)["status"] == "warn"
    assert report["no_loss"] is False
    px.close()


def test_doctor_atomic_pass_on_atomic_store():
    px = phronexus_with(InMemoryKV())
    assert _atomic_check(px.durability_report())["status"] == "pass"
    px.close()
