"""Scheduler: occurrence math + exactly-once firing across replicas."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from phronexus import Phronexus, Settings
from phronexus.scheduler.engine import Scheduler
from phronexus.scheduler.models import ScheduleSpec
from phronexus.statemachine.io import MemoryOutputPublisher


@pytest.fixture()
def px() -> Phronexus:
    settings = Settings(backend="memory")
    settings.observability.log_level = "WARNING"
    p = Phronexus(settings)
    yield p
    p.close()


# --- occurrence math ------------------------------------------------------

def test_interval_occurrence_is_bucket_floor():
    spec = ScheduleSpec(name="s", topic="kafka://t", interval_seconds=60)
    # 12:00:37 -> the 12:00:00 bucket.
    assert spec.current_occurrence(1_000_000_037) == (1_000_000_037 // 60) * 60
    # Same bucket for any instant within the minute (…020 … …079 is one 60s bucket).
    assert spec.current_occurrence(1_000_000_059) == spec.current_occurrence(1_000_000_037)
    # Next minute is a new occurrence.
    assert spec.current_occurrence(1_000_000_080) != spec.current_occurrence(1_000_000_059)


def test_daily_at_occurrence():
    spec = ScheduleSpec(name="eod", topic="kafka://t", daily_at="18:30", timezone="America/New_York")
    tz = ZoneInfo("America/New_York")
    # 19:00 today -> today's 18:30 occurrence.
    now = datetime(2026, 7, 11, 19, 0, tzinfo=tz).timestamp()
    occ = spec.current_occurrence(now)
    assert datetime.fromtimestamp(occ, tz) == datetime(2026, 7, 11, 18, 30, tzinfo=tz)
    # 09:00 today (before the cutoff) -> yesterday's 18:30 occurrence.
    before = datetime(2026, 7, 11, 9, 0, tzinfo=tz).timestamp()
    occ2 = spec.current_occurrence(before)
    assert datetime.fromtimestamp(occ2, tz) == datetime(2026, 7, 10, 18, 30, tzinfo=tz)


def test_disabled_schedule_has_no_occurrence():
    spec = ScheduleSpec(name="s", topic="kafka://t", interval_seconds=60, enabled=False)
    assert spec.current_occurrence(1_000_000_000) is None


def test_exactly_one_cadence_required():
    with pytest.raises(ValueError):
        ScheduleSpec(name="s", topic="kafka://t")  # neither
    with pytest.raises(ValueError):
        ScheduleSpec(name="s", topic="kafka://t", interval_seconds=60, daily_at="18:30")  # both


# --- firing ---------------------------------------------------------------

def test_tick_fires_and_publishes(px):
    out = MemoryOutputPublisher()
    sch = Scheduler(px, output=out)
    sch.upsert_schedule(ScheduleSpec(
        name="cube-refresh", topic="kafka://mfl.cube.requests",
        interval_seconds=60, payload={"job": "refresh"},
    ))
    now = 1_000_000_030
    fired = sch.tick(now=now)
    assert fired == ["cube-refresh"]
    assert len(out.events) == 1
    ev = out.events[0]
    assert ev.topic == "kafka://mfl.cube.requests"
    assert ev.type == "ScheduledTrigger"
    assert ev.payload["schedule"] == "cube-refresh"
    assert ev.payload["payload"] == {"job": "refresh"}


def test_tick_is_idempotent_within_an_occurrence(px):
    out = MemoryOutputPublisher()
    sch = Scheduler(px, output=out)
    sch.upsert_schedule(ScheduleSpec(name="s", topic="kafka://t", interval_seconds=60))
    now = 1_000_000_030
    assert sch.tick(now=now) == ["s"]
    # Same occurrence -> nothing fires again.
    assert sch.tick(now=now + 5) == []
    assert len(out.events) == 1
    # Next bucket -> fires once more.
    assert sch.tick(now=now + 60) == ["s"]
    assert len(out.events) == 2


def test_exactly_once_across_replicas(px):
    """Two schedulers sharing one store must fire an occurrence exactly once."""
    out_a = MemoryOutputPublisher()
    out_b = MemoryOutputPublisher()
    a = Scheduler(px, output=out_a)
    b = Scheduler(px, output=out_b)
    a.upsert_schedule(ScheduleSpec(name="eod", topic="kafka://ccr.eod", interval_seconds=60))
    now = 1_000_000_030

    # Both claim the same occurrence; the CAS lease lets exactly one win.
    fired_a = a.tick(now=now)
    fired_b = b.tick(now=now)
    assert sorted(fired_a + fired_b) == ["eod"]  # exactly one replica fired it

    total = len(out_a.events) + len(out_b.events)
    assert total == 1

    # State reflects a single firing.
    state = a.get_state("eod")
    assert state["last"] == (now // 60) * 60
    assert state["count"] == 1


def test_drain_retries_leftover_outbox(px):
    """A trigger claimed but not yet relayed is drained on the next call."""
    class Flaky(MemoryOutputPublisher):
        def __init__(self):
            super().__init__()
            self.fail_next = True

        def publish(self, event):
            if self.fail_next:
                self.fail_next = False
                raise RuntimeError("broker down")
            super().publish(event)

    out = Flaky()
    sch = Scheduler(px, output=out)
    sch.upsert_schedule(ScheduleSpec(name="s", topic="kafka://t", interval_seconds=60))
    now = 1_000_000_030

    # First tick claims the occurrence but the publish fails -> stays in outbox.
    fired = sch.tick(now=now)
    assert fired == ["s"]
    assert out.events == []
    # The claim persisted (exactly-once holds even though relay failed).
    assert sch.get_state("s")["count"] == 1
    # A later drain relays it — at-least-once delivery, no lost trigger.
    assert sch.drain() == 1
    assert len(out.events) == 1


def test_remove_schedule_stops_firing(px):
    sch = Scheduler(px)
    sch.upsert_schedule(ScheduleSpec(name="s", topic="kafka://t", interval_seconds=60))
    assert [s.name for s in sch.list_schedules()] == ["s"]
    sch.remove_schedule("s")
    assert sch.list_schedules() == []
    assert sch.tick(now=1_000_000_030) == []
