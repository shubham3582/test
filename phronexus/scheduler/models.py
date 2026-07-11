"""Schedule definitions and the trigger event."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, model_validator


class ScheduleSpec(BaseModel):
    """A schedule. Set exactly one cadence: ``interval_seconds`` or ``daily_at``."""

    model_config = {"extra": "forbid"}

    name: str
    enabled: bool = True
    topic: str                              # where the trigger is published (URI)
    payload: dict[str, Any] = Field(default_factory=dict)   # merged into the trigger
    interval_seconds: Optional[int] = None  # fire every N seconds
    daily_at: Optional[str] = None          # "HH:MM" — fire once per day at this time
    timezone: str = "UTC"                   # IANA tz for daily_at

    @model_validator(mode="after")
    def _one_cadence(self) -> "ScheduleSpec":
        if bool(self.interval_seconds) == bool(self.daily_at):
            raise ValueError("set exactly one of interval_seconds or daily_at")
        if self.interval_seconds is not None and self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if self.daily_at is not None:
            h, _, m = self.daily_at.partition(":")
            if not (h.isdigit() and m.isdigit() and 0 <= int(h) < 24 and 0 <= int(m) < 60):
                raise ValueError("daily_at must be HH:MM")
        return self

    def current_occurrence(self, now: float) -> Optional[int]:
        """Epoch of the most recent scheduled time <= now (a monotonic id), or None."""
        if not self.enabled:
            return None
        if self.interval_seconds:
            return (int(now) // self.interval_seconds) * self.interval_seconds
        # daily_at
        tz = ZoneInfo(self.timezone)
        dt_now = datetime.fromtimestamp(now, tz)
        h, m = (int(x) for x in self.daily_at.split(":"))
        scheduled = dt_now.replace(hour=h, minute=m, second=0, microsecond=0)
        if dt_now < scheduled:
            scheduled -= timedelta(days=1)
        return int(scheduled.timestamp())


@dataclass
class TriggerEvent:
    schedule: str
    occurrence: int       # epoch of the scheduled time (stable id for dedup)
    fired_at: float
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schedule": self.schedule,
            "occurrence": self.occurrence,
            "fired_at": self.fired_at,
            "payload": self.payload,
        }
