"""Distributed scheduler with Aerospike-backed state.

Fires each schedule occurrence **exactly once across replicas** by claiming it
with a generation-CAS lease on a per-schedule state record, then enqueuing the
trigger into a transactional outbox and relaying it to a topic. So multiple
scheduler replicas can run for availability without generating duplicate events
(kafka topic -> service -> the service pulls data via Phronexus).
"""

from phronexus.scheduler.engine import Scheduler
from phronexus.scheduler.models import ScheduleSpec, TriggerEvent

__all__ = ["Scheduler", "ScheduleSpec", "TriggerEvent"]
