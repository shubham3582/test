"""In-memory sink — captures events for tests and local demos."""

from __future__ import annotations

from phronexus.events.base import CommitEvent, EventSink


class MemorySink(EventSink):
    def __init__(self) -> None:
        self.events: list[CommitEvent] = []

    def emit(self, event: CommitEvent) -> None:
        self.events.append(event)

    def drain(self) -> list[CommitEvent]:
        out = list(self.events)
        self.events.clear()
        return out
