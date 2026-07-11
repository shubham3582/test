"""Input sources and output publishers for the state machine.

In-memory implementations back tests/demos; Kafka implementations (optional
dependency) back production. Output is the transactional-outbox drain target,
so publishing is at-least-once and the consumer of these topics must dedup.
"""

from __future__ import annotations

import abc
import json
from typing import Optional

from phronexus.config import KafkaSettings
from phronexus.errors import ConfigError
from phronexus.statemachine.models import InputEvent, OutputEvent


# --- input ---------------------------------------------------------------

class InputSource(abc.ABC):
    @abc.abstractmethod
    def poll(self, max_events: int) -> list[InputEvent]: ...

    def close(self) -> None:  # pragma: no cover
        pass


class MemoryInputSource(InputSource):
    def __init__(self, events: Optional[list[InputEvent]] = None):
        self.events: list[InputEvent] = list(events or [])

    def offer(self, event: InputEvent) -> None:
        self.events.append(event)

    def poll(self, max_events: int) -> list[InputEvent]:
        out = self.events[:max_events]
        del self.events[:max_events]
        return out


class KafkaInputSource(InputSource):  # pragma: no cover - needs a broker
    def __init__(self, cfg: KafkaSettings, topics: list[str], group_id: str, journal=None):
        from phronexus.kafka_client import make_consumer

        # Manual offset commit: we commit only AFTER a batch is processed +
        # relayed, so a crash mid-batch replays it rather than losing it. The
        # transition/outbox path is idempotent, so replay is safe.
        self._c = make_consumer(cfg, {  # TLS/mTLS + SASL + MSK IAM
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self._c.subscribe(topics)
        self._journal = journal  # optional MessageJournal — records the raw envelope

    def poll(self, max_events: int) -> list[InputEvent]:
        out: list[InputEvent] = []
        for _ in range(max_events):
            msg = self._c.poll(0.5)
            if msg is None:
                break
            if msg.error():
                continue
            d = json.loads(msg.value())
            if self._journal is not None:
                # Persist the full inbound message as msgpack before processing.
                self._journal.record(
                    f"{msg.topic()}:{msg.partition()}:{msg.offset()}", d,
                    topic=msg.topic(), partition=msg.partition(), offset=msg.offset(),
                    ts=(msg.timestamp() or (0, 0.0))[1] / 1000.0,
                )
            out.append(InputEvent(
                entity=d["entity"], event_type=d["event_type"], key=d["key"],
                payload=d.get("payload", {}), event_id=d["event_id"], ts=d.get("ts", 0.0),
            ))
        return out

    def commit(self) -> None:
        """Commit consumed offsets — call only after the batch is durably handled."""
        self._c.commit(asynchronous=False)

    def close(self) -> None:
        self._c.close()


# --- output --------------------------------------------------------------

class OutputPublisher(abc.ABC):
    @abc.abstractmethod
    def publish(self, event: OutputEvent) -> None: ...

    def close(self) -> None:  # pragma: no cover
        pass


class MemoryOutputPublisher(OutputPublisher):
    def __init__(self) -> None:
        self.events: list[OutputEvent] = []

    def publish(self, event: OutputEvent) -> None:
        self.events.append(event)


class NullOutputPublisher(OutputPublisher):
    """Drops output — for consumer-only topologies (or `null://` emit targets)."""

    def publish(self, event: OutputEvent) -> None:
        return None


class HttpOutputPublisher(OutputPublisher):
    """POSTs the event JSON to its ``http(s)://`` topic URL (TLS/mTLS capable).

    A non-2xx response raises, so the event stays in the outbox for retry
    (at-least-once). Requires httpx (the 'client' extra).
    """

    def __init__(
        self,
        *,
        timeout: float = 5.0,
        verify: bool | str = True,                 # True or a CA bundle path
        cert: Optional[tuple[str, str]] = None,    # (client_cert, client_key) -> mTLS
        headers: Optional[dict[str, str]] = None,
    ):
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover
            raise ConfigError("HttpOutputPublisher requires httpx: pip install 'phronexus-core[client]'") from exc
        self._client = httpx.Client(timeout=timeout, verify=verify, cert=cert, headers=headers or {})

    def publish(self, event: OutputEvent) -> None:
        resp = self._client.post(event.topic, json=event.to_dict())
        resp.raise_for_status()

    def close(self) -> None:
        self._client.close()


class RoutingOutputPublisher(OutputPublisher):
    """Dispatches each event to a publisher by its topic URI scheme.

        kafka://…        -> Kafka/MSK
        http:// https:// -> HttpOutputPublisher
        null://          -> dropped
        (bare name)      -> the default publisher (treated as a Kafka topic)
    """

    def __init__(self, routes: dict[str, OutputPublisher], default: Optional[OutputPublisher] = None):
        self._routes = routes
        self._default = default

    def publish(self, event: OutputEvent) -> None:
        scheme = event.topic.split("://", 1)[0] if "://" in event.topic else "kafka"
        pub = self._routes.get(scheme, self._default)
        if pub is None:
            raise ConfigError(f"no output route for scheme {scheme!r} (topic {event.topic!r})")
        pub.publish(event)

    def close(self) -> None:
        for pub in {id(p): p for p in [*self._routes.values(), self._default] if p}.values():
            pub.close()


def build_output_publisher(settings) -> "OutputPublisher":
    """Build a scheme-routing publisher from settings.

    Routes ``http(s)://`` to an HttpOutputPublisher and ``null://`` to a drop.
    ``kafka://`` (and bare topic names) go to Kafka when enabled, otherwise to an
    in-memory publisher (so tests/dev capture them). This is what lets one state
    machine emit to Kafka, HTTP, or nowhere — per transition.
    """
    sm = settings.statemachine
    routes: dict[str, OutputPublisher] = {"null": NullOutputPublisher()}

    http = HttpOutputPublisher(
        timeout=sm.http_timeout,
        verify=(sm.http_tls_cafile or True),
        cert=((sm.http_tls_certfile, sm.http_tls_keyfile)
              if sm.http_tls_certfile and sm.http_tls_keyfile else None),
        headers=sm.http_headers or None,
    ) if _httpx_available() else None
    if http is not None:
        routes["http"] = http
        routes["https"] = http

    if settings.kafka.enabled:
        routes["kafka"] = KafkaOutputPublisher(settings.kafka)
        default = None
    else:
        default = MemoryOutputPublisher()  # captures kafka:// in tests/dev
    return RoutingOutputPublisher(routes, default=default)


def _httpx_available() -> bool:
    try:
        import httpx  # noqa: F401
        return True
    except ImportError:  # pragma: no cover
        return False


class KafkaOutputPublisher(OutputPublisher):  # pragma: no cover - needs a broker
    def __init__(self, cfg: KafkaSettings):
        from phronexus.kafka_client import make_producer

        self._p = make_producer(cfg)  # TLS/mTLS + SASL + MSK IAM

    def publish(self, event: OutputEvent) -> None:
        # topic may be a bare name or a "kafka://name" URI.
        topic = event.topic.split("://", 1)[-1]
        self._p.produce(topic, key=event.key.encode(), value=json.dumps(event.to_dict()).encode())
        self._p.poll(0)

    def close(self) -> None:
        self._p.flush(5)
