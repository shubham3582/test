"""Input sources and output publishers for the state machine.

In-memory implementations back tests/demos; Kafka implementations (optional
dependency) back production. Output is the transactional-outbox drain target,
so publishing is at-least-once and the consumer of these topics must dedup.
"""

from __future__ import annotations

import abc
import json
from typing import Callable, Optional

import structlog

from phronexus.config import KafkaSettings
from phronexus.errors import ConfigError
from phronexus.statemachine.models import InputEvent, OutputEvent

log = structlog.get_logger("phronexus.statemachine")

# A poison handler receives the raw undecodable message, its broker coordinates
# (topic/partition/offset), and the decode error. See KafkaInputSource.poll.
PoisonHandler = Callable[[bytes, dict, Exception], None]


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


def _log_poison(raw: bytes, meta: dict, exc: Exception) -> None:
    """Default poison handler: log and drop. A structurally-broken message can't
    be reprocessed into a valid document, so dropping it (offset advances) is
    strictly better than wedging the partition on endless redelivery."""
    log.error("statemachine.poison_message", error=str(exc),
              bytes=len(raw), **meta)


class KafkaInputSource(InputSource):  # pragma: no cover - needs a broker
    def __init__(self, cfg: KafkaSettings, topics: list[str], group_id: str, journal=None,
                 on_poison: Optional[PoisonHandler] = None):
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
        # Where structurally-undecodable messages go. See poll().
        self._on_poison = on_poison or _log_poison
        # True once poll() has consumed messages the group hasn't committed yet.
        # Lets the runner commit even a batch that decoded to *no* events (every
        # message was poison), so a poison-only batch can't redeliver forever.
        self.uncommitted = False

    def poll(self, max_events: int) -> list[InputEvent]:
        out: list[InputEvent] = []
        for _ in range(max_events):
            msg = self._c.poll(0.5)
            if msg is None:
                break
            if msg.error():
                continue
            self.uncommitted = True  # a real message was consumed (good or poison)
            raw = msg.value()
            meta = {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()}
            try:
                d = json.loads(raw)
                event = InputEvent(
                    entity=d["entity"], event_type=d["event_type"], key=d["key"],
                    payload=d.get("payload", {}), event_id=d["event_id"], ts=d.get("ts", 0.0),
                )
            except (ValueError, KeyError, TypeError) as exc:
                # Undecodable envelope (bad JSON / missing required field). It can't
                # become an InputEvent, so the semantic DLQ (which keys on
                # entity/event_id) can't take it. Route the RAW bytes to the poison
                # handler and skip, so the offset advances — one bad message must
                # not wedge the whole partition on endless redelivery.
                self._on_poison(raw, meta, exc)
                continue
            if self._journal is not None:
                # Persist the full inbound message as msgpack before processing.
                self._journal.record(
                    f"{meta['topic']}:{meta['partition']}:{meta['offset']}", d,
                    ts=(msg.timestamp() or (0, 0.0))[1] / 1000.0, **meta,
                )
            out.append(event)
        return out

    def commit(self) -> None:
        """Commit consumed offsets — call only after the batch is durably handled."""
        self._c.commit(asynchronous=False)
        self.uncommitted = False

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
