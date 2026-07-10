"""OpenTelemetry traces + metrics with a zero-dependency no-op fallback.

When ``otel_enabled`` is False (or the SDK isn't installed) every call is a
cheap no-op, so the core has no hard dependency on OTel while still being fully
instrumented for production APM when it's turned on.
"""

from __future__ import annotations

import contextlib
import time
from typing import Any, Iterator

from phronexus.config import ObservabilitySettings


class _NoopSpan:
    def set_attribute(self, *_a, **_k) -> None: ...
    def record_exception(self, *_a, **_k) -> None: ...


class Telemetry:
    def __init__(self, cfg: ObservabilitySettings):
        self.cfg = cfg
        self._tracer = None
        self._counters: dict[str, Any] = {}
        self._histograms: dict[str, Any] = {}
        self._meter = None
        self._noop_counts: dict[str, float] = {}
        if cfg.otel_enabled:
            self._init_otel()

    def _init_otel(self) -> None:  # pragma: no cover - needs the SDK + collector
        from opentelemetry import metrics, trace
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create({"service.name": self.cfg.service_name})
        tp = TracerProvider(resource=resource)
        tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=self.cfg.otel_endpoint)))
        trace.set_tracer_provider(tp)
        reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=self.cfg.otel_endpoint))
        metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[reader]))
        self._tracer = trace.get_tracer("phronexus")
        self._meter = metrics.get_meter("phronexus")

    # --- tracing --------------------------------------------------------

    @contextlib.contextmanager
    def span(self, name: str, **attrs: Any) -> Iterator[Any]:
        if self._tracer is None:
            yield _NoopSpan()
            return
        with self._tracer.start_as_current_span(name) as sp:  # pragma: no cover
            for k, v in attrs.items():
                sp.set_attribute(k, v)
            yield sp

    # --- metrics --------------------------------------------------------

    def incr(self, name: str, value: float = 1.0, **attrs: Any) -> None:
        if self._meter is None:
            self._noop_counts[name] = self._noop_counts.get(name, 0.0) + value
            return
        c = self._counters.get(name)  # pragma: no cover
        if c is None:
            c = self._meter.create_counter(name)
            self._counters[name] = c
        c.add(value, attrs)

    def observe(self, name: str, value: float, **attrs: Any) -> None:
        if self._meter is None:
            return
        h = self._histograms.get(name)  # pragma: no cover
        if h is None:
            h = self._meter.create_histogram(name, unit="ms")
            self._histograms[name] = h
        h.record(value, attrs)

    @contextlib.contextmanager
    def timed(self, name: str, **attrs: Any) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
        finally:
            self.observe(name, (time.perf_counter() - start) * 1000.0, **attrs)

    # Test/introspection helper for the no-op path.
    def counter_value(self, name: str) -> float:
        return self._noop_counts.get(name, 0.0)
