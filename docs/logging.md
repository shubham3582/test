# Logging — one place to change the format

## The single place

Every module logs through `structlog.get_logger(__name__)` and **never**
configures formatting itself. All format, level, and shared-field decisions live
in exactly one function:

**`phronexus/observability/logging.py` → `configure_logging(cfg)`**

It's called once at process startup — by `Phronexus(...)` (the library/API) and by
every standalone worker (`retention.main`, `audit.main`, `retention.compact`,
`retention.validate`). Change that function (or its config) and **the whole
platform's logs change** — app, API, and all workers.

## What it produces today

A structlog processor chain, rendered as JSON (default) or a colorized console
line. Every record carries `service` (and `request_id` inside an API request —
bound per request by `phronexus/api/middleware.py`):

```json
{"event": "document.committed", "entity": "trade", "doc_id": "T-1",
 "level": "info", "timestamp": "2026-07-11T20:17:16.239Z",
 "service": "phronexus-core", "request_id": "abbb63d3…"}
```

The chain (in `configure_logging`):

```python
processors = [
    structlog.contextvars.merge_contextvars,     # service, request_id, …
    structlog.processors.add_log_level,           # -> "level"
    structlog.processors.TimeStamper(fmt="iso"),  # -> "timestamp"
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    structlog.processors.JSONRenderer()           # or dev.ConsoleRenderer()
]
```

## Change it without code (config)

Set on `observability` (env `PHRONEXUS_OBSERVABILITY__*`, or `config/observability.yaml`):

| Setting | Effect |
|---|---|
| `log_json: true \| false` | JSON (prod) vs colorized console (dev) |
| `log_level: INFO \| DEBUG \| …` | minimum level (filtered at the bound logger) |
| `service_name: "…"` | the `service` field bound onto every line |

```bash
PHRONEXUS_OBSERVABILITY__LOG_JSON=false
PHRONEXUS_OBSERVABILITY__LOG_LEVEL=DEBUG
PHRONEXUS_OBSERVABILITY__SERVICE_NAME=phronexus-risk
```

## Change the format itself (edit the one function)

Any format change — key names, timestamp format, extra fields, a different
renderer — is an edit to the `processors` list in `configure_logging`. Common
recipes:

**Different timestamp / rename its key**
```python
structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", key="ts")
```

**Add a field to every line** (host, region, version — a custom processor)
```python
import socket
def add_host(_, __, event_dict):
    event_dict["host"] = socket.gethostname()
    return event_dict
# insert into `processors` before the renderer
```

**Rename standard keys** to match your platform (e.g. `event → message`,
`level → severity`) with a small processor, or configure the renderers'
key mappings. Because it's one chain, the rename applies everywhere.

**A fully custom line format** — replace the final renderer:
```python
processors.append(structlog.processors.KeyValueRenderer(key_order=["timestamp", "level", "service", "event"]))
# or your own callable: (logger, method_name, event_dict) -> str
```

**Ship to an APM/collector as OTLP** — logs stay structured; enable OpenTelemetry
(`otel_enabled: true`) so traces/metrics correlate by `service` + `request_id`.
The log *format* still comes from this one function.

## Why one place is enough

- All modules use `structlog.get_logger(__name__)` — no local `logging.basicConfig`
  or handlers anywhere else.
- `configure_logging` sets the processor chain **and** `merge_contextvars`, so any
  field bound via `structlog.contextvars.bind_contextvars(...)` (like the API's
  `request_id`) automatically appears in every line, in whatever format you choose.
- Workers and the API call the same function, so there's no per-surface drift.
