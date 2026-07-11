"""Binary journals — full messages and request/response pairs as msgpack.

Two small stores that share the same envelope convention (see :mod:`phronexus.codec`):
a few typed metadata bins for lookup, plus one bin holding the full payload as a
msgpack blob, decoded byte-faithfully on read.

- :class:`MessageJournal` — persists each raw inbound message (e.g. the full
  Kafka envelope) under its own key, so traffic can be inspected or replayed.
- :class:`RequestJournal` — persists, per request, the request and its response
  as two msgpack blobs alongside status/type metadata — an audit trail of every
  interaction the state machine handled.

Both are backed by the KVStore, so they work on the in-memory backend (tests)
and Aerospike (production) unchanged. Metadata bin names are kept <=15 chars to
respect Aerospike's bin-name limit; any extra metadata keys a caller passes must
do the same.
"""

from __future__ import annotations

from typing import Any, Optional

from phronexus import codec
from phronexus.kv.base import KVStore

# Blob bin names.
REQ_BIN = "req"
RESP_BIN = "resp"


class MessageJournal:
    """Stores full inbound messages: metadata bins + one msgpack ``raw`` blob."""

    def __init__(self, store: KVStore, set_name: str = "_messages", ttl: int = 0):
        self._store = store
        self._set = set_name
        self._ttl = ttl

    def record(
        self,
        key: str,
        message: Any,
        *,
        topic: str = "",
        partition: int = 0,
        offset: int = 0,
        ts: float = 0.0,
    ) -> str:
        """Persist ``message`` (any msgpack-able object) under ``key``."""
        self._store.put(
            self._set, key,
            {"topic": topic, "part": partition, "offset": offset, "ts": ts,
             codec.RAW_BIN: codec.pack(message)},
            ttl=self._ttl,
        )
        return key

    def read(self, key: str) -> Optional[dict[str, Any]]:
        """Return ``{"meta": {...}, "message": <decoded>}`` or None."""
        rec = self._store.get(self._set, key)
        if rec is None:
            return None
        bins = dict(rec.bins)
        message = codec.unpack(bins.pop(codec.RAW_BIN))
        return {"meta": bins, "message": message}


class RequestJournal:
    """Per-request record: metadata bins + msgpack ``req`` blob + msgpack ``resp`` blob."""

    def __init__(self, store: KVStore, set_name: str = "_interactions", ttl: int = 0):
        self._store = store
        self._set = set_name
        self._ttl = ttl

    def record(
        self,
        req_id: str,
        *,
        request: Any,
        response: Any,
        entity: str = "",
        type: str = "",
        status: str = "",
        ts: float = 0.0,
        **meta: Any,
    ) -> str:
        """Persist a request + its response as two msgpack blobs, keyed by ``req_id``.

        Extra ``meta`` kwargs are stored as-is metadata bins (keep names <=15 chars).
        """
        bins: dict[str, Any] = {
            "entity": entity, "type": type, "status": status, "ts": ts,
            REQ_BIN: codec.pack(request), RESP_BIN: codec.pack(response),
        }
        bins.update(meta)
        self._store.put(self._set, req_id, bins, ttl=self._ttl)
        return req_id

    def read(self, req_id: str) -> Optional[dict[str, Any]]:
        """Return ``{"meta": {...}, "request": <decoded>, "response": <decoded>}`` or None."""
        rec = self._store.get(self._set, req_id)
        if rec is None:
            return None
        bins = dict(rec.bins)
        request = codec.unpack(bins.pop(REQ_BIN))
        response = codec.unpack(bins.pop(RESP_BIN))
        return {"meta": bins, "request": request, "response": response}
