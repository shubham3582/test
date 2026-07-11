"""Binary payload codec — msgpack envelopes for stored records.

The storage convention across the framework is: a handful of small, typed
*metadata bins* (queryable) plus **one bin holding the full payload as a msgpack
blob**. ``pack``/``unpack`` are the only place that knows the wire format, so it
can be swapped (or wrapped with compression/encryption) without touching any
caller. The same blob is written on the way in and decoded on the way out, so a
round-trip is byte-faithful.

msgpack is a hard dependency (compact, fast, cross-language) — a consumer in any
language can read the blob with a standard msgpack library.
"""

from __future__ import annotations

from typing import Any

import msgpack

# One bin name, used everywhere a full payload is stored as a blob.
RAW_BIN = "raw"


def pack(obj: Any) -> bytes:
    """Serialize a Python object to a msgpack blob (bytes)."""
    return msgpack.packb(obj, use_bin_type=True)


def unpack(blob: bytes) -> Any:
    """Deserialize a msgpack blob back to a Python object.

    ``strict_map_key=False`` so non-string map keys survive a round-trip; blobs
    come from our own ``pack`` (or a trusted producer), so this is safe.
    """
    return msgpack.unpackb(bytes(blob), raw=False, strict_map_key=False)
