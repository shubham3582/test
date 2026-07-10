"""Key-value storage abstraction and backends."""

from phronexus.kv.base import KVStore, Record, Transaction
from phronexus.kv.memory import InMemoryKV

__all__ = ["KVStore", "Record", "Transaction", "InMemoryKV", "build_store"]


def build_store(settings) -> KVStore:
    """Construct a KV backend from :class:`phronexus.config.Settings`."""
    if settings.backend == "memory":
        return InMemoryKV()
    if settings.backend == "aerospike":
        from phronexus.kv.aerospike import AerospikeKV  # lazy: optional dep

        return AerospikeKV(settings.aerospike)
    raise ValueError(f"unknown backend: {settings.backend!r}")
