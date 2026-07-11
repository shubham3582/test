"""Key-value storage abstraction and backends."""

from phronexus.kv.base import KVStore, Record, Transaction
from phronexus.kv.memory import InMemoryKV

__all__ = ["KVStore", "Record", "Transaction", "InMemoryKV", "build_store"]


def build_store(settings, telemetry=None) -> KVStore:
    """Construct a KV backend from :class:`phronexus.config.Settings`.

    ``telemetry`` (a :class:`~phronexus.observability.telemetry.Telemetry`) is
    used by the Aerospike backend to emit per-operation metrics. When omitted
    (e.g. a standalone worker) one is built from ``settings.observability`` so
    metrics still export if OTel is enabled there.
    """
    if settings.backend == "memory":
        return InMemoryKV()
    if settings.backend == "aerospike":
        from phronexus.kv.aerospike import AerospikeKV  # lazy: optional dep

        if telemetry is None:
            from phronexus.observability.telemetry import Telemetry

            telemetry = Telemetry(settings.observability)
        return AerospikeKV(settings.aerospike, telemetry=telemetry)
    raise ValueError(f"unknown backend: {settings.backend!r}")
