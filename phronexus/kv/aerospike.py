"""Aerospike KV backend.

Mirrors :class:`phronexus.kv.memory.InMemoryKV` semantics on a real cluster:
generation-based CAS, TTL, and multi-record transactions (Aerospike 8.0+). The
``aerospike`` client is an optional dependency and imported lazily so the core
package stays installable without it.
"""

from __future__ import annotations

import contextlib
import time
from typing import Any, Iterator, Optional

from phronexus.config import AerospikeSettings
from phronexus.errors import ConfigError, GenerationConflict, StorageError
from phronexus.kv.base import KVStore, Record, Transaction, TransactionContext

# OTLP metrics emitted per operation (consumable by Dynatrace / CloudWatch via
# the OTel collector). One histogram + one counter, tagged by op and outcome.
_M_DURATION = "phronexus.aerospike.op.duration"   # milliseconds
_M_COUNT = "phronexus.aerospike.op.count"         # requests, by outcome

try:  # pragma: no cover - exercised only with the driver installed
    import aerospike
    from aerospike import exception as ax
except ImportError:  # pragma: no cover
    aerospike = None
    ax = None


def _require_driver() -> None:
    if aerospike is None:  # pragma: no cover
        raise ConfigError(
            "backend=aerospike requires the aerospike client: "
            "pip install 'phronexus-core[aerospike]'"
        )


def build_client_config(cfg: AerospikeSettings) -> dict[str, Any]:
    """Build the native ``aerospike.client`` config dict from settings.

    Pure (no driver calls beyond optional constant lookups) so it can be unit
    tested without a live cluster. Native passthrough is applied last:
    ``cfg.policies`` merges into ``config['policies']`` and ``cfg.client_config``
    merges at the top level — so any native client option can be set from config.
    """
    hosts = [
        (h.split(":")[0], int(h.split(":")[1]))
        for h in cfg.hosts.split(",")
        if h.strip()
    ]
    config: dict[str, Any] = {"hosts": hosts}
    # Authentication mode (INTERNAL / EXTERNAL / EXTERNAL_INSECURE / PKI).
    auth_attr = f"AUTH_{cfg.auth_mode.upper()}"
    if aerospike is not None and hasattr(aerospike, auth_attr):
        config["policies"] = {"auth_mode": getattr(aerospike, auth_attr)}
    if cfg.tls_enable:
        config["tls"] = {
            "enable": True,
            "cafile": cfg.tls_cafile,
            "certfile": cfg.tls_certfile,  # client cert -> mTLS
            "keyfile": cfg.tls_keyfile,
        }
        config["hosts"] = [(h, p, cfg.tls_name) for (h, p) in hosts]
    # --- native passthrough (wins over the computed defaults) ---
    if cfg.policies:
        config.setdefault("policies", {}).update(cfg.policies)
    for k, v in (cfg.client_config or {}).items():
        if k == "policies" and isinstance(v, dict):
            config.setdefault("policies", {}).update(v)
        else:
            config[k] = v
    return config


class _AeroTxn(Transaction):  # pragma: no cover - needs a live cluster
    def __init__(self, client, native, op=None):
        self._client = client
        self.native = native  # aerospike.Transaction or None
        self._op = op         # AerospikeKV._op context manager, or None

    def commit(self) -> None:
        if self.native is None:
            return
        if self._op is None:
            self._client.commit(self.native)
            return
        with self._op("txn_commit"):
            self._client.commit(self.native)

    def abort(self) -> None:
        if self.native is None:
            return
        if self._op is None:
            self._client.abort(self.native)
            return
        with self._op("txn_abort"):
            self._client.abort(self.native)


class AerospikeKV(KVStore):  # pragma: no cover - needs a live cluster
    def __init__(self, cfg: AerospikeSettings, telemetry=None):
        _require_driver()
        self.cfg = cfg
        self.namespace = cfg.namespace
        if telemetry is None:  # standalone construction -> no-op metrics
            from phronexus.config import ObservabilitySettings
            from phronexus.observability.telemetry import Telemetry

            telemetry = Telemetry(ObservabilitySettings())
        self._tel = telemetry
        client = aerospike.client(build_client_config(cfg))
        self._client = client.connect(cfg.user, cfg.password) if cfg.user else client.connect()

    # --- per-operation metrics ------------------------------------------

    def _record(self, op: str, outcome: str, start: float, error: Optional[str] = None) -> None:
        self._tel.observe(_M_DURATION, (time.perf_counter() - start) * 1000.0, op=op, outcome=outcome)
        attrs = {"op": op, "outcome": outcome}
        if error is not None:
            attrs["error"] = error
        self._tel.incr(_M_COUNT, 1.0, **attrs)

    @contextlib.contextmanager
    def _op(self, op: str):
        """Time an operation and record latency + outcome. ``GenerationConflict``
        (optimistic-concurrency CAS) is tagged ``conflict``, not ``error``."""
        start = time.perf_counter()
        outcome, error = "ok", None
        try:
            yield
        except GenerationConflict as exc:
            outcome, error = "conflict", type(exc).__name__
            raise
        except Exception as exc:  # noqa: BLE001 - classify then re-raise
            outcome, error = "error", type(exc).__name__
            raise
        finally:
            self._record(op, outcome, start, error)

    def native_client(self):
        """The connected native ``aerospike.Client`` (supported escape hatch).

        Use for native features not surfaced by the KVStore API (expressions,
        CDT list/map ops, operate(), batch, secondary-index queries, UDFs). For
        the managed-set guard and namespaced helpers, prefer
        :meth:`phronexus.core.Phronexus.native_aerospike`.
        """
        return self._client

    def _key(self, set_name: str, key: str):
        return (self.namespace, set_name, key)

    @staticmethod
    def _ttl(ttl: int) -> int:
        # Parity with the memory backend: 0 (or less) means "never expire".
        return aerospike.TTL_NEVER_EXPIRE if ttl <= 0 else ttl

    def _policy(self, expected_generation, txn):
        # Store the primary key with the record (not just its digest) so scan()
        # can return the user key — the registry/reaper/relays iterate by key.
        policy: dict[str, Any] = {"key": aerospike.POLICY_KEY_SEND}
        if expected_generation is not None:
            policy["gen"] = aerospike.POLICY_GEN_EQ
        if txn is not None and getattr(txn, "native", None) is not None:
            policy["txn"] = txn.native
        return policy

    def get(self, set_name, key, *, txn: Optional[Transaction] = None) -> Optional[Record]:
        policy = {}
        if txn is not None and getattr(txn, "native", None) is not None:
            policy["txn"] = txn.native
        with self._op("get"):
            try:
                _, meta, bins = self._client.get(self._key(set_name, key), policy=policy)
            except ax.RecordNotFound:
                return None  # a miss is a normal read, not a failure
            return Record(bins=bins, generation=meta["gen"], ttl=meta.get("ttl", 0))

    def batch_get(
        self, set_name: str, keys: list[str], *, txn: Optional[Transaction] = None
    ) -> dict[str, Record]:
        """One round-trip for many keys via the native batch API (falls back to
        per-key gets inside a transaction, which batch reads don't join)."""
        if txn is not None or not keys:
            return super().batch_get(set_name, keys, txn=txn)
        as_keys = [self._key(set_name, k) for k in keys]
        out: dict[str, Record] = {}

        def _add(rec) -> None:
            if not rec:
                return
            key, meta, bins = rec[0], rec[1], rec[2] if len(rec) > 2 else None
            if meta is None or bins is None or key[2] is None:
                return  # not found / no user key
            out[key[2]] = Record(bins=bins, generation=meta["gen"], ttl=meta.get("ttl", 0))

        with self._op("batch_get"):
            if hasattr(self._client, "batch_read"):   # modern client
                for br in self._client.batch_read(as_keys).batch_records:
                    if getattr(br, "result", 0) == 0:
                        _add(getattr(br, "record", None))
            elif hasattr(self._client, "get_many"):    # legacy client
                for rec in self._client.get_many(as_keys):
                    _add(rec)
            else:  # no batch API — per-key gets (each self-instruments as "get")
                return super().batch_get(set_name, keys, txn=txn)
        return out

    def put(
        self,
        set_name,
        key,
        bins,
        *,
        ttl=0,
        expected_generation=None,
        txn: Optional[Transaction] = None,
    ) -> int:
        meta = {"ttl": self._ttl(ttl)}
        if expected_generation is not None:
            meta["gen"] = expected_generation
        with self._op("put"):
            try:
                self._client.put(
                    self._key(set_name, key),
                    bins,
                    meta=meta,
                    policy=self._policy(expected_generation, txn),
                )
            except ax.RecordGenerationError as exc:
                raise GenerationConflict(str(exc)) from exc
            except ax.AerospikeError as exc:
                raise StorageError(str(exc)) from exc
        # Generation is server-assigned; callers that need it re-read.
        return expected_generation + 1 if expected_generation is not None else 0

    def remove(self, set_name, key, *, expected_generation=None, txn: Optional[Transaction] = None):
        meta = {"gen": expected_generation} if expected_generation is not None else None
        with self._op("remove"):
            try:
                self._client.remove(
                    self._key(set_name, key),
                    meta=meta,
                    policy=self._policy(expected_generation, txn),
                )
            except ax.RecordNotFound:
                return
            except ax.RecordGenerationError as exc:
                raise GenerationConflict(str(exc)) from exc

    def scan(self, set_name) -> Iterator[tuple[str, Record]]:
        # query() with no predicate is a full-set scan (scan() is deprecated in
        # newer clients). Records carry their user key because writes use
        # POLICY_KEY_SEND; skip any legacy record that lacks one.
        start = time.perf_counter()
        outcome, error = "ok", None
        try:
            q = self._client.query(self.namespace, set_name)
            for (key, meta, bins) in q.results():
                userkey = key[2]
                if userkey is None:
                    continue
                yield userkey, Record(bins=bins, generation=meta["gen"], ttl=meta.get("ttl", 0))
        except Exception as exc:  # noqa: BLE001
            outcome, error = "error", type(exc).__name__
            raise
        finally:
            self._record("scan", outcome, start, error)

    def transaction(self) -> TransactionContext:
        native = None
        if self.cfg.use_native_txn and hasattr(aerospike, "Transaction"):
            native = aerospike.Transaction()
        return TransactionContext(txn=_AeroTxn(self._client, native, self._op))

    def close(self) -> None:
        self._client.close()
