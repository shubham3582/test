"""Aerospike KV backend.

Mirrors :class:`phronexus.kv.memory.InMemoryKV` semantics on a real cluster:
generation-based CAS, TTL, and multi-record transactions (Aerospike 8.0+). The
``aerospike`` client is an optional dependency and imported lazily so the core
package stays installable without it.
"""

from __future__ import annotations

from typing import Any, Iterator, Optional

from phronexus.config import AerospikeSettings
from phronexus.errors import ConfigError, GenerationConflict, StorageError
from phronexus.kv.base import KVStore, Record, Transaction, TransactionContext

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


class _AeroTxn(Transaction):  # pragma: no cover - needs a live cluster
    def __init__(self, client, native):
        self._client = client
        self.native = native  # aerospike.Transaction or None

    def commit(self) -> None:
        if self.native is not None:
            self._client.commit(self.native)

    def abort(self) -> None:
        if self.native is not None:
            self._client.abort(self.native)


class AerospikeKV(KVStore):  # pragma: no cover - needs a live cluster
    def __init__(self, cfg: AerospikeSettings):
        _require_driver()
        self.cfg = cfg
        self.namespace = cfg.namespace
        hosts = [
            (h.split(":")[0], int(h.split(":")[1]))
            for h in cfg.hosts.split(",")
            if h.strip()
        ]
        config: dict[str, Any] = {"hosts": hosts}
        # Authentication mode (INTERNAL / EXTERNAL / EXTERNAL_INSECURE / PKI).
        auth_attr = f"AUTH_{cfg.auth_mode.upper()}"
        if hasattr(aerospike, auth_attr):
            config["policies"] = {"auth_mode": getattr(aerospike, auth_attr)}
        if cfg.tls_enable:
            config["tls"] = {
                "enable": True,
                "cafile": cfg.tls_cafile,
                "certfile": cfg.tls_certfile,  # client cert -> mTLS
                "keyfile": cfg.tls_keyfile,
            }
            config["hosts"] = [(h, p, cfg.tls_name) for (h, p) in hosts]
        client = aerospike.client(config)
        self._client = client.connect(cfg.user, cfg.password) if cfg.user else client.connect()

    def _key(self, set_name: str, key: str):
        return (self.namespace, set_name, key)

    @staticmethod
    def _ttl(ttl: int) -> int:
        # Parity with the memory backend: 0 (or less) means "never expire".
        return aerospike.TTL_NEVER_EXPIRE if ttl <= 0 else ttl

    def _policy(self, expected_generation, txn):
        policy: dict[str, Any] = {}
        if expected_generation is not None:
            policy["gen"] = aerospike.POLICY_GEN_EQ
        if txn is not None and getattr(txn, "native", None) is not None:
            policy["txn"] = txn.native
        return policy

    def get(self, set_name, key, *, txn: Optional[Transaction] = None) -> Optional[Record]:
        policy = {}
        if txn is not None and getattr(txn, "native", None) is not None:
            policy["txn"] = txn.native
        try:
            _, meta, bins = self._client.get(self._key(set_name, key), policy=policy)
        except ax.RecordNotFound:
            return None
        return Record(bins=bins, generation=meta["gen"], ttl=meta.get("ttl", 0))

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
        scan = self._client.scan(self.namespace, set_name)
        for (key, meta, bins) in scan.results():
            userkey = key[2]
            yield userkey, Record(bins=bins, generation=meta["gen"], ttl=meta.get("ttl", 0))

    def transaction(self) -> TransactionContext:
        native = None
        if self.cfg.use_native_txn and hasattr(aerospike, "Transaction"):
            native = aerospike.Transaction()
        return TransactionContext(txn=_AeroTxn(self._client, native))

    def close(self) -> None:
        self._client.close()
