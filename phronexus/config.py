"""Runtime configuration.

Settings are read from environment variables (prefix ``PHRONEXUS_``) and/or a
``.env`` file. Nested groups use ``__`` as the delimiter, e.g.
``PHRONEXUS_AEROSPIKE__HOSTS=10.0.0.1:3000``.
"""

from __future__ import annotations

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AerospikeSettings(BaseModel):
    hosts: str = "127.0.0.1:3000"  # comma-separated host:port pairs
    namespace: str = "phronexus"
    contracts_set: str = "_contracts"
    index_set: str = "_inv"
    outbox_set: str = "_outbox"
    # Security
    tls_enable: bool = False
    tls_cafile: str | None = None
    tls_certfile: str | None = None  # client cert for mTLS
    tls_keyfile: str | None = None
    tls_name: str | None = None
    user: str | None = None
    password: str | None = None
    # Prefer native multi-record transactions (Aerospike 8.0+) when available.
    use_native_txn: bool = True


class KafkaSettings(BaseModel):
    enabled: bool = False
    bootstrap_servers: str = "localhost:9092"
    topic_prefix: str = "phronexus.commits"
    client_id: str = "phronexus-core"


class IcebergSettings(BaseModel):
    enabled: bool = False
    catalog_uri: str = "http://localhost:8181"
    warehouse: str = "s3://phronexus/warehouse"


class ObservabilitySettings(BaseModel):
    service_name: str = "phronexus-core"
    log_level: str = "INFO"
    log_json: bool = True
    otel_enabled: bool = False
    otel_endpoint: str = "http://localhost:4317"
    metrics_enabled: bool = True


class ContractSettings(BaseModel):
    # In-memory cache refresh cadence — enables schema evolution without redeploy.
    refresh_seconds: int = 300
    # If True, a background thread refreshes on the cadence above; otherwise the
    # cache refreshes lazily on access once stale.
    background_refresh: bool = False


class ReaperSettings(BaseModel):
    enabled: bool = False
    interval_seconds: int = 60
    orphan_grace_seconds: int = 300


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PHRONEXUS_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    # "memory" runs entirely in-process (tests / demos); "aerospike" uses a cluster.
    backend: str = "memory"

    aerospike: AerospikeSettings = Field(default_factory=AerospikeSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    iceberg: IcebergSettings = Field(default_factory=IcebergSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    contracts: ContractSettings = Field(default_factory=ContractSettings)
    reaper: ReaperSettings = Field(default_factory=ReaperSettings)

    @property
    def namespace(self) -> str:
        return self.aerospike.namespace
