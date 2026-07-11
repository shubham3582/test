"""Runtime configuration.

Configuration is layered, highest precedence first:

1. Constructor kwargs
2. Environment variables (prefix ``PHRONEXUS_``; nested groups use ``__``, e.g.
   ``PHRONEXUS_KAFKA__SASL_PASSWORD``)
3. ``.env`` file
4. **Per-subsystem YAML files** in a config directory (``aerospike.yaml``,
   ``kafka.yaml``, ``iceberg.yaml``, ``api.yaml``, ``observability.yaml``,
   ``statemachine.yaml``, ``contracts.yaml``, plus a top-level ``phronexus.yaml``)

So operators keep readable, per-subsystem files under version control and
override individual values with environment variables at deploy time. Secrets
should never sit in the files: reference them with ``${ENV_VAR}`` or
``${ENV_VAR:-default}`` placeholders that are interpolated from the environment
at load time.

Enable file config by pointing ``PHRONEXUS_CONFIG_DIR`` at the directory (or call
``load_settings("path/to/config")``). When unset, only env/.env/defaults apply,
so the in-memory dev default is unchanged.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

# ---------------------------------------------------------------------------
# Subsystem settings
# ---------------------------------------------------------------------------


class AerospikeSettings(BaseModel):
    hosts: str = "127.0.0.1:3000"  # comma-separated host:port pairs
    namespace: str = "phronexus"
    contracts_set: str = "_contracts"
    index_set: str = "_inv"
    outbox_set: str = "_outbox"
    # --- security: TLS / mTLS + auth ---
    tls_enable: bool = False
    tls_cafile: str | None = None       # CA bundle to verify the cluster
    tls_certfile: str | None = None     # client certificate -> mTLS
    tls_keyfile: str | None = None      # client private key -> mTLS
    tls_name: str | None = None         # TLS name presented by the cluster
    user: str | None = None
    password: str | None = None
    auth_mode: str = "INTERNAL"         # INTERNAL | EXTERNAL | EXTERNAL_INSECURE | PKI
    # Prefer native multi-record transactions (Aerospike 8.0+) when available.
    use_native_txn: bool = True


class KafkaSettings(BaseModel):
    enabled: bool = False
    bootstrap_servers: str = "localhost:9092"
    topic_prefix: str = "phronexus.commits"
    client_id: str = "phronexus-core"
    # --- security ---
    # PLAINTEXT | SSL | SASL_SSL | SASL_PLAINTEXT
    security_protocol: str = "PLAINTEXT"
    ssl_cafile: str | None = None        # CA bundle to verify brokers
    ssl_certfile: str | None = None      # client certificate -> mTLS
    ssl_keyfile: str | None = None       # client private key -> mTLS
    ssl_key_password: str | None = None
    ssl_verify: bool = True              # verify broker certificate
    ssl_endpoint_identification: bool = True  # verify broker hostname
    sasl_mechanism: str | None = None    # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | OAUTHBEARER | GSSAPI
    sasl_username: str | None = None
    sasl_password: str | None = None
    # --- Amazon MSK IAM auth ---
    # When true, use SASL_SSL + OAUTHBEARER with an AWS SigV4 token provider
    # (requires the 'msk' extra: aws-msk-iam-sasl-signer).
    msk_iam: bool = False
    aws_region: str | None = None        # region used to sign MSK IAM tokens

    def client_config(self) -> dict[str, Any]:
        """Map to a librdkafka (confluent-kafka) client configuration dict.

        The OAUTHBEARER token callback for MSK IAM is attached separately at
        client construction (see phronexus.kafka_client), not here.
        """
        cfg: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "security.protocol": self.security_protocol,
        }
        if self.msk_iam:
            # MSK IAM: TLS transport + OAUTHBEARER token signed with SigV4.
            cfg["security.protocol"] = "SASL_SSL"
            cfg["sasl.mechanism"] = "OAUTHBEARER"
            if self.ssl_cafile:  # MSK uses a public CA; override only if pinned
                cfg["ssl.ca.location"] = self.ssl_cafile
            return cfg
        if self.security_protocol in ("SSL", "SASL_SSL"):
            if self.ssl_cafile:
                cfg["ssl.ca.location"] = self.ssl_cafile
            if self.ssl_certfile:
                cfg["ssl.certificate.location"] = self.ssl_certfile
            if self.ssl_keyfile:
                cfg["ssl.key.location"] = self.ssl_keyfile
            if self.ssl_key_password:
                cfg["ssl.key.password"] = self.ssl_key_password
            cfg["enable.ssl.certificate.verification"] = self.ssl_verify
            if not self.ssl_endpoint_identification:
                cfg["ssl.endpoint.identification.algorithm"] = "none"
        if self.security_protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
            if self.sasl_mechanism:
                cfg["sasl.mechanism"] = self.sasl_mechanism
            if self.sasl_username:
                cfg["sasl.username"] = self.sasl_username
            if self.sasl_password:
                cfg["sasl.password"] = self.sasl_password
        return cfg


class IcebergSettings(BaseModel):
    enabled: bool = False
    # "memory" (in-process warehouse for tests/demos) or "iceberg" (pyiceberg).
    backend: str = "memory"
    catalog_name: str = "phronexus"
    catalog_type: str = "rest"               # pyiceberg catalog type
    catalog_uri: str = "http://localhost:8181"
    warehouse: str = "s3://phronexus/warehouse"
    batch_size: int = 500
    # --- AWS SigV4 signing (Amazon S3 Tables / Glue Iceberg REST) ---
    sigv4_enabled: bool = False
    signing_name: str | None = None          # "s3tables" (direct) or "glue" (via Glue)
    signing_region: str | None = None        # AWS region used to sign requests
    # --- REST catalog auth / TLS ---
    catalog_token: str | None = None         # bearer token for the REST catalog
    catalog_tls_cafile: str | None = None    # CA bundle to verify the catalog
    catalog_tls_certfile: str | None = None  # client certificate -> mTLS to catalog
    catalog_tls_keyfile: str | None = None
    # --- object store (S3-compatible) ---
    s3_endpoint: str | None = None
    s3_region: str | None = None
    s3_access_key_id: str | None = None
    s3_secret_access_key: str | None = None
    s3_tls_verify: bool = True

    def catalog_properties(self) -> dict[str, Any]:
        """Build a pyiceberg ``load_catalog`` properties dict.

        Key names follow pyiceberg's REST/S3 FileIO conventions; confirm against
        the pinned pyiceberg version for a given deployment.
        """
        props: dict[str, Any] = {
            "type": self.catalog_type,
            "uri": self.catalog_uri,
            "warehouse": self.warehouse,
        }
        if self.sigv4_enabled:
            # Amazon S3 Tables / Glue Iceberg REST endpoints authenticate with SigV4.
            props["rest.sigv4-enabled"] = "true"
            if self.signing_name:
                props["rest.signing-name"] = self.signing_name
            if self.signing_region:
                props["rest.signing-region"] = self.signing_region
        if self.catalog_token:
            props["token"] = self.catalog_token
        if self.catalog_tls_cafile:
            props["ssl.cabundle"] = self.catalog_tls_cafile
        if self.catalog_tls_certfile:
            props["ssl.client.cert"] = self.catalog_tls_certfile
        if self.catalog_tls_keyfile:
            props["ssl.client.key"] = self.catalog_tls_keyfile
        if self.s3_endpoint:
            props["s3.endpoint"] = self.s3_endpoint
        if self.s3_region:
            props["s3.region"] = self.s3_region
        if self.s3_access_key_id:
            props["s3.access-key-id"] = self.s3_access_key_id
        if self.s3_secret_access_key:
            props["s3.secret-access-key"] = self.s3_secret_access_key
        if not self.s3_tls_verify:
            props["s3.connect.ssl-verify"] = False
        return props


class AuthSettings(BaseModel):
    # Any subset of: "none", "api_key", "bearer", "mtls". Tried in order; the
    # first that produces a principal wins. Default is open (dev only).
    schemes: list[str] = Field(default_factory=lambda: ["none"])
    api_key_header: str = "X-API-Key"
    # key -> principal name. In production store hashes, not raw keys.
    api_keys: dict[str, str] = Field(default_factory=dict)
    # opaque bearer token -> principal name (prototype; swap for JWT verify).
    bearer_tokens: dict[str, str] = Field(default_factory=dict)
    # Header a trusted TLS-terminating proxy uses to forward the client-cert CN.
    mtls_cn_header: str = "X-Client-Cert-CN"
    # Optional CN allow-list -> principal; empty means "any presented CN".
    mtls_allowed_cns: dict[str, str] = Field(default_factory=dict)
    # Principals allowed to hit contract-admin endpoints; empty means all.
    admin_principals: list[str] = Field(default_factory=list)


class ApiSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8080
    # Server-side TLS / mTLS (uvicorn). Terminate at a proxy instead if preferred.
    tls_certfile: str | None = None
    tls_keyfile: str | None = None
    tls_ca_certs: str | None = None  # set to require client certs (mTLS)
    require_client_cert: bool = False
    auth: AuthSettings = Field(default_factory=AuthSettings)


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


class StateMachineSettings(BaseModel):
    # Sets backing the transactional outbox and the input-dedup markers.
    outbox_set: str = "_sm_outbox"
    dedup_set: str = "_sm_dedup"
    dedup_ttl: int = 604800  # 7 days; markers past redelivery windows can expire
    # Kafka topic the autonomous runner consumes domain events from (if wired).
    input_topics: list[str] = Field(default_factory=list)
    consumer_group: str = "phronexus-statemachine"
    # If True, process() drains the outbox inline after commit (simple, but a
    # slow broker/webhook adds latency to the request). If False, a standalone
    # relay (python -m phronexus.statemachine.relay) owns the drain.
    inline_relay: bool = True
    relay_poll_seconds: float = 1.0
    # HTTP output publisher (for http(s):// emit targets) — TLS/mTLS + headers.
    http_timeout: float = 5.0
    http_tls_cafile: str | None = None
    http_tls_certfile: str | None = None    # client cert -> mTLS
    http_tls_keyfile: str | None = None
    http_headers: dict[str, str] = Field(default_factory=dict)


class ReaperSettings(BaseModel):
    enabled: bool = False
    interval_seconds: int = 60
    orphan_grace_seconds: int = 300


# ---------------------------------------------------------------------------
# File-based config source (per-subsystem YAML + ${ENV} interpolation)
# ---------------------------------------------------------------------------

_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")

# top-level scalars live in phronexus.yaml; every other group maps to <group>.yaml
_GROUP_FILES = {
    "aerospike": "aerospike.yaml",
    "kafka": "kafka.yaml",
    "iceberg": "iceberg.yaml",
    "api": "api.yaml",
    "observability": "observability.yaml",
    "contracts": "contracts.yaml",
    "statemachine": "statemachine.yaml",
    "reaper": "reaper.yaml",
}

# Set by load_settings(); read by the settings source.
_CONFIG_DIR_OVERRIDE: Optional[str] = None


def _interpolate(value: Any) -> Any:
    """Recursively replace ``${VAR}`` / ``${VAR:-default}`` from the environment."""
    if isinstance(value, str):
        def sub(m: re.Match) -> str:
            var, default = m.group(1), m.group(2)
            return os.environ.get(var, default if default is not None else "")
        return _ENV_RE.sub(sub, value)
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    return value


def _load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError(f"config file {path} must contain a mapping")
    return _interpolate(data)


class FileConfigSource(PydanticBaseSettingsSource):
    """Loads per-subsystem YAML files from a config directory."""

    def __init__(self, settings_cls, config_dir: Optional[str]):
        super().__init__(settings_cls)
        self._data: dict[str, Any] = {}
        if config_dir:
            self._data = self._load(Path(config_dir))

    def _load(self, root: Path) -> dict[str, Any]:
        if not root.is_dir():
            return {}
        data: dict[str, Any] = {}
        top = root / "phronexus.yaml"
        if top.exists():
            data.update(_load_yaml(top))
        for group, fname in _GROUP_FILES.items():
            fp = root / fname
            if fp.exists():
                data[group] = _load_yaml(fp)
        return data

    def get_field_value(self, field, field_name):  # pragma: no cover - unused
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        return self._data


# ---------------------------------------------------------------------------
# Root settings
# ---------------------------------------------------------------------------


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
    api: ApiSettings = Field(default_factory=ApiSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    iceberg: IcebergSettings = Field(default_factory=IcebergSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    contracts: ContractSettings = Field(default_factory=ContractSettings)
    reaper: ReaperSettings = Field(default_factory=ReaperSettings)
    statemachine: StateMachineSettings = Field(default_factory=StateMachineSettings)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        config_dir = _CONFIG_DIR_OVERRIDE or os.environ.get("PHRONEXUS_CONFIG_DIR")
        # Precedence (first wins): init > env > .env > YAML files > file secrets.
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            FileConfigSource(settings_cls, config_dir),
            file_secret_settings,
        )

    @property
    def namespace(self) -> str:
        return self.aerospike.namespace


def load_settings(config_dir: Optional[str] = None) -> Settings:
    """Load settings, optionally from a directory of per-subsystem YAML files.

    Environment variables still override file values.
    """
    global _CONFIG_DIR_OVERRIDE
    _CONFIG_DIR_OVERRIDE = config_dir
    try:
        return Settings()
    finally:
        _CONFIG_DIR_OVERRIDE = None
