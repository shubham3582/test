"""Centralised Kafka client construction (confluent-kafka).

One place builds Producers/Consumers so security wiring — TLS/mTLS, SASL, and
the Amazon MSK IAM OAUTHBEARER token callback — lives in a single spot. All
imports are lazy so the core stays installable without the driver.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

import structlog

from phronexus.config import KafkaSettings
from phronexus.errors import ConfigError

log = structlog.get_logger(__name__)


def _msk_oauth_cb(region: str) -> Callable[[str], tuple[str, float]]:
    """OAUTHBEARER token provider for Amazon MSK IAM.

    Returns a callback that mints a SigV4-signed token from the caller's AWS
    credentials (instance role / env / profile). Requires the 'msk' extra.
    """
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    except ImportError as exc:  # pragma: no cover
        raise ConfigError(
            "kafka.msk_iam requires aws-msk-iam-sasl-signer: "
            "pip install 'phronexus-core[msk]'"
        ) from exc

    def _cb(_oauth_config: str) -> tuple[str, float]:  # pragma: no cover - needs AWS
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        return token, expiry_ms / 1000.0

    return _cb


def _augment(cfg: KafkaSettings, conf: dict[str, Any]) -> dict[str, Any]:
    if cfg.msk_iam:
        if not cfg.aws_region:
            raise ConfigError("kafka.msk_iam requires kafka.aws_region")
        conf["oauth_cb"] = _msk_oauth_cb(cfg.aws_region)
    return conf


def make_producer(cfg: KafkaSettings):  # pragma: no cover - needs a broker
    from confluent_kafka import Producer

    return Producer(_augment(cfg, cfg.client_config()))


def make_consumer(cfg: KafkaSettings, extra: Optional[dict[str, Any]] = None):  # pragma: no cover
    from confluent_kafka import Consumer

    conf = cfg.client_config()
    if extra:
        conf.update(extra)
    return Consumer(_augment(cfg, conf))
