from __future__ import annotations

from phronexus.config import Settings, load_settings


def _write(dirpath, name, text):
    (dirpath / name).write_text(text)


def test_defaults_without_config_dir():
    s = Settings(backend="memory")
    assert s.backend == "memory"
    assert s.kafka.security_protocol == "PLAINTEXT"


def test_loads_per_subsystem_files(tmp_path, monkeypatch):
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "s3cret")
    _write(tmp_path, "phronexus.yaml", "backend: aerospike\n")
    _write(tmp_path, "aerospike.yaml", "hosts: 10.0.0.1:4333\ntls_enable: true\nauth_mode: EXTERNAL\n")
    _write(tmp_path, "kafka.yaml", (
        "enabled: true\n"
        "security_protocol: SASL_SSL\n"
        "ssl_certfile: /certs/client.pem\n"
        "sasl_mechanism: SCRAM-SHA-512\n"
        "sasl_username: svc\n"
        "sasl_password: \"${KAFKA_SASL_PASSWORD}\"\n"
    ))
    _write(tmp_path, "iceberg.yaml", (
        "enabled: true\nbackend: iceberg\n"
        "catalog_token: \"${ICEBERG_TOKEN:-tok-default}\"\n"
        "s3_access_key_id: AKIA\ns3_secret_access_key: shh\n"
    ))

    s = load_settings(str(tmp_path))
    assert s.backend == "aerospike"
    assert s.aerospike.hosts == "10.0.0.1:4333" and s.aerospike.tls_enable is True
    # env interpolation into the file value
    assert s.kafka.sasl_password == "s3cret"
    # ${VAR:-default} fallback when the env var is absent
    assert s.iceberg.catalog_token == "tok-default"


def test_env_overrides_file(tmp_path, monkeypatch):
    _write(tmp_path, "kafka.yaml", "enabled: true\nbootstrap_servers: file-broker:9092\nclient_id: from-file\n")
    monkeypatch.setenv("PHRONEXUS_KAFKA__BOOTSTRAP_SERVERS", "env-broker:9092")
    s = load_settings(str(tmp_path))
    # env wins for the field it sets...
    assert s.kafka.bootstrap_servers == "env-broker:9092"
    # ...while file-only fields survive the deep merge.
    assert s.kafka.client_id == "from-file"
    assert s.kafka.enabled is True


def test_kafka_client_config_maps_security():
    s = Settings(backend="memory")
    s.kafka.security_protocol = "SASL_SSL"
    s.kafka.ssl_cafile = "/ca.pem"
    s.kafka.ssl_certfile = "/client.pem"
    s.kafka.ssl_keyfile = "/client.key"
    s.kafka.sasl_mechanism = "SCRAM-SHA-512"
    s.kafka.sasl_username = "svc"
    s.kafka.sasl_password = "pw"
    cfg = s.kafka.client_config()
    assert cfg["security.protocol"] == "SASL_SSL"
    assert cfg["ssl.ca.location"] == "/ca.pem"
    assert cfg["ssl.certificate.location"] == "/client.pem"   # mTLS
    assert cfg["ssl.key.location"] == "/client.key"
    assert cfg["sasl.mechanism"] == "SCRAM-SHA-512"
    assert cfg["sasl.username"] == "svc" and cfg["sasl.password"] == "pw"


def test_kafka_plaintext_has_no_tls_keys():
    cfg = Settings(backend="memory").kafka.client_config()
    assert cfg["security.protocol"] == "PLAINTEXT"
    assert "ssl.ca.location" not in cfg and "sasl.mechanism" not in cfg


def test_kafka_msk_iam_client_config():
    s = Settings(backend="memory")
    s.kafka.msk_iam = True
    s.kafka.aws_region = "us-east-1"
    cfg = s.kafka.client_config()
    assert cfg["security.protocol"] == "SASL_SSL"
    assert cfg["sasl.mechanism"] == "OAUTHBEARER"
    # The token callback is attached at client construction, not in the dict.
    assert "oauth_cb" not in cfg


def test_iceberg_s3tables_sigv4_properties():
    s = Settings(backend="memory")
    s.iceberg.sigv4_enabled = True
    s.iceberg.signing_name = "s3tables"
    s.iceberg.signing_region = "us-east-1"
    s.iceberg.catalog_uri = "https://s3tables.us-east-1.amazonaws.com/iceberg"
    s.iceberg.warehouse = "arn:aws:s3tables:us-east-1:123:bucket/phronexus"
    props = s.iceberg.catalog_properties()
    assert props["type"] == "rest"
    assert props["rest.sigv4-enabled"] == "true"
    assert props["rest.signing-name"] == "s3tables"
    assert props["rest.signing-region"] == "us-east-1"
    assert props["warehouse"].startswith("arn:aws:s3tables:")


def test_iceberg_catalog_properties():
    s = Settings(backend="memory")
    s.iceberg.catalog_token = "tok"
    s.iceberg.catalog_tls_cafile = "/ca.pem"
    s.iceberg.catalog_tls_certfile = "/client.pem"
    s.iceberg.s3_endpoint = "https://minio:9000"
    s.iceberg.s3_access_key_id = "AKIA"
    s.iceberg.s3_secret_access_key = "shh"
    props = s.iceberg.catalog_properties()
    assert props["token"] == "tok"
    assert props["ssl.cabundle"] == "/ca.pem" and props["ssl.client.cert"] == "/client.pem"
    assert props["s3.endpoint"] == "https://minio:9000"
    assert props["s3.access-key-id"] == "AKIA" and props["s3.secret-access-key"] == "shh"


def test_example_config_dir_loads(monkeypatch):
    # The shipped config/ templates must parse (secrets resolve to blank/defaults).
    s = load_settings("config")
    assert s.aerospike.tls_enable is True
    # Kafka template targets Amazon MSK IAM.
    assert s.kafka.msk_iam is True and s.kafka.aws_region
    assert s.kafka.client_config()["sasl.mechanism"] == "OAUTHBEARER"
    # Iceberg template targets Amazon S3 Tables via SigV4.
    assert s.iceberg.backend == "iceberg" and s.iceberg.sigv4_enabled is True
    assert s.iceberg.signing_name == "s3tables"
