# Configuration

Per-subsystem configuration files. Each maps to a settings group in
`phronexus/config.py`:

| File | Subsystem | Security |
|------|-----------|----------|
| `phronexus.yaml` | top-level (backend) | — |
| `aerospike.yaml` | operational store | TLS/mTLS + user/pass + auth mode |
| `kafka.yaml` | change-feed / state-machine transport | Amazon MSK IAM / SASL-SCRAM / mTLS |
| `iceberg.yaml` | long-term retention | Amazon S3 Tables (SigV4) or REST token/TLS + S3 creds |
| `api.yaml` | REST server + auth | server TLS/mTLS + API-key/bearer/mTLS auth |
| `observability.yaml` | logging + OpenTelemetry | — |
| `statemachine.yaml` | transactional state machine | — |

## Enabling

File config is **opt-in** so the in-memory dev default is unaffected. Point the
process at this directory:

```bash
export PHRONEXUS_CONFIG_DIR=./config
python -m phronexus.api.server
```

or programmatically:

```python
from phronexus.config import load_settings
settings = load_settings("config")
```

## Precedence (highest first)

1. Constructor kwargs
2. Environment variables (`PHRONEXUS_*`, nested with `__`)
3. `.env` file
4. These YAML files
5. Docker/K8s file secrets

So a value here is a committed default that any environment variable can override
at deploy time — e.g. `PHRONEXUS_KAFKA__SASL_PASSWORD` beats `kafka.yaml`.

## Secrets

**Never commit real credentials.** Reference them with `${ENV_VAR}` or
`${ENV_VAR:-default}` placeholders; they are interpolated from the environment
when the file loads. Certificate/key **paths** may live in the files; the private
material they point to is mounted at deploy time (K8s secret, vault sidecar, …).

```yaml
sasl_password: "${KAFKA_SASL_PASSWORD}"          # supplied by the environment
tls_keyfile: /etc/phronexus/certs/kafka-client.key   # mounted secret
```
