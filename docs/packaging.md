# Packaging — build a wheel, consume it, install air-gapped

Phronexus Core is a normal Python package (`phronexus-core`, PEP 517 /
setuptools). Distribute it as a wheel and let other apps depend on it; for
disconnected sites, ship a self-contained offline bundle.

## 1. Build the wheel

```bash
python -m build          # -> dist/phronexus_core-<ver>-py3-none-any.whl (+ .tar.gz sdist)
# or, without the `build` tool:
python -m pip wheel . -w dist --no-deps
```

The wheel is pure-Python (`py3-none-any`) — the compiled bits live in dependencies
(`pydantic-core`, `aerospike`, `confluent-kafka`, `pyarrow`), not in Phronexus. The
UI assets ship inside it (declared as package data). Bump `version` in
`pyproject.toml` per release.

## 2. Consume it as a library

Install only what you use — backends and integrations are **optional extras**, so
the core + in-memory backend stay a tiny dependency footprint:

```bash
pip install phronexus-core                          # core + in-memory backend
pip install "phronexus-core[aerospike,msk,s3tables]"  # Aerospike + AWS MSK + S3 Tables
pip install "phronexus-core[api]"                   # + the REST server
```

| Extra | Adds | For |
|---|---|---|
| `aerospike` | `aerospike` (latest — 19.x) | the Aerospike backend |
| `kafka` | `confluent-kafka` | a plain Kafka change feed |
| `msk` | `confluent-kafka`, `aws-msk-iam-sasl-signer` | **AWS-managed MSK** (IAM auth) |
| `iceberg` | `pyiceberg`, `pyarrow` | Iceberg retention (REST/Glue catalog) |
| `s3tables` | `pyiceberg`, `pyarrow`, `boto3` | **Amazon S3 Tables** (its Iceberg REST endpoint + S3 data files) |
| `api` | `fastapi`, `uvicorn` | the REST API + console |
| `client` | `httpx` | the remote SDK + HTTP output publisher |
| `oidc` | `pyjwt[crypto]`, `httpx` | Entra/Azure AD SSO |
| `otel` | OpenTelemetry SDK + OTLP exporter | traces + metrics (incl. per-op Aerospike metrics) |

**AWS-managed backends** — MSK and S3 Tables are config, not code: point the
change feed at the MSK bootstrap brokers with IAM SASL (`msk` extra), and the
Iceberg catalog at the S3 Tables Iceberg REST endpoint with S3 FileIO (`s3tables`
extra). See [deployment.md](deployment.md) → *Amazon MSK* and *Amazon S3 Tables*.

Then build on it — see [api-reference.md](api-reference.md) for the verbs and
[`examples/library_embed/`](../examples/library_embed) for embedding the whole
API (operational store + state machine + retention) in another app such as a
DishtaYantra DAG:

```python
from phronexus import Phronexus, Settings
px = Phronexus(Settings())     # its own Aerospike/Kafka connections
```

Private index (recommended for internal distribution): publish the wheel to a
private PyPI (Artifactory / CodeArtifact / devpi) and `pip install` from there,
or pin a direct wheel URL / path in the consumer's requirements.

## 3. Install on an air-gapped host (Python 3.11 – 3.14)

A wheel alone isn't enough offline — you need **every transitive dependency as a
wheel**, and because the compiled deps (`pydantic-core`, `aerospike`,
`confluent-kafka`, `pyarrow`) ship one wheel *per Python version*, the bundle
carries all of them. `scripts/build_offline_bundle.sh` builds a single wheelhouse
covering **3.11, 3.12, 3.13, and 3.14** in one shot:

```bash
# On a CONNECTED machine of the SAME OS/ARCH as the target:
scripts/build_offline_bundle.sh                         # default: aerospike,msk,s3tables,api,client,oidc,otel
scripts/build_offline_bundle.sh aerospike,msk,api       # a subset

# -> dist/phronexus-offline.tar.gz   (wheels for every Python in PHRONEXUS_PYVERS)
```

The bundle contains a `MANIFEST.txt` **coverage matrix** (which `python × extra`
combos are installable) and an installer:

```
phronexus-offline/
├── wheelhouse/     phronexus_core-*.whl (py3-none-any) + per-version dependency wheels
├── install.sh      ./install.sh [extras]   -> pip --no-index --find-links wheelhouse
└── MANIFEST.txt    build platform + coverage matrix
```

On the **air-gapped host** (no network), on any of the covered Python versions:

```bash
tar xzf phronexus-offline.tar.gz && cd phronexus-offline
./install.sh aerospike,msk,s3tables      # pip --no-index picks the wheels for THIS Python
phronexus --help
```

`--no-index` guarantees pip never reaches the internet; a clean install proves the
bundle is complete for that Python version.

### The one rule that actually bites: match the **platform**, and build natively

Compiled wheels are specific to **OS + CPU architecture + Python minor**. The
Python-version dimension is handled for you (the script downloads 3.11–3.14). The
part that trips people up is the **platform tag**:

- **Build natively** — run the script inside a machine/base image of the target's
  OS + arch (e.g. `python:3.13-slim` on linux/amd64). pip then selects the correct
  wheels automatically. This is the reliable path.
- **Avoid cross-`--platform`** unless you know the exact manylinux variant. Asking
  for a stale tag (e.g. `manylinux2014_x86_64`) silently *misses* newer
  `manylinux_2_28` wheels and reports false "no wheel" — the package is fine, the
  tag was wrong. Ship **one bundle per target architecture** (x86_64, aarch64).

> **Verified (linux/aarch64, native):** every compiled dependency — `aerospike`
> 19.x, `confluent-kafka` 2.15, `pyarrow` 25 — has wheels for **3.11, 3.12, 3.13,
> and 3.14**, so one bundle covers all four. An `[api,client,kafka]` bundle (59
> wheels) installs offline with `--no-index` into a fresh venv and imports
> `phronexus` + `fastapi` + `httpx` + `confluent_kafka` with no network.
