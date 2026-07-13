# Running on Amazon Linux (no Docker, air-gapped)

How to run the Phronexus engine directly on an Amazon Linux host (EC2 or
on-prem), against an Aerospike cluster you already run, with **no internet at
runtime and no Docker**. Tested shape: Amazon Linux 2023, Python 3.11.

## Does it run without FastAPI?

**Yes.** FastAPI is only pulled in by the `api` extra and is used *only* for the
REST API + web console. The engine — the Python library and the `phronexus` CLI —
imports no FastAPI and runs fully headless.

| Mode | Needs FastAPI? | Install extra | Entry point |
|---|---|---|---|
| **Library** (embed in your process) | no | `[aerospike]` | `from phronexus import Phronexus, Settings` |
| **CLI** (ingest / put / get / query / backfill / resync / doctor) | no | `[aerospike]` | `phronexus …` |
| **REST API + web UI** | yes | `[aerospike,api]` | `python -m phronexus.api.server` |

Kafka (`[kafka]`) and Iceberg (`[iceberg]`) are separate extras and are **off by
default** — skip them for an Aerospike-only deployment.

---

## 1. Build the offline bundle (on a connected host)

`phronexus-core` is pure-Python, but its compiled dependencies (`pydantic-core`,
`aerospike`, and — only if you add them — `pyarrow`, `confluent-kafka`) ship one
wheel **per Python version and per platform**. Build the wheelhouse on a
machine with internet, targeting the **same CPU arch as the air-gapped host**:

```bash
# x86_64 target, Python 3.11, headless engine (Aerospike only):
PHRONEXUS_PYVERS=3.11 PHRONEXUS_PLATFORM=manylinux2014_x86_64 \
  scripts/build_offline_bundle.sh aerospike

# ...or include the REST API + UI:
PHRONEXUS_PYVERS=3.11 PHRONEXUS_PLATFORM=manylinux2014_x86_64 \
  scripts/build_offline_bundle.sh aerospike,api

# Graviton / arm64 host: use manylinux2014_aarch64
```

This writes `dist/phronexus-offline/` (a `wheelhouse/`, an `install.sh`, and a
`MANIFEST.txt` coverage matrix). **Read `MANIFEST.txt`** — it lists which
`(python, extra)` combos have wheels. Copy the whole `dist/phronexus-offline/`
directory to the air-gapped host (S3 in-VPC, SSM, a signed artifact, USB, …).

> Match the build to the target: same Python minor (`PHRONEXUS_PYVERS`) and same
> platform tag (`PHRONEXUS_PLATFORM`). A wheel built for `x86_64` won't install on
> Graviton and vice versa.

---

## 2. Prepare the Amazon Linux host

Amazon Linux 2023 defaults to Python 3.9; install 3.11 (no build tools needed —
the wheels are prebuilt binaries):

```bash
sudo dnf install -y python3.11 python3.11-pip
```

> Only if a wheel is missing for your combo and pip must compile from an sdist:
> `sudo dnf install -y gcc gcc-c++ python3.11-devel openssl-devel zlib-devel`.
> With a complete `manylinux` wheelhouse this is unnecessary.

Create an isolated venv:

```bash
sudo mkdir -p /opt/phronexus && sudo chown "$USER" /opt/phronexus
python3.11 -m venv /opt/phronexus/venv
```

---

## 3. Install offline (no network)

```bash
# from the transferred bundle directory:
PYTHON=/opt/phronexus/venv/bin/python ./install.sh aerospike           # headless engine
# or, for the REST API + UI:
PYTHON=/opt/phronexus/venv/bin/python ./install.sh aerospike,api
```

`install.sh` runs `pip install --no-index --find-links=wheelhouse …`, so it never
touches the network. Equivalent manual form:

```bash
/opt/phronexus/venv/bin/pip install --no-index --find-links=wheelhouse \
  'phronexus-core[aerospike]'
```

---

## 4. Configure

Point Phronexus at your Aerospike cluster via env vars (nested keys use `__`).
For a service, put them in an EnvironmentFile:

```ini
# /etc/phronexus/phronexus.env
PHRONEXUS_BACKEND=aerospike
PHRONEXUS_AEROSPIKE__HOSTS=10.0.1.10:3000        # your cluster; comma-separated for multiple
PHRONEXUS_AEROSPIKE__NAMESPACE=phronexus         # must match a namespace your cluster defines
PHRONEXUS_AEROSPIKE__USE_NATIVE_TXN=false        # false for Community Edition; true only on EE 8.0+ SC
PHRONEXUS_OBSERVABILITY__LOG_LEVEL=INFO
# EE security (optional):
# PHRONEXUS_AEROSPIKE__USER=... / __PASSWORD=... / __AUTH_MODE=INTERNAL
# PHRONEXUS_AEROSPIKE__TLS_ENABLE=true / __TLS_CAFILE=/etc/pki/as-ca.pem / __TLS_CERTFILE / __TLS_KEYFILE
```

Kafka and Iceberg are unset → in-process `MemorySink`, no cold tier, nothing
leaves the host except Aerospike traffic. (Aerospike namespaces are static, set
in the server's `aerospike.conf`; make sure the one above exists on your cluster.)

---

## 5. Run

### Headless (no FastAPI)

```bash
export $(grep -v '^#' /etc/phronexus/phronexus.env | xargs)   # load config into the shell
source /opt/phronexus/venv/bin/activate

phronexus ingest /opt/phronexus/contracts        # load your contracts into Aerospike (source of truth)
phronexus doctor                                  # durability preflight (non-zero exit if config can lose data)
phronexus put trade '{"trade_id":"T-1","counterparty":"GS","notional":1000000,"ccy":"USD","trade_date":20260713,"book":"R"}'
phronexus get trade T-1
phronexus query trade '[{"field":"counterparty","op":"eq","value":"GS"}]'
```

Or embed it as a library in your own service — no CLI, no FastAPI:

```python
from phronexus import Phronexus, Settings
px = Phronexus(Settings())            # reads PHRONEXUS_* from the environment
px.load_contract_dir("/opt/phronexus/contracts")
px.put("trade", {...}); print(px.get("trade", "T-1"))
px.close()
```

### REST API + web console (needs the `api` extra)

Install as a systemd service:

```ini
# /etc/systemd/system/phronexus-api.service
[Unit]
Description=Phronexus API
After=network-online.target
Wants=network-online.target

[Service]
EnvironmentFile=/etc/phronexus/phronexus.env
Environment=PHRONEXUS_API__AUTH__SCHEMES=["jwt"]
Environment=PHRONEXUS_API__AUTH__JWT_SECRET=change-me
ExecStart=/opt/phronexus/venv/bin/python -m phronexus.api.server
Restart=on-failure
User=phronexus

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable --now phronexus-api
curl -fsS http://localhost:8080/readyz          # readiness
# REST at :8080, web console at :8080/ui, Swagger at :8080/docs
```

Set the bind host/port via `PHRONEXUS_API__HOST` / `PHRONEXUS_API__PORT`. If
clients are off-box, open the port in the instance's security group / firewalld.

---

## Notes

- **Community Edition** has no multi-record transactions → keep
  `USE_NATIVE_TXN=false` (the manifest-last write still gives all-or-nothing
  visibility). On EE 8.0+ with strong consistency, set it `true`. If you use the
  state machine on CE, also set `PHRONEXUS_STATEMACHINE__REQUIRE_ATOMIC=false`.
- **Air-gapped runtime.** Nothing phones home: Kafka off, Iceberg `memory`,
  `otel_enabled=false` by default. The only egress is to your Aerospike cluster
  (and, if you enable the API, inbound from your clients).
- **Adding tiers later.** Kafka change feed → add the `[kafka]` extra +
  `PHRONEXUS_KAFKA__*`; Iceberg retention + `resync` → add the `[iceberg]` extra +
  `PHRONEXUS_ICEBERG__*` (see [resync.md](resync.md), [deployment.md](deployment.md)).
- **Packaging details** (extras matrix, coverage caveats) are in
  [packaging.md](packaging.md).
