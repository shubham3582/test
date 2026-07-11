# Local Docker deployment (Aerospike + Kafka + Phronexus)

Run the whole stack on your Mac with Docker Desktop and smoke-test it.

## Quick start

```bash
cd deploy
./setup.sh
```

That will:
1. Build the Phronexus image and start **Aerospike** (Community Edition),
   **Redpanda** (Kafka-compatible), and the **Phronexus API**.
2. Wait for readiness, ingest the example contracts into Aerospike, and refresh.
3. Run a smoke test: validate → write → read → query → lifecycle event.

Then open:
- **Management UI:** http://localhost:8080/ui/  (login: **admin / admin**)
- **Swagger UI:** http://localhost:8080/docs
- **Readiness:** http://localhost:8080/readyz

The UI lets you browse/edit contracts (validate-before-save), validate documents,
drive state machines, and browse data. Auth is JWT login (change the default
creds and `JWT_SECRET` in `docker-compose.yml`); it can integrate with Microsoft
Entra / Azure AD — see [../docs/deployment.md](../docs/deployment.md#management-ui).

Tear down (removes data volumes):

```bash
docker compose down -v
```

## What's running

| Service | Image | Ports | Notes |
|---|---|---|---|
| `phx-aerospike` | `aerospike/aerospike-server:7.1.0.0` (CE) | 3000 | operational store |
| `phx-redpanda` | `redpandadata/redpanda:v24.1.7` | 9092, 9644 | Kafka API + admin |
| `phx-api` | built from `../Dockerfile` | 8080 | REST API |
| `phx-runner` | built from `../Dockerfile` | — | optional (profile `workers`) |

## Try it by hand

```bash
BOND='{"document":{"isin":"US0378331005","issuer":"APPLE","coupon":3.85,"currency":"USD","maturity_date":20310215,"callable":true}}'

curl -X PUT  localhost:8080/entities/bond/documents -H 'content-type: application/json' -d "$BOND"
curl         localhost:8080/entities/bond/documents/US0378331005
curl -X POST localhost:8080/entities/bond/query -H 'content-type: application/json' \
     -d '{"where":[{"field":"issuer","op":"eq","value":"APPLE"}],"sort":[{"field":"coupon","order":"desc"}]}'
curl -X POST localhost:8080/entities/bond/events -H 'content-type: application/json' \
     -d '{"event_type":"BondIssued","key":"US0378331005","event_id":"e1","payload":{"isin":"US0378331005","issuer":"APPLE","coupon":3.85,"currency":"USD","maturity_date":20310215,"callable":true}}'

# contracts live in Aerospike (source of truth):
curl localhost:8080/contracts
```

### Watch the Kafka change-feed

```bash
docker compose exec redpanda rpk topic list
docker compose exec redpanda rpk topic consume phronexus.commits.bond --num 5
```

### Run the autonomous state-machine runner

```bash
docker compose --profile workers up -d phronexus-runner

# feed it a domain event via Kafka:
docker compose exec redpanda rpk topic create bonds.events
docker compose exec redpanda rpk topic produce bonds.events <<'EOF'
{"entity":"bond","event_type":"BondCalled","key":"US0378331005","event_id":"e2","payload":{}}
EOF
docker compose logs -f phronexus-runner
```

## Notes & troubleshooting

- **Apple Silicon (arm64).** The images are multi-arch. If one lacks an arm64
  build, add `platform: linux/amd64` to that service in `docker-compose.yml`
  (Docker Desktop emulates it). You can also bump the image tags if a pull fails.
- **Community Edition has no multi-record transactions.** The compose sets
  `PHRONEXUS_AEROSPIKE__USE_NATIVE_TXN=false`. The manifest pattern still gives
  all-or-nothing *visibility* (the manifest is written last), and index /
  change-feed writes are ordered before it — but they are **not truly atomic** on
  CE. For production atomicity guarantees (and effectively-once state machine)
  use **Aerospike Enterprise + a strong-consistency namespace** and set
  `use_native_txn=true`.
- **Aerospike config.** The image renders its config from a template on startup,
  so this stack mounts `aerospike.conf` at `/etc/aerospike/aerospike.template.conf`
  (not `aerospike.conf`). If your image tag doesn't use that template path and the
  container exits with `/etc/aerospike/aerospike.conf: Read-only file system`,
  fall back to env-var templating: remove the volume and set
  `NAMESPACE: phronexus` (and optionally `MEM_GB: "1"`, `STORAGE_GB: "1"`,
  `DEFAULT_TTL: "0"`) in the `aerospike` service environment.
- **Logs:** `docker compose logs -f phronexus-api` (or `phx-aerospike`, `phx-redpanda`).
- **Rebuild after code changes:** `docker compose up -d --build phronexus-api`.
- **First build is slow** (installs the Aerospike + Kafka clients); subsequent
  builds are cached.

## Config

The API is configured entirely by environment variables in `docker-compose.yml`
(`PHRONEXUS_*`). For a TLS/mTLS/MSK/S3-Tables setup, point
`PHRONEXUS_CONFIG_DIR` at a mounted config directory instead — see
[`../config/`](../config) and [`../docs/deployment.md`](../docs/deployment.md).
