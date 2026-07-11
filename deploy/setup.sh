#!/usr/bin/env bash
#
# One-command local deploy + smoke test for Phronexus (Aerospike + Kafka).
#   cd deploy && ./setup.sh
#
# Requires Docker Desktop (with `docker compose`). Tear down with:
#   cd deploy && docker compose down -v
set -euo pipefail
cd "$(dirname "$0")"

API="http://localhost:8080"
say() { printf "\n\033[1;36m==> %s\033[0m\n" "$*"; }
ok()  { printf "\033[1;32m  ✓ %s\033[0m\n" "$*"; }

command -v docker >/dev/null || { echo "Docker is required"; exit 1; }
docker compose version >/dev/null || { echo "'docker compose' (v2) is required"; exit 1; }

say "Building images and starting the stack (Aerospike + Redpanda + Phronexus API)"
docker compose up -d --build

say "Waiting for the API to become ready"
for i in $(seq 1 60); do
  if curl -fsS "$API/readyz" >/dev/null 2>&1; then ok "API ready"; break; fi
  [ "$i" = 60 ] && { echo "API did not become ready; logs:"; docker compose logs --tail=50 phronexus-api; exit 1; }
  sleep 2
done

say "Signing in (default creds admin/admin — change in docker-compose.yml)"
TOKEN=$(curl -fsS -X POST "$API/auth/login" -H 'content-type: application/json' \
  -d '{"username":"admin","password":"admin"}' | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[ -n "$TOKEN" ] && ok "logged in (JWT issued)" || { echo "login failed"; exit 1; }
AUTH=(-H "Authorization: Bearer $TOKEN")

say "Ingesting contracts into Aerospike (source of truth)"
docker compose exec -T phronexus-api python -m phronexus.cli ingest examples/bond >/dev/null
docker compose exec -T phronexus-api python -m phronexus.cli ingest contracts_examples >/dev/null
curl -fsS "${AUTH[@]}" -X POST "$API/contracts/refresh" >/dev/null
ok "contracts ingested and cache refreshed"
echo "  stored contracts:"; curl -fsS "${AUTH[@]}" "$API/contracts" | sed 's/,/,\n   /g' | head -20

BOND='{"document":{"isin":"US0378331005","issuer":"APPLE","coupon":3.85,"currency":"USD","maturity_date":20310215,"callable":true}}'

say "Smoke test"
echo "-- validate (JSON Schema + DQ):"
curl -fsS "${AUTH[@]}" -X POST "$API/entities/bond/validate" -H 'content-type: application/json' -d "$BOND"; echo
echo "-- write:"
curl -fsS "${AUTH[@]}" -X PUT "$API/entities/bond/documents" -H 'content-type: application/json' -d "$BOND"; echo
echo "-- read:"
curl -fsS "${AUTH[@]}" "$API/entities/bond/documents/US0378331005"; echo
echo "-- query (issuer=APPLE):"
curl -fsS "${AUTH[@]}" -X POST "$API/entities/bond/query" -H 'content-type: application/json' \
  -d '{"where":[{"field":"issuer","op":"eq","value":"APPLE"}]}'; echo
echo "-- lifecycle event (BondIssued -> active, emits to Kafka):"
curl -fsS "${AUTH[@]}" -X POST "$API/entities/bond/events" -H 'content-type: application/json' \
  -d '{"event_type":"BondIssued","key":"US0378331005","event_id":"evt-1","payload":'"$(echo "$BOND" | sed 's/^{"document"://;s/}$//')"'}'; echo

say "Done"
ok "Management UI:  $API/ui/   (login: admin / admin)"
ok "API:            $API"
ok "Swagger UI:     $API/docs"
ok "Readiness:      $API/readyz"
ok "Redpanda admin: http://localhost:9644/public_metrics"
echo
echo "Optional — start the autonomous Kafka state-machine runner:"
echo "  docker compose --profile workers up -d phronexus-runner"
echo "Tear down:"
echo "  docker compose down -v"
