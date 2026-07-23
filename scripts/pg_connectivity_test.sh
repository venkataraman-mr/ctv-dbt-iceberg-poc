#!/usr/bin/env bash
# Prod Postgres connectivity test from the AWS VM.
# Run after DevOps opens the cross-cloud firewall. Tests both the HOST and the CONTAINER
# network paths (Trino/PyIceberg connect from inside containers, so that path is what matters).
# Reads PG_* from .env. Layers: DNS -> TCP -> Postgres auth (auth only if creds are filled).
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; [ -f .env ] && . ./.env; set +a
: "${PG_HOST:=azeus2-postgres-mrdpp-p-01.vivvix.net}"
: "${PG_PORT:=5432}"
PG_IP="10.198.107.69"   # private IP recorded in the architecture doc (Part 2.1)

echo "== target: $PG_HOST:$PG_PORT  (also testing private IP $PG_IP) =="

echo "-- 1. DNS resolution (VM host) --"
getent hosts "$PG_HOST" || { echo "  getent: no result; trying nslookup"; nslookup "$PG_HOST" 2>/dev/null || echo "  DNS FAILED"; }

tcp() { timeout 5 bash -c "</dev/tcp/$1/$2" 2>/dev/null && echo "  OPEN            $1:$2" || echo "  BLOCKED/timeout $1:$2"; }
echo "-- 2. TCP reachability from the VM host --"
tcp "$PG_HOST" "$PG_PORT"
tcp "$PG_IP"   "$PG_PORT"

echo "-- 3. TCP from inside the ingestion container (the path Trino/PyIceberg use) --"
docker exec ingestion python - "$PG_HOST" "$PG_IP" "$PG_PORT" <<'PY' 2>/dev/null || echo "  (ingestion container not running?)"
import socket, sys
host, ip, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
for tgt in [(host, port), (ip, port)]:
    try:
        socket.create_connection(tgt, 5).close(); print("  OPEN            ", tgt)
    except Exception as e:
        print("  BLOCKED         ", tgt, "->", type(e).__name__, e)
PY

echo "-- 4. Postgres auth (only if PG_USER/PG_PASSWORD/PG_DB are set in .env) --"
if [ -n "${PG_USER:-}" ] && [[ "${PG_USER:-}" != REPLACE* ]] && [ -n "${PG_PASSWORD:-}" ]; then
  echo "  (pulls postgres:16-alpine ~80MB the first time; disk is tight — prune if needed)"
  docker run --rm -e PGPASSWORD="$PG_PASSWORD" postgres:16-alpine \
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "${PG_DB}" \
    -c "select current_user, current_database(), version();" \
    && echo "  AUTH OK" || echo "  AUTH FAILED (see psql error above)"
else
  echo "  skipped — fill PG_USER / PG_PASSWORD / PG_DB in .env to run the auth check"
fi
echo "== done =="
