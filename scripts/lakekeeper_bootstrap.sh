#!/usr/bin/env bash
# Bootstrap Lakekeeper for the CTV catalog PoC:
#   1) bootstrap the server (sets initial admin + first project)
#   2) create an S3 warehouse `ctv_lakekeeper` -> s3://dataplatformpoc-venketa/lakekeeper
# Run AFTER `lakekeeper` is serving (docker compose up -d lakekeeper; migrate ran first).
#
# Unsecured PoC deployment: no OpenID, so we send a throwaway bearer token ("dummy") — Lakekeeper accepts any.
# The management API shape is version-specific — cross-check docs.lakekeeper.io if a call 4xx's.
# Needs: curl, jq. Reads AWS creds from env (same AWS_* as the rest of the stack).
set -euo pipefail

LK=${LK:-http://localhost:8282}                 # host-published Lakekeeper (8282 -> container 8181)
TOKEN=${TOKEN:-dummy}                            # unsecured: any bearer works
WAREHOUSE=${WAREHOUSE:-ctv_lakekeeper}
BUCKET=${BUCKET:-dataplatformpoc-venketa}
PREFIX=${PREFIX:-lakekeeper}
REGION=${AWS_REGION:-us-east-2}
: "${AWS_ACCESS_KEY_ID:?set AWS_ACCESS_KEY_ID}"
: "${AWS_SECRET_ACCESS_KEY:?set AWS_SECRET_ACCESS_KEY}"

echo "1) bootstrap the server (initial admin + first project)"
curl -s -X POST "$LK/management/v1/bootstrap" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"accept-terms-of-use": true}' ; echo

echo "2) create S3 warehouse $WAREHOUSE -> s3://$BUCKET/$PREFIX"
# sts-enabled=false: no IAM role to assume (PoC). With access-key creds + STS off, Lakekeeper uses S3 remote
# signing (the client asks Lakekeeper to sign each S3 request) rather than vending temporary creds. That works
# for in-network Trino; the external-Databricks path needs BASE_URI reachable (revisit at the cross-cloud step).
curl -s -X POST "$LK/management/v1/warehouse" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{
  "warehouse-name": "'"$WAREHOUSE"'",
  "storage-profile": {
    "type": "s3",
    "bucket": "'"$BUCKET"'",
    "key-prefix": "'"$PREFIX"'",
    "region": "'"$REGION"'",
    "sts-enabled": false,
    "flavor": "aws"
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "'"$AWS_ACCESS_KEY_ID"'",
    "aws-secret-access-key": "'"$AWS_SECRET_ACCESS_KEY"'"
  }
}' ; echo

echo
echo "DONE. Wire Trino: cp scripts/lakekeeper.properties.staged infra/trino/catalog/lakekeeper.properties"
echo "then: docker compose up -d trino   (recreate to load the new catalog)"
echo "Verify: docker exec -i trino trino --execute \"SHOW CATALOGS\"   # expect 'lakekeeper'"
