#!/usr/bin/env bash
# Bootstrap an Apache Polaris catalog for the CTV catalog PoC:
#   catalog `ctv_poc` -> s3://dataplatformpoc-venketa/polaris  + a Trino principal + RBAC grants.
# Run AFTER the `polaris` service is up (docker compose ... up -d polaris).
#
# The Polaris MANAGEMENT API shape is version-specific — cross-check against
# https://polaris.apache.org/ (management API) if a call 4xx's. This is a documented starting point.
# Needs: curl, jq.
set -euo pipefail

POLARIS=${POLARIS:-http://localhost:8181}
ROOT_CLIENT=${ROOT_CLIENT:-root}
ROOT_SECRET=${ROOT_SECRET:-s3cr3t}          # matches POLARIS_BOOTSTRAP_CREDENTIALS in the overlay
CATALOG=${CATALOG:-ctv_poc}
S3_LOC=${S3_LOC:-s3://dataplatformpoc-venketa/polaris}
ROLE_ARN=${ROLE_ARN:-}                       # optional: AWS role for Polaris storage; else it uses AWS_* env keys

echo "1) get root OAuth2 token"
TOKEN=$(curl -s -X POST "$POLARIS/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d "client_id=$ROOT_CLIENT" -d "client_secret=$ROOT_SECRET" \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
[ -n "$TOKEN" ] && [ "$TOKEN" != null ] || { echo "no token — check root creds / endpoint"; exit 1; }

echo "2) create catalog $CATALOG -> $S3_LOC"
curl -s -X POST "$POLARIS/api/management/v1/catalogs" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{
  "catalog": {
    "name": "'"$CATALOG"'",
    "type": "INTERNAL",
    "properties": { "default-base-location": "'"$S3_LOC"'" },
    "storageConfigInfo": { "storageType": "S3", "allowedLocations": ["'"$S3_LOC"'"]'"${ROLE_ARN:+, \"roleArn\": \"$ROLE_ARN\"}"' }
  }
}' ; echo

echo "3) create a principal for Trino (capture its client_id:client_secret from the output!)"
curl -s -X POST "$POLARIS/api/management/v1/principals" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"principal": {"name": "trino_poc"}}' ; echo

echo "4) create roles + grants (principal-role -> catalog-role -> CATALOG_MANAGE_CONTENT) and assign to trino_poc"
curl -s -X POST "$POLARIS/api/management/v1/principal-roles" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"principalRole": {"name": "trino_role"}}' ; echo
curl -s -X PUT "$POLARIS/api/management/v1/principals/trino_poc/principal-roles" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"principalRole": {"name": "trino_role"}}' ; echo
curl -s -X POST "$POLARIS/api/management/v1/catalogs/$CATALOG/catalog-roles" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"catalogRole": {"name": "admin_role"}}' ; echo
curl -s -X PUT "$POLARIS/api/management/v1/catalogs/$CATALOG/catalog-roles/admin_role/grants" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' ; echo
curl -s -X PUT "$POLARIS/api/management/v1/principal-roles/trino_role/catalog-roles/$CATALOG" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"catalogRole": {"name": "admin_role"}}' ; echo

echo
echo "DONE. Put the trino_poc client_id:client_secret (from step 3) into scripts/polaris.properties.staged"
echo "(iceberg.rest-catalog.oauth2.credential), copy that file to infra/trino/catalog/polaris.properties,"
echo "then: docker compose restart trino"
echo
echo "RBAC test idea: repeat step 3 for a 2nd principal with a READ-ONLY catalog role"
echo "(privilege TABLE_READ_DATA instead of CATALOG_MANAGE_CONTENT) and verify it can SELECT but not INSERT."
