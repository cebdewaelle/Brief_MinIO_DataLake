#!/bin/sh
set -e

mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

# ── Buckets ────────────────────────────────────────────────
mc mb --ignore-existing local/raw
mc mb --ignore-existing local/staging
mc mb --ignore-existing local/curated
mc mb --ignore-existing local/archive
echo "[OK] Buckets créés"

# ── Policies ───────────────────────────────────────────────
mc admin policy create local policy-ingestion   /policies/policy-ingestion.json
mc admin policy create local policy-etl         /policies/policy-etl.json
mc admin policy create local policy-analyst     /policies/policy-analyst.json
mc admin policy create local policy-engineer    /policies/policy-engineer.json
mc admin policy create local policy-openmetadata /policies/policy-openmetadata.json
echo "[OK] Policies créées"

# ── Users ──────────────────────────────────────────────────
mc admin user add local "${MINIO_USER_INGESTION}"    "${MINIO_PASS_INGESTION}"
mc admin user add local "${MINIO_USER_ETL}"          "${MINIO_PASS_ETL}"
mc admin user add local "${MINIO_USER_ANALYST}"      "${MINIO_PASS_ANALYST}"
mc admin user add local "${MINIO_USER_ENGINEER}"     "${MINIO_PASS_ENGINEER}"
mc admin user add local "${MINIO_USER_OPENMETADATA}" "${MINIO_PASS_OPENMETADATA}"
echo "[OK] Users créés"

# ── Attachements policy → user ─────────────────────────────
mc admin policy attach local policy-ingestion    --user "${MINIO_USER_INGESTION}"
mc admin policy attach local policy-etl          --user "${MINIO_USER_ETL}"
mc admin policy attach local policy-analyst      --user "${MINIO_USER_ANALYST}"
mc admin policy attach local policy-engineer     --user "${MINIO_USER_ENGINEER}"
mc admin policy attach local policy-openmetadata --user "${MINIO_USER_OPENMETADATA}"
echo "[OK] Policies attachées aux users"

# ── SSE-S3 : chiffrement côté serveur sur tous les buckets ────
mc encrypt set sse-s3 local/raw
mc encrypt set sse-s3 local/staging
mc encrypt set sse-s3 local/curated
mc encrypt set sse-s3 local/archive
echo "[OK] SSE-S3 activé sur tous les buckets"

echo "MinIO prêt : 4 buckets, 5 policies, 5 users, SSE-S3 activé."

# ── ILM : tiering vers MinIO Cold ─────────────────────────
# Enregistre le MinIO Cold comme tier froid (COLDTIER)
mc ilm tier add minio local COLDTIER \
  --endpoint "http://minio-cold:9000" \
  --access-key "${MINIO_COLD_ROOT_USER}" \
  --secret-key "${MINIO_COLD_ROOT_PASSWORD}" \
  --bucket archive
echo "[OK] Tier COLDTIER configuré (→ minio-cold/archive)"

# Transition automatique vers COLDTIER après 180 jours
mc ilm rule add --transition-days 180 --transition-tier COLDTIER local/raw
mc ilm rule add --transition-days 180 --transition-tier COLDTIER local/staging
echo "[OK] Règles ILM transition 180 jours sur raw/ et staging/"
