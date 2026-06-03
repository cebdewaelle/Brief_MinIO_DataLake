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
mc admin policy create local policy-openmetadata /policies/policy-openmetadata.json
echo "[OK] Policies créées"

# ── Users ──────────────────────────────────────────────────
mc admin user add local "${MINIO_USER_INGESTION}"    "${MINIO_PASS_INGESTION}"
mc admin user add local "${MINIO_USER_ETL}"          "${MINIO_PASS_ETL}"
mc admin user add local "${MINIO_USER_ANALYST}"      "${MINIO_PASS_ANALYST}"
mc admin user add local "${MINIO_USER_OPENMETADATA}" "${MINIO_PASS_OPENMETADATA}"
echo "[OK] Users créés"

# ── Attachements policy → user ─────────────────────────────
mc admin policy attach local policy-ingestion    --user "${MINIO_USER_INGESTION}"
mc admin policy attach local policy-etl          --user "${MINIO_USER_ETL}"
mc admin policy attach local policy-analyst      --user "${MINIO_USER_ANALYST}"
mc admin policy attach local policy-openmetadata --user "${MINIO_USER_OPENMETADATA}"
echo "[OK] Policies attachées aux users"

echo "MinIO prêt : 4 buckets, 4 policies, 4 users."
