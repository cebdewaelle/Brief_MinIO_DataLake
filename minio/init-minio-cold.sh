#!/bin/sh
set -e

mc alias set cold http://minio-cold:9000 "${MINIO_COLD_ROOT_USER}" "${MINIO_COLD_ROOT_PASSWORD}"

# ── Bucket d'archive (backend physique du tier froid) ──────
mc mb --ignore-existing cold/archive
echo "[OK] Bucket archive créé sur MinIO Cold"

# ── ILM : suppression définitive après 730 jours (2 ans) ──
mc ilm rule add --expire-days 730 cold/archive
echo "[OK] Règle ILM expiration 730 jours sur archive/"

echo "MinIO Cold prêt : bucket archive + règle ILM 730 jours."
