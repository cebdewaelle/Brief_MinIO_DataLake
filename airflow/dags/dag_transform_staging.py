"""
DAG de transformation raw/ → staging/ (schéma Silver unifié).

Schéma de sortie :
  timestamp, line_id, temperature, pressure, elapsed_time, label, source_file, ingested_at

Transformations appliquées via apply_silver_schema() :
  - Colonnes en lowercase, elapsed_time absent → NULL
  - Timestamp ISO 8601, types numériques explicites
  - Ajout line_id, source_file, ingested_at
  - Déduplication sur (timestamp, line_id)
  - Format : Parquet compressé gzip
"""

import io
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datalake_datasets import RAW_MULTI

RAW_BUCKET = "raw"
STAGING_BUCKET = "staging"
DATA_DIR = Path("/opt/airflow/data")
LINES = ["lineB", "lineC", "lineD", "lineE"]  # lineA gérée par transform_lineA_batch (chunks journaliers)

default_args = {"owner": "engineer", "retries": 1}


def find_csv(line_name: str) -> Path:
    letter = line_name[-1].upper()
    matches = list(DATA_DIR.glob(f"Line{letter}*.csv"))
    if not matches:
        raise FileNotFoundError(f"Aucun CSV trouvé pour {line_name} dans {DATA_DIR}")
    return matches[0]


def transform_line(line_name: str, **context):
    from minio_utils import (
        already_uploaded,
        build_key,
        compute_md5_bytes,
        extract_partition,
        get_object_as_bytes,
        get_s3_client,
    )

    csv_path = find_csv(line_name)
    year, month = extract_partition(csv_path)

    raw_key = build_key(line_name, year, month, csv_path.name)
    parquet_name = csv_path.stem + ".parquet"
    staging_key = build_key(line_name, year, month, parquet_name)

    client = get_s3_client(role="etl")
    raw_bytes = get_object_as_bytes(client, RAW_BUCKET, raw_key)
    df = pd.read_csv(io.BytesIO(raw_bytes))

    # ── Schéma Silver unifié ───────────────────────────────────────────
    from minio_utils import apply_silver_schema
    df = apply_silver_schema(df, line_id=line_name, source_file=csv_path.name)

    # ── Sérialisation Parquet gzip ─────────────────────────────────────
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="gzip", engine="pyarrow")
    parquet_bytes = buffer.getvalue()

    hex_md5, b64_md5 = compute_md5_bytes(parquet_bytes)

    if already_uploaded(client, STAGING_BUCKET, staging_key, hex_md5):
        print(f"[SKIP] {parquet_name} déjà présent en staging avec MD5 identique.")
        return

    client.put_object(
        Bucket=STAGING_BUCKET,
        Key=staging_key,
        Body=parquet_bytes,
        ContentType="application/octet-stream",
        ContentMD5=b64_md5,
        Metadata={
            "md5": hex_md5,
            "source": raw_key,
            "rows": str(len(df)),
        },
    )
    print(f"[OK] {parquet_name} → s3://{STAGING_BUCKET}/{staging_key}")
    print(f"     {len(df)} lignes | MD5 : {hex_md5}")


def verify_staging(**context):
    from botocore.exceptions import ClientError
    from minio_utils import build_key, extract_partition, get_s3_client

    client = get_s3_client(role="etl")
    errors = []

    for line_name in LINES:
        csv_path = find_csv(line_name)
        year, month = extract_partition(csv_path)
        parquet_name = csv_path.stem + ".parquet"
        key = build_key(line_name, year, month, parquet_name)

        try:
            resp = client.head_object(Bucket=STAGING_BUCKET, Key=key)
            meta = resp.get("Metadata", {})
            rows = meta.get("rows", "?")
            md5 = meta.get("md5", "?")
            print(f"[OK] {parquet_name} — {rows} lignes | MD5 : {md5}")
        except ClientError:
            errors.append(f"{parquet_name} : absent de staging/")

    if errors:
        raise ValueError("Fichiers manquants en staging :\n" + "\n".join(errors))

    print("Vérification staging complète.")


with DAG(
    dag_id="transform_staging",
    default_args=default_args,
    description="Transformation raw/ → staging/ : harmonisation colonnes + format timestamp",
    schedule=[RAW_MULTI],  # déclenché automatiquement après ingest_raw_csv
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "staging", "transform"],
) as dag:

    transform_tasks = [
        PythonOperator(
            task_id=f"transform_{line}",
            python_callable=transform_line,
            op_kwargs={"line_name": line},
        )
        for line in LINES
    ]

    verify_task = PythonOperator(
        task_id="verify_staging",
        python_callable=verify_staging,
    )

    transform_tasks >> verify_task
