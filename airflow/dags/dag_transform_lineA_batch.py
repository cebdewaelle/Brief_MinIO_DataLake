"""
DAG de transformation des chunks journaliers LineA : raw/ → staging/.

Contrairement à transform_staging qui lit depuis le filesystem local,
ce DAG découvre les chunks directement dans raw/ via list_objects_v2.
C'est le pattern correct pour un transform : il ne dépend pas du filesystem
local mais uniquement de ce qui est présent dans le DataLake.

Transformations appliquées (identiques à transform_staging) :
  - Colonnes en lowercase
  - Timestamp normalisé ISO 8601
  - Types numériques explicites (elapsed_time → float64)
  - Format de sortie : Parquet compressé snappy

Schedule : None → "@daily" pour automatiser à la suite de ingest_lineA_batch
"""

from __future__ import annotations

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datalake_datasets import RAW_LINEA

RAW_BUCKET = "raw"
STAGING_BUCKET = "staging"
LINE_PREFIX = "production_lines/line=lineA/"

EXPECTED_COLUMNS = {"timestamp", "temperature", "pressure", "elapsed_time", "label"}


@dag(
    dag_id="transform_lineA_batch",
    description="Transformation chunks journaliers LineA raw/ → staging/ Parquet",
    schedule=[RAW_LINEA],  # déclenché automatiquement après ingest_lineA_batch
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "staging", "batch", "lineA"],
)
def transform_lineA_batch():

    @task
    def list_raw_chunks() -> list[dict]:
        """
        Découvre les chunks LineA dans raw/ via list_objects_v2.
        Ne dépend pas du filesystem local : seul le contenu de MinIO fait foi.
        """
        from minio_utils import get_s3_client

        client = get_s3_client(role="etl")
        paginator = client.get_paginator("list_objects_v2")

        chunks = []
        for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=LINE_PREFIX):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv"):
                    chunks.append({"key": key, "size": obj["Size"]})

        if not chunks:
            raise ValueError(f"Aucun chunk CSV trouvé dans raw/{LINE_PREFIX}")

        print(f"[INFO] {len(chunks)} chunks trouvés dans raw/")
        for c in chunks:
            print(f"       {c['key']}  ({c['size']} bytes)")
        return chunks

    @task
    def transform_chunk(chunk: dict) -> dict:
        """
        Transforme un chunk CSV (raw/) en Parquet (staging/).
        Idempotent : skip si le Parquet existe déjà avec le même MD5.
        """
        import io
        import pandas as pd
        from minio_utils import (
            already_uploaded,
            compute_md5_bytes,
            get_object_as_bytes,
            get_s3_client,
        )

        raw_key: str = chunk["key"]
        # raw_key  ex: production_lines/line=lineA/year=2025/month=05/LineA_2025-05-01.csv
        # staging_key :  même chemin, extension .parquet
        staging_key = raw_key.replace(RAW_BUCKET, STAGING_BUCKET).rsplit(".", 1)[0] + ".parquet"
        # Correction : le bucket ne fait pas partie de la clé, on remplace juste l'extension
        staging_key = raw_key.rsplit(".", 1)[0] + ".parquet"

        client = get_s3_client(role="etl")
        raw_bytes = get_object_as_bytes(client, RAW_BUCKET, raw_key)

        df = pd.read_csv(io.BytesIO(raw_bytes))

        # ── Transformations ───────────────────────────────────────────────
        df.columns = [col.lower() for col in df.columns]

        for col in EXPECTED_COLUMNS - set(df.columns):
            df[col] = None

        df["timestamp"] = (
            pd.to_datetime(df["timestamp"], errors="raise")
            .dt.strftime("%Y-%m-%dT%H:%M:%S")
        )
        for col in ("temperature", "pressure"):
            df[col] = pd.to_numeric(df[col], errors="raise").astype("float64")
        df["elapsed_time"] = pd.to_numeric(df["elapsed_time"], errors="coerce").astype("float64")
        df["label"] = df["label"].astype("int8")

        # ── Sérialisation Parquet ─────────────────────────────────────────
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, compression="snappy", engine="pyarrow")
        parquet_bytes = buffer.getvalue()

        hex_md5, b64_md5 = compute_md5_bytes(parquet_bytes)

        if already_uploaded(client, STAGING_BUCKET, staging_key, hex_md5):
            print(f"[SKIP] {staging_key} déjà présent en staging.")
            return {"key": staging_key, "status": "skip", "rows": len(df)}

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
        print(f"[OK] {staging_key}  ({len(df)} lignes | MD5: {hex_md5})")
        return {"key": staging_key, "status": "transformed", "rows": len(df)}

    @task
    def summarize(results: list[dict]) -> None:
        transformed = [r for r in results if r["status"] == "transformed"]
        skipped = [r for r in results if r["status"] == "skip"]
        total_rows = sum(r["rows"] for r in results)

        print(f"\n{'─' * 50}")
        print(f"Bilan transform LineA batch")
        print(f"  Chunks transformés : {len(transformed)}")
        print(f"  Chunks skippés     : {len(skipped)}")
        print(f"  Total lignes       : {total_rows}")
        print(f"{'─' * 50}")

    # ── Pipeline ──────────────────────────────────────────────────────────
    chunks = list_raw_chunks()
    results = transform_chunk.expand(chunk=chunks)
    summarize(results)


transform_lineA_batch()
