"""
DAG de transformation des chunks journaliers LineA : raw/ → staging/ (schéma Silver unifié).

Schéma de sortie :
  timestamp, line_id, temperature, pressure, elapsed_time, label, source_file, ingested_at

Découvre les chunks directement dans raw/ via list_objects_v2 — pas de dépendance filesystem.
Transformations via apply_silver_schema() (même logique que transform_staging).
Format : Parquet compressé gzip.
"""

from __future__ import annotations

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datalake_datasets import RAW_LINEA

RAW_BUCKET = "raw"
STAGING_BUCKET = "staging"
LINE_PREFIX = "production_lines/line=lineA/"


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
        staging_key = raw_key.rsplit(".", 1)[0] + ".parquet"
        source_file = raw_key.split("/")[-1]

        client = get_s3_client(role="etl")
        raw_bytes = get_object_as_bytes(client, RAW_BUCKET, raw_key)
        df = pd.read_csv(io.BytesIO(raw_bytes))

        # ── Schéma Silver unifié ──────────────────────────────────────────
        from minio_utils import apply_silver_schema
        df = apply_silver_schema(df, line_id="lineA", source_file=source_file)

        # ── Sérialisation Parquet gzip ────────────────────────────────────
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, compression="gzip", engine="pyarrow")
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
