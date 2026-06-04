"""
DAG d'ingestion de LineA par chunks journaliers — simulation d'un flux réel.

LineA_Stable_10K.csv contient ~10 000 mesures (1/minute) soit ~7 jours de données.
Chaque jour est uploadé comme un fichier CSV indépendant dans raw/, permettant de
simuler l'arrivée progressive des données plutôt qu'un upload monolithique.

Patterns Airflow utilisés :
  - TaskFlow API (@task) : plus lisible que PythonOperator, gestion XCom automatique
  - Dynamic task mapping (.expand()) : une tâche par chunk créée à l'exécution

Chemin cible :
  raw/production_lines/line=lineA/year=YYYY/month=MM/LineA_YYYY-MM-DD.csv

Schedule : None (déclenchement manuel).
Pour automatiser : schedule="@daily" + filtrage via context["ds"]
"""

from __future__ import annotations

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datalake_datasets import RAW_LINEA

DATA_DIR = "/opt/airflow/data"
BUCKET = "raw"
LINE = "lineA"


@dag(
    dag_id="ingest_lineA_batch",
    description="Ingestion LineA par chunks journaliers avec dynamic task mapping",
    schedule=None,  # → "@daily" pour automatiser
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion", "raw", "batch", "lineA"],
)
def ingest_lineA_batch():

    @task
    def get_daily_chunks() -> list[dict]:
        """
        Lit LineA et retourne la liste des jours présents avec leur row count.
        Ce retour pilote le dynamic task mapping : autant de tâches upload
        que de jours distincts dans le fichier.
        """
        import pandas as pd
        from pathlib import Path

        path = next(Path(DATA_DIR).glob("LineA*.csv"))
        df = pd.read_csv(path, parse_dates=["timestamp"])
        df["date"] = df["timestamp"].dt.date.astype(str)

        chunks = [
            {"date": date, "rows": int(len(group))}
            for date, group in df.groupby("date")
        ]
        print(f"[INFO] {len(chunks)} chunks détectés pour {path.name}")
        for c in chunks:
            print(f"       {c['date']} → {c['rows']} lignes")
        return chunks

    @task
    def upload_chunk(chunk: dict) -> dict:
        """
        Upload un chunk journalier vers raw/.
        Idempotent : skip si le fichier existe déjà avec le même MD5.
        Retourne un résumé transmis via XCom à la tâche summarize.
        """
        import pandas as pd
        from pathlib import Path
        from minio_utils import already_uploaded, build_key, compute_md5_bytes, get_s3_client

        date: str = chunk["date"]  # "2025-05-01"
        year, month, _ = date.split("-")

        path = next(Path(DATA_DIR).glob("LineA*.csv"))
        df = pd.read_csv(path, parse_dates=["timestamp"])
        df_day = df[df["timestamp"].dt.date.astype(str) == date]

        filename = f"LineA_{date}.csv"
        key = build_key(LINE, year, month, filename)

        csv_bytes = df_day.to_csv(index=False).encode("utf-8")
        hex_md5, b64_md5 = compute_md5_bytes(csv_bytes)
        client = get_s3_client(role="ingestion")

        if already_uploaded(client, BUCKET, key, hex_md5):
            print(f"[SKIP] {filename} déjà présent avec MD5 identique.")
            return {"date": date, "status": "skip", "rows": len(df_day), "md5": hex_md5}

        client.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=csv_bytes,
            ContentType="text/csv",
            ContentMD5=b64_md5,
            Metadata={
                "md5": hex_md5,
                "rows": str(len(df_day)),
                "source_date": date,
                "source_file": path.name,
            },
        )
        print(f"[OK] {filename} → s3://{BUCKET}/{key}  ({len(df_day)} lignes | MD5: {hex_md5})")
        return {"date": date, "status": "uploaded", "rows": len(df_day), "md5": hex_md5}

    @task(outlets=[RAW_LINEA])
    def write_manifest(results: list[dict]) -> None:
        """
        Écrit un manifest JSON dans raw/ après tous les uploads.

        Stocké à la racine de la partition line=lineA/ (hors year=/month=/) car
        il couvre potentiellement plusieurs mois.

        Clé : raw/production_lines/line=lineA/LineA_manifest.json

        Structure :
        {
          "line": "lineA",
          "generated_at": "...",
          "total_rows": 10000,
          "total_chunks": 7,
          "chunks": [
            {"date": "2025-05-01", "key": "...", "md5": "...", "rows": 1440},
            ...
          ]
        }
        """
        import json
        from datetime import datetime, timezone
        from minio_utils import build_key, get_s3_client

        client = get_s3_client(role="ingestion")

        # Reconstitution des clés depuis les résultats des upload_chunk
        chunks_meta = []
        for r in sorted(results, key=lambda x: x["date"]):
            year, month, _ = r["date"].split("-")
            filename = f"LineA_{r['date']}.csv"
            chunks_meta.append({
                "date": r["date"],
                "filename": filename,
                "key": build_key(LINE, year, month, filename),
                "md5": r["md5"],
                "rows": r["rows"],
                "status": r["status"],
            })

        manifest = {
            "line": LINE,
            "source_file": "LineA_Stable_10K.csv",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_chunks": len(chunks_meta),
            "total_rows": sum(r["rows"] for r in results),
            "chunks": chunks_meta,
        }

        manifest_key = f"production_lines/line={LINE}/LineA_manifest.json"
        manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")

        client.put_object(
            Bucket=BUCKET,
            Key=manifest_key,
            Body=manifest_bytes,
            ContentType="application/json",
        )

        uploaded = sum(1 for r in results if r["status"] == "uploaded")
        skipped  = sum(1 for r in results if r["status"] == "skip")
        print(f"\n{'─' * 50}")
        print(f"Manifest écrit : s3://{BUCKET}/{manifest_key}")
        print(f"  Chunks uploadés : {uploaded}")
        print(f"  Chunks skippés  : {skipped}")
        print(f"  Total lignes    : {manifest['total_rows']}")
        print(f"{'─' * 50}")

    # ── Pipeline ──────────────────────────────────────────────────────────
    # get_daily_chunks() retourne une liste → .expand() crée une tâche par élément
    chunks = get_daily_chunks()
    results = upload_chunk.expand(chunk=chunks)
    write_manifest(results)


ingest_lineA_batch()
