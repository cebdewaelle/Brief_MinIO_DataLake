"""
Utilitaires MinIO partagés entre les DAGs.
Importable directement car airflow/plugins/ est dans le PYTHONPATH Airflow.
"""

import base64
import hashlib
import io
import os
import re
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def get_s3_client(role: str = "ingestion"):
    """Client boto3 pour un rôle donné (ingestion, etl, analyst…)."""
    r = role.upper()
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.environ.get(f"MINIO_USER_{r}"),
        aws_secret_access_key=os.environ.get(f"MINIO_PASS_{r}"),
        region_name="us-east-1",
    )


def compute_md5(path: Path) -> tuple[str, str]:
    """Retourne (hex, base64) — hex pour comparaison, base64 pour Content-MD5."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest(), base64.b64encode(h.digest()).decode()


def compute_md5_bytes(data: bytes) -> tuple[str, str]:
    """Même chose depuis un buffer en mémoire (pour Parquet généré à la volée)."""
    h = hashlib.md5(data)
    return h.hexdigest(), base64.b64encode(h.digest()).decode()


def extract_partition(csv_path: Path) -> tuple[str, str]:
    """Lit le premier timestamp du CSV → (year, month)."""
    with open(csv_path, encoding="utf-8") as f:
        f.readline()
        first_data = f.readline().strip()
    first_col = first_data.split(",")[0].strip().strip('"')
    match = re.match(r"(\d{4})-(\d{2})", first_col)
    if not match:
        raise ValueError(f"Format timestamp inattendu dans '{csv_path.name}' : '{first_col}'")
    return match.group(1), match.group(2)


def build_key(line: str, year: str, month: str, filename: str) -> str:
    return f"production_lines/line={line}/year={year}/month={month}/{filename}"


def already_uploaded(client, bucket: str, key: str, hex_md5: str) -> bool:
    """Vérifie si l'objet existe déjà dans MinIO avec le même MD5 (idempotence)."""
    try:
        resp = client.head_object(Bucket=bucket, Key=key)
        return resp.get("Metadata", {}).get("md5", "") == hex_md5
    except ClientError:
        return False


def get_object_as_bytes(client, bucket: str, key: str) -> bytes:
    resp = client.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()
