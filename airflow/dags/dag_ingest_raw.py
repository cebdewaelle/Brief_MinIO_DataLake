"""
DAG d'ingestion des CSV de production vers raw/.

Une tâche par ligne (parallèle), puis une tâche de vérification MD5 globale.

Schedule : None (déclenchement manuel).
Pour passer en mensuel : remplacer schedule=None par schedule="@monthly"
"""

from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

BUCKET = "raw"
DATA_DIR = Path("/opt/airflow/data")
LINES = ["lineA", "lineB", "lineC", "lineD", "lineE"]

default_args = {"owner": "engineer", "retries": 1}


def find_csv(line_name: str) -> Path:
    letter = line_name[-1].upper()
    matches = list(DATA_DIR.glob(f"Line{letter}*.csv"))
    if not matches:
        raise FileNotFoundError(f"Aucun CSV trouvé pour {line_name} dans {DATA_DIR}")
    return matches[0]


def upload_line(line_name: str, **context):
    from minio_utils import already_uploaded, build_key, compute_md5, extract_partition, get_s3_client

    csv_path = find_csv(line_name)
    year, month = extract_partition(csv_path)
    key = build_key(line_name, year, month, csv_path.name)
    hex_md5, b64_md5 = compute_md5(csv_path)
    client = get_s3_client()

    if already_uploaded(client, BUCKET, key, hex_md5):
        print(f"[SKIP] {csv_path.name} déjà présent avec MD5 identique — aucune action.")
        return

    with open(csv_path, "rb") as f:
        data = f.read()

    client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=data,
        ContentType="text/csv",
        ContentMD5=b64_md5,
        Metadata={"md5": hex_md5},
    )
    print(f"[OK] {csv_path.name} → s3://{BUCKET}/{key}  MD5:{hex_md5}")


def verify_all(**context):
    from botocore.exceptions import ClientError
    from minio_utils import build_key, compute_md5, extract_partition, get_s3_client

    client = get_s3_client()
    errors = []

    for line_name in LINES:
        csv_path = find_csv(line_name)
        year, month = extract_partition(csv_path)
        key = build_key(line_name, year, month, csv_path.name)
        local_hex, _ = compute_md5(csv_path)

        try:
            resp = client.head_object(Bucket=BUCKET, Key=key)
            stored = resp.get("Metadata", {}).get("md5", "")
            if stored == local_hex:
                print(f"[OK] {csv_path.name} — MD5 {local_hex}")
            else:
                errors.append(f"{csv_path.name} : local={local_hex}  meta={stored}")
        except ClientError:
            errors.append(f"{csv_path.name} : absent de MinIO")

    if errors:
        raise ValueError("Échecs de vérification :\n" + "\n".join(errors))

    print("Tous les fichiers sont intègres.")


with DAG(
    dag_id="ingest_raw_csv",
    default_args=default_args,
    description="Ingestion mensuelle des CSV vers raw/ avec vérification MD5",
    schedule=None,  # passer à "@monthly" pour automatiser
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion", "raw", "minio"],
) as dag:

    upload_tasks = [
        PythonOperator(
            task_id=f"upload_{line}",
            python_callable=upload_line,
            op_kwargs={"line_name": line},
        )
        for line in LINES
    ]

    verify_task = PythonOperator(
        task_id="verify_md5",
        python_callable=verify_all,
    )

    upload_tasks >> verify_task
