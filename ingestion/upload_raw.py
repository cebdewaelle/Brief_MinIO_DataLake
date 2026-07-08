"""
Upload des CSV de production vers raw/ dans MinIO avec vérification d'intégrité MD5.

Chemin cible : raw/production_lines/line=<lineX>/year=YYYY/month=MM/<fichier>.csv

Usage :
    python ingestion/upload_raw.py            # upload
    python ingestion/upload_raw.py --dry-run  # chemins sans upload
    python ingestion/upload_raw.py --verify   # vérification MD5 post-upload
"""

import argparse
import base64
import hashlib
import os
import re
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


def get_s3_client():
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        sys.exit("Erreur : AWS_ACCESS_KEY_ID et AWS_SECRET_ACCESS_KEY doivent être définis.")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def extract_line(filename):
    """LineA_Stable_10K.csv → lineA"""
    match = re.match(r"(Line[A-E])", filename, re.IGNORECASE)
    if not match:
        sys.exit(f"Erreur : impossible d'extraire le nom de ligne depuis '{filename}'.")
    return "line" + match.group(1)[-1].upper()


def extract_partition(csv_path):
    """Lit le premier timestamp du CSV → (year, month)"""
    with open(csv_path, encoding="utf-8") as f:
        f.readline()
        first_data = f.readline().strip()

    first_col = first_data.split(",")[0].strip().strip('"')
    match = re.match(r"(\d{4})-(\d{2})", first_col)
    if not match:
        sys.exit(f"Erreur : format de timestamp inattendu dans '{csv_path.name}' : '{first_col}'.")

    return match.group(1), match.group(2)


def compute_md5(path):
    """Retourne (hex, base64) du MD5 — hex pour stockage/comparaison, base64 pour Content-MD5."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest(), base64.b64encode(h.digest()).decode()


def read_md5_file(csv_path):
    """Cherche LineA.md5 dans checksums/ à la racine du projet — retourne le hash ou None."""
    match = re.match(r"(Line[A-E])", csv_path.name, re.IGNORECASE)
    if not match:
        return None
    project_root = Path(__file__).parent.parent
    md5_path = project_root / "checksums" / f"{match.group(1)}.md5"
    return md5_path.read_text().strip() if md5_path.exists() else None


def upload_file(client, bucket, key, csv_path):
    hex_md5, b64_md5 = compute_md5(csv_path)

    with open(csv_path, "rb") as f:
        data = f.read()

    try:
        # Content-MD5 : MinIO valide l'intégrité côté serveur pendant le transfert.
        # Metadata md5 : hash stocké sur l'objet, récupérable sans re-télécharger le fichier.
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType="text/csv",
            ContentMD5=b64_md5,
            Metadata={"md5": hex_md5},
        )
        print(f"  [OK] {csv_path.name} → s3://{bucket}/{key}")
        print(f"       MD5 : {hex_md5}")
    except ClientError as e:
        print(f"  [ERREUR] {csv_path.name} : {e}")


def verify_file(client, bucket, key, csv_path):
    local_hex, _ = compute_md5(csv_path)
    ref_hex = read_md5_file(csv_path)

    try:
        response = client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        print(f"  [ABSENT] {csv_path.name} : objet introuvable dans MinIO")
        return

    etag = response["ETag"].strip('"')
    stored_hex = response.get("Metadata", {}).get("md5", "")

    etag_ok = (etag == local_hex) if "-" not in etag else None  # multipart : non comparable
    meta_ok = stored_hex == local_hex
    ref_ok = (ref_hex is None) or (ref_hex == local_hex)

    all_ok = meta_ok and ref_ok and (etag_ok is not False)
    status = "[OK]" if all_ok else "[KO]"

    print(f"  {status} {csv_path.name}")
    print(f"       ref   : {ref_hex or '(aucun fichier .md5)'}")
    print(f"       local : {local_hex}")
    print(f"       meta  : {stored_hex or '(non stocké)'}")
    etag_label = etag if etag_ok is not None else f"{etag} (multipart, non comparable)"
    print(f"       etag  : {etag_label}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--bucket", default="raw")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    csv_files = sorted(data_dir.glob("*.csv"))

    if not csv_files:
        sys.exit(f"Aucun fichier CSV trouvé dans '{data_dir}'.")

    client = get_s3_client() if not args.dry_run else None

    print(f"Bucket : {args.bucket} | Fichiers : {len(csv_files)}\n")

    for csv_path in csv_files:
        line = extract_line(csv_path.name)
        year, month = extract_partition(csv_path)
        key = f"production_lines/line={line}/year={year}/month={month}/{csv_path.name}"

        if args.dry_run:
            print(f"  [DRY-RUN] {csv_path.name} → s3://{args.bucket}/{key}")
            continue

        if args.verify:
            verify_file(client, args.bucket, key, csv_path)
        else:
            upload_file(client, args.bucket, key, csv_path)

    print("\nTerminé.")


if __name__ == "__main__":
    main()
