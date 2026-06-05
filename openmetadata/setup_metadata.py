"""
Configuration des fiches métadonnées OpenMetadata pour chaque ligne de production.

Crée ou met à jour :
  1. L'équipe "Maintenance" (propriétaire des assets)
  2. Le service S3/MinIO dans OpenMetadata
  3. Un Container par ligne de production avec description, owner, source, fréquence

Usage :
    python openmetadata/setup_metadata.py
    python openmetadata/setup_metadata.py --om-url http://localhost:8585 --dry-run
"""

import argparse
import base64
import json
import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv()


# ── Schéma Silver unifié (staging/) ──────────────────────────────────────────
def build_silver_columns(elapsed_time_in_source: bool) -> list[dict]:
    """Retourne la liste des colonnes du schéma Silver pour un container."""
    elapsed_desc = (
        "Temps écoulé depuis le démarrage de la ligne (secondes)."
        if elapsed_time_in_source
        else "Temps écoulé depuis le démarrage (secondes). Absent dans la source → NULL en staging."
    )
    return [
        {
            "name": "timestamp",
            "dataType": "TIMESTAMP",
            "description": "Horodatage de la mesure (ISO 8601).",
        },
        {
            "name": "line_id",
            "dataType": "VARCHAR",
            "description": "Identifiant de la ligne de production (ex: lineA).",
        },
        {
            "name": "temperature",
            "dataType": "FLOAT",
            "description": "Température relevée par le capteur (°C).",
        },
        {
            "name": "pressure",
            "dataType": "FLOAT",
            "description": "Pression relevée par le capteur (bar).",
        },
        {"name": "elapsed_time", "dataType": "FLOAT", "description": elapsed_desc},
        {
            "name": "label",
            "dataType": "INT",
            "description": "0 = nominal, 1 = anomalie",
        },
        {
            "name": "source_file",
            "dataType": "VARCHAR",
            "description": "Nom du fichier CSV source ayant produit cet enregistrement.",
        },
        {
            "name": "ingested_at",
            "dataType": "TIMESTAMP",
            "description": "Horodatage d'ingestion en staging (UTC).",
        },
    ]


# ── Métadonnées par ligne de production ───────────────────────────────────────
LINES_METADATA = {
    "lineA": {
        "display_name": "Ligne de production A",
        "description": (
            "Ligne de production A — régime **stable**.\n\n"
            "| Champ | Valeur |\n|---|---|\n"
            "| Source | Capteurs industriels ligne A |\n"
            "| Fréquence de collecte | 1 mesure / minute |\n"
            "| Propriétaire | Responsable Maintenance |\n"
            "| Colonnes | timestamp, temperature, pressure, elapsed_time, label |\n"
            "| Particularité | Ingestion par chunks journaliers (10 000 mesures) |"
        ),
        "prefix": "production_lines/line=lineA/",
        "elapsed_time_in_source": True,
        "tags": ["LineA", "Stable"],
    },
    "lineB": {
        "display_name": "Ligne de production B",
        "description": (
            "Ligne de production B — régime **flux variable**.\n\n"
            "| Champ | Valeur |\n|---|---|\n"
            "| Source | Capteurs industriels ligne B |\n"
            "| Fréquence de collecte | 1 mesure / minute |\n"
            "| Propriétaire | Responsable Maintenance |\n"
            "| Colonnes | timestamp, temperature, pressure, elapsed_time, label |"
        ),
        "prefix": "production_lines/line=lineB/",
        "elapsed_time_in_source": True,
        "tags": ["LineB", "Flux"],
    },
    "lineC": {
        "display_name": "Ligne de production C",
        "description": (
            "Ligne de production C — régime **turbulent**.\n\n"
            "| Champ | Valeur |\n|---|---|\n"
            "| Source | Capteurs industriels ligne C |\n"
            "| Fréquence de collecte | 1 mesure / minute |\n"
            "| Propriétaire | Responsable Maintenance |\n"
            "| Colonnes | timestamp, temperature, pressure, label |\n"
            "| Particularité | elapsed_time absent → NULL en staging |"
        ),
        "prefix": "production_lines/line=lineC/",
        "elapsed_time_in_source": False,
        "tags": ["LineC", "Turbulent"],
    },
    "lineD": {
        "display_name": "Ligne de production D",
        "description": (
            "Ligne de production D — régime **contrôle de pics**.\n\n"
            "| Champ | Valeur |\n|---|---|\n"
            "| Source | Capteurs industriels ligne D |\n"
            "| Fréquence de collecte | 1 mesure / minute |\n"
            "| Propriétaire | Responsable Maintenance |\n"
            "| Colonnes | timestamp, temperature, pressure, label |\n"
            "| Particularité | elapsed_time absent → NULL en staging |"
        ),
        "prefix": "production_lines/line=lineD/",
        "elapsed_time_in_source": False,
        "tags": ["LineD", "SpikeControl"],
    },
    "lineE": {
        "display_name": "Ligne de production E",
        "description": (
            "Ligne de production E — régime **fluide stable**.\n\n"
            "| Champ | Valeur |\n|---|---|\n"
            "| Source | Capteurs industriels ligne E |\n"
            "| Fréquence de collecte | 1 mesure / minute |\n"
            "| Propriétaire | Responsable Maintenance |\n"
            "| Colonnes | timestamp, temperature, pressure, label |\n"
            "| Particularité | elapsed_time absent → NULL en staging |"
        ),
        "prefix": "production_lines/line=lineE/",
        "elapsed_time_in_source": False,
        "tags": ["LineE", "SmoothRun"],
    },
}


class OpenMetadataClient:
    def __init__(self, base_url: str, email: str, password: str, dry_run: bool = False):
        self.base_url = base_url.rstrip("/")
        self.dry_run = dry_run
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self._authenticate(email, password)

    def _authenticate(self, email: str, password: str):
        resp = self.session.post(
            f"{self.base_url}/api/v1/users/login",
            json={
                "email": email,
                "password": base64.b64encode(password.encode()).decode(),
            },
        )
        resp.raise_for_status()
        token = resp.json()["accessToken"]
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        print(f"[AUTH] Connecté à OpenMetadata ({self.base_url})")

    def _get(self, path: str, params: dict = None) -> dict | None:
        resp = self.session.get(f"{self.base_url}{path}", params=params)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def _put(self, path: str, payload: dict) -> dict:
        if self.dry_run:
            print(f"  [DRY-RUN] PUT {path}")
            print(
                f"            {json.dumps(payload, indent=2, ensure_ascii=False)[:200]}"
            )
            return {}
        resp = self.session.put(f"{self.base_url}{path}", json=payload)
        resp.raise_for_status()
        return resp.json()

    # ── Équipe Maintenance ────────────────────────────────────────────────────

    def ensure_team(self, name: str = "Maintenance") -> str:
        """Crée l'équipe si elle n'existe pas, retourne son id."""
        existing = self._get(f"/api/v1/teams/name/{name}")
        if existing:
            print(f"[TEAM] '{name}' déjà présente (id: {existing['id']})")
            return existing["id"]

        result = self._put(
            "/api/v1/teams",
            {
                "name": name,
                "displayName": "Équipe Maintenance",
                "description": "Responsables de la maintenance des lignes de production industrielles.",
                "teamType": "Group",  # Group = seul type pouvant posséder des entités dans OM
            },
        )
        team_id = result.get("id", "dry-run-id")
        print(f"[TEAM] '{name}' créée (id: {team_id})")
        return team_id

    # ── Service S3/MinIO ──────────────────────────────────────────────────────

    def ensure_storage_service(self, name: str = "minio-datalake") -> str:
        """Crée ou met à jour le service S3 MinIO, retourne son id."""
        payload = {
            "name": name,
            "displayName": "MinIO DataLake",
            "description": "Stockage objet S3-compatible hébergeant les couches raw/, staging/, curated/ et archive/ du DataLake industriel.",
            "serviceType": "S3",
            "connection": {
                "config": {
                    "type": "S3",
                    "awsConfig": {
                        "awsAccessKeyId": os.environ.get(
                            "MINIO_USER_OPENMETADATA", "openmetadata"
                        ),
                        "awsSecretAccessKey": os.environ.get(
                            "MINIO_PASS_OPENMETADATA", "openmetadata_secret123"
                        ),
                        "awsRegion": "us-east-1",
                        "endPointURL": "http://minio:9000",
                    },
                }
            },
        }
        result = self._put("/api/v1/services/storageServices", payload)
        svc_id = result.get("id", "dry-run-id")
        print(f"[SERVICE] '{name}' configuré (id: {svc_id})")
        return svc_id

    # ── Containers (une fiche par ligne) ─────────────────────────────────────

    def ensure_container(self, line_id: str, meta: dict, service_id: str, team_id: str):
        """Crée ou met à jour le container OpenMetadata pour une ligne."""
        payload = {
            "name": line_id,
            "displayName": meta["display_name"],
            "description": meta["description"],
            "service": "minio-datalake",  # FQN string, pas un objet
            "prefix": meta["prefix"],
            "owners": [{"id": team_id, "type": "team"}],
            "dataModel": {
                "isPartitioned": True,
                "columns": build_silver_columns(meta["elapsed_time_in_source"]),
            },
        }
        result = self._put("/api/v1/containers", payload)
        container_id = result.get("id", "dry-run-id")
        print(f"[CONTAINER] '{meta['display_name']}' configuré (id: {container_id})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--om-url", default="http://localhost:8585")
    parser.add_argument(
        "--om-email",
        default=os.environ.get("OM_ADMIN_EMAIL", "admin@open-metadata.org"),
    )
    parser.add_argument(
        "--om-password", default=os.environ.get("OM_ADMIN_PASSWORD", "admin")
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    client = OpenMetadataClient(
        args.om_url, args.om_email, args.om_password, args.dry_run
    )

    print("\n── Équipe ──────────────────────────────────────")
    team_id = client.ensure_team("Maintenance")

    print("\n── Service MinIO ───────────────────────────────")
    service_id = client.ensure_storage_service("minio-datalake")

    print("\n── Fiches métadonnées par ligne ────────────────")
    for line_id, meta in LINES_METADATA.items():
        client.ensure_container(line_id, meta, service_id, team_id)

    print("\nTerminé.")


if __name__ == "__main__":
    main()
