# DataLake MinIO — Capteurs industriels

DataLake basé sur MinIO pour l'ingestion et le stockage de données de capteurs industriels (5 lignes de production, 1 mesure/minute).

## Architecture en couches

```
raw/          → données brutes CSV, immuables, source de vérité
staging/      → données transformées en Parquet (colonnes harmonisées, timestamps normalisés)
curated/      → données certifiées, consommables par les analystes
archive/      → données archivées automatiquement après 180 jours (ILM)
```

Chaque bucket suit un partitionnement Hive-style :

```
<bucket>/production_lines/line=<lineX>/year=YYYY/month=MM/<fichier>
```

## Stack technique

| Composant | Version | Rôle |
|-----------|---------|------|
| MinIO | latest | Stockage objet S3-compatible |
| Apache Airflow | 2.9.3 | Orchestration ETL (LocalExecutor) |
| Apache Spark | 3.5.5 | Traitement distribué |
| PostgreSQL | 15 | Backend Airflow |
| OpenMetadata | 1.5.14 | Catalogue de données |
| MySQL | 8.0 | Backend OpenMetadata |
| Elasticsearch | 8.10.4 | Moteur de recherche OpenMetadata |

## Prérequis

- Docker + Docker Compose
- Python 3.12+ avec venv

## Démarrage rapide

```bash
# 1. Créer le fichier de configuration
cp .env.example .env   # puis éditer les mots de passe

# 2. Lancer la stack
docker compose up -d

# 3. Vérifier que tout est up
docker compose ps
```

### Accès aux interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | voir `.env` |
| Airflow | http://localhost:8080 | voir `.env` |
| Spark UI | http://localhost:8082 | — |
| OpenMetadata | http://localhost:8585 | admin / admin |

## IAM MinIO — Utilisateurs et droits

Voir [docs/droits_et_gouvernance.md](docs/droits_et_gouvernance.md) pour la matrice complète.

| Utilisateur | raw/ | staging/ | curated/ | archive/ |
|-------------|:----:|:--------:|:--------:|:--------:|
| `ingestion` | R/W | — | — | — |
| `etl` | R | R/W | R/W | — |
| `engineer` | R/W | R/W | R/W | — |
| `analyst` | — | — | R | — |
| `openmetadata` | R | R | R | R |

L'initialisation des buckets, policies et users est automatique au démarrage via `minio/init-minio.sh`.

## Ingestion des CSV

Les 5 fichiers CSV (`data/`) représentent les lignes de production LineA → LineE.

```bash
# Créer le venv et installer les dépendances
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt

# Vérifier les chemins cibles (sans uploader)
AWS_ENDPOINT_URL=http://localhost:9000 .venv/bin/python ingestion/upload_raw.py --dry-run

# Uploader vers raw/ avec validation MD5 côté serveur
AWS_ENDPOINT_URL=http://localhost:9000 .venv/bin/python ingestion/upload_raw.py

# Vérifier l'intégrité post-upload (ref / local / métadonnée MinIO / ETag)
AWS_ENDPOINT_URL=http://localhost:9000 .venv/bin/python ingestion/upload_raw.py --verify
```

Le script stocke le hash MD5 comme métadonnée de chaque objet MinIO. Les checksums de référence sont dans `checksums/`.

## DAGs Airflow

Les DAGs sont déclenchés manuellement (`schedule=None`), conçus pour passer en `@monthly`.

| DAG | Source | Destination | Description |
|-----|--------|-------------|-------------|
| `ingest_raw_csv` | `data/` (CSV) | `raw/` | Upload avec validation MD5, idempotent |
| `transform_staging` | `raw/` (CSV) | `staging/` (Parquet) | Harmonisation colonnes + timestamps |

### `ingest_raw_csv`
- 5 tâches upload en parallèle (une par ligne) + 1 tâche `verify_md5`
- Idempotent : skip si le fichier est déjà présent avec le même MD5
- Credentials : user `ingestion`

### `transform_staging`
- Lit les CSV depuis `raw/` via boto3
- Transformations : colonnes en lowercase, timestamp ISO 8601, types numériques explicites
- Colonnes absentes (`elapsed_time` sur LineC/D/E) → `NULL`
- Sortie : Parquet compressé snappy avec métadonnées (md5, source, rows)
- Credentials : user `etl`

## Structure du projet

```
.
├── airflow/
│   ├── config/airflow.cfg
│   ├── dags/
│   │   ├── dag_ingest_raw.py       # DAG ingestion CSV → raw/
│   │   └── dag_transform_staging.py # DAG transformation raw/ → staging/
│   ├── logs/
│   └── plugins/
│       └── minio_utils.py          # Utilitaires boto3 partagés entre DAGs
├── checksums/          # MD5 de référence des CSV sources
├── data/               # CSV bruts (gitignorés)
├── docs/
│   ├── brief_minIO_dataLake.drawio
│   ├── brief_minIO_dataLake_simple.drawio
│   └── droits_et_gouvernance.md
├── ingestion/
│   └── upload_raw.py   # Upload boto3 CLI + vérification MD5
├── minio/
│   ├── init-minio.sh   # Init buckets / policies / users
│   └── policies/       # JSON IAM par rôle
├── spark/
│   └── jobs/
├── docker-compose.yml
├── requirements.txt
└── .env                # Credentials (gitignored)
```

## Variables d'environnement

Copier `.env.example` en `.env` et adapter les valeurs. Ne jamais commiter `.env`.

```
MINIO_ROOT_USER / MINIO_ROOT_PASSWORD
MINIO_USER_INGESTION / MINIO_PASS_INGESTION
MINIO_USER_ETL / MINIO_PASS_ETL
MINIO_USER_ENGINEER / MINIO_PASS_ENGINEER
MINIO_USER_ANALYST / MINIO_PASS_ANALYST
MINIO_USER_OPENMETADATA / MINIO_PASS_OPENMETADATA
AWS_ENDPOINT_URL=http://minio:9000        # dans Docker
AWS_ENDPOINT_URL=http://localhost:9000    # depuis l'hôte (.env.local)
```
