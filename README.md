# DataLake MinIO — Capteurs industriels

DataLake basé sur MinIO pour l'ingestion et le stockage de données de capteurs industriels (5 lignes de production, 1 mesure/minute).

## Architecture en couches

```
raw/          → données brutes CSV, immuables, source de vérité
staging/      → données transformées en Parquet (colonnes harmonisées, timestamps normalisés)
curated/      → données certifiées, consommables par les analystes
archive/      → données archivées automatiquement après 180 jours (ILM)
```

Partitionnement Hive-style :

```
<bucket>/production_lines/line=<lineX>/year=YYYY/month=MM/<fichier>
```

LineA est ingérée en **chunks journaliers** (simulation de flux réel). Les autres lignes sont ingérées en fichier unique par run.

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

## Pipeline ETL — DAGs Airflow

### Vue d'ensemble

Les DAGs d'ingestion se déclenchent **manuellement** (ou peuvent être planifiés). Les DAGs de transformation s'enchaînent **automatiquement** via le mécanisme Airflow Datasets — dès qu'un DAG d'ingestion termine avec succès, son transform associé démarre sans intervention.

```
[Manuel]  ingest_raw_csv      ──► (Dataset RAW_MULTI) ──► transform_staging
[Manuel]  ingest_lineA_batch  ──► (Dataset RAW_LINEA) ──► transform_lineA_batch
```

L'onglet **Datasets** dans l'UI Airflow affiche le graphe de dépendances entre DAGs.

### Détail des DAGs

| DAG | Déclenchement | Source | Destination | Pattern |
|-----|--------------|--------|-------------|---------|
| `ingest_raw_csv` | Manuel | `data/` LineB→E | `raw/` CSV | 4 tâches statiques |
| `ingest_lineA_batch` | Manuel | `data/` LineA | `raw/` CSV/jour + manifest | Dynamic task mapping |
| `transform_staging` | Auto (Dataset) | `raw/` LineB→E | `staging/` Parquet | 4 tâches statiques |
| `transform_lineA_batch` | Auto (Dataset) | `raw/` LineA chunks | `staging/` Parquet/jour | Dynamic task mapping |

### `ingest_raw_csv`
- 4 tâches upload en parallèle (LineB, C, D, E) + `verify_md5`
- `verify_md5` déclare le Dataset `RAW_MULTI` → déclenche `transform_staging`
- Idempotent : skip si MD5 identique en MinIO
- Credentials : user `ingestion`

### `ingest_lineA_batch`
- Découpe LineA (~10 000 lignes) en chunks journaliers (~1 440 lignes/jour)
- Dynamic task mapping : une tâche `upload_chunk` par jour, créée à l'exécution
- `write_manifest` génère `raw/production_lines/line=lineA/LineA_manifest.json`
- `write_manifest` déclare le Dataset `RAW_LINEA` → déclenche `transform_lineA_batch`
- Credentials : user `ingestion`

### `transform_staging`
- Déclenché automatiquement après `ingest_raw_csv`
- Lit les CSV depuis `raw/` et écrit des Parquet dans `staging/`
- Transformations : colonnes en lowercase, timestamp ISO 8601, types explicites
- `elapsed_time` absent sur LineC/D/E → `NULL`
- Parquet compressé snappy, métadonnées : md5, source, rows
- Credentials : user `etl`

### `transform_lineA_batch`
- Déclenché automatiquement après `ingest_lineA_batch`
- Découvre les chunks dans `raw/` via `list_objects_v2` (pas de dépendance filesystem)
- Dynamic task mapping : une tâche `transform_chunk` par chunk trouvé
- Credentials : user `etl`

## Ingestion manuelle (CLI)

Pour uploader les CSV sans passer par Airflow :

```bash
# Créer le venv et installer les dépendances
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt

# Vérifier les chemins cibles (sans uploader)
AWS_ENDPOINT_URL=http://localhost:9000 .venv/bin/python ingestion/upload_raw.py --dry-run

# Uploader vers raw/ avec validation MD5 côté serveur
AWS_ENDPOINT_URL=http://localhost:9000 .venv/bin/python ingestion/upload_raw.py

# Vérifier l'intégrité post-upload
AWS_ENDPOINT_URL=http://localhost:9000 .venv/bin/python ingestion/upload_raw.py --verify
```

> Note : le CLI upload toutes les lignes (y compris LineA) en fichier unique. Pour le mode batch de LineA, utiliser le DAG `ingest_lineA_batch`.

## Structure du projet

```
.
├── airflow/
│   ├── config/airflow.cfg
│   ├── dags/
│   │   ├── dag_ingest_raw.py            # Ingestion LineB→E → raw/
│   │   ├── dag_ingest_lineA_batch.py    # Ingestion LineA par chunks → raw/
│   │   ├── dag_transform_staging.py     # Transform LineB→E raw/ → staging/
│   │   └── dag_transform_lineA_batch.py # Transform LineA chunks raw/ → staging/
│   ├── logs/
│   └── plugins/
│       ├── datalake_datasets.py         # Datasets Airflow partagés (RAW_LINEA, RAW_MULTI)
│       └── minio_utils.py              # Utilitaires boto3 partagés entre DAGs
├── checksums/          # MD5 de référence des CSV sources (versionnés)
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
