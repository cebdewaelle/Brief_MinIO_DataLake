# DataLake MinIO — Capteurs industriels

DataLake basé sur MinIO pour l'ingestion et le stockage de données de capteurs industriels (5 lignes de production, 1 mesure/minute).

## Architecture en couches

```
raw/          → données brutes CSV, immuables, source de vérité
staging/      → données transformées en Parquet, schéma Silver unifié
curated/      → données certifiées, consommables par les analystes
archive/      → tier froid (minio-cold), alimenté automatiquement par ILM
```

Partitionnement Hive-style :

```
<bucket>/production_lines/line=<lineX>/year=YYYY/month=MM/<fichier>
```

LineA est ingérée en **chunks journaliers** (simulation de flux réel). Les autres lignes sont ingérées en fichier unique par run.

## Stack technique

| Composant | Version | Rôle |
|-----------|---------|------|
| MinIO (hot) | latest | Stockage objet S3-compatible — raw/, staging/, curated/ |
| MinIO (cold) | latest | Tier froid ILM — archive/ |
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

# 2. Lancer la stack complète
docker compose up -d

# 3. Vérifier que tout est up
docker compose ps
```

### Accès aux interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console (hot) | http://localhost:9001 | voir `.env` |
| MinIO Console (cold) | http://localhost:9003 | voir `.env` (`MINIO_COLD_*`) |
| Airflow | http://localhost:8080 | voir `.env` |
| Spark UI | http://localhost:8082 | — |
| OpenMetadata | http://localhost:8585 | admin / admin |

## Cycle de vie des données (ILM)

```
raw/ + staging/  ──(180 jours)──►  minio-cold/archive/  ──(730 jours)──►  suppression
```

- Après **180 jours** : les objets de `raw/` et `staging/` sont déplacés physiquement vers le MinIO cold (tier `COLDTIER`). Ils restent accessibles via le MinIO hot de manière transparente.
- Après **730 jours** sur le cold (910 jours au total) : suppression définitive.

Voir [docs/chiffrement_minio.md](docs/chiffrement_minio.md) pour les options de chiffrement SSE-S3 / SSE-KMS / SSE-C.

## IAM MinIO — Utilisateurs et droits

Voir [docs/droits_et_gouvernance.md](docs/droits_et_gouvernance.md) pour la matrice complète.

| Utilisateur | raw/ | staging/ | curated/ | archive/ |
|-------------|:----:|:--------:|:--------:|:--------:|
| `ingestion` | R/W | — | — | — |
| `etl` | R | R/W | R/W | — |
| `engineer` | R/W | R/W | R/W | — |
| `analyst` | — | — | R | — |
| `openmetadata` | R | R | R | R |

## Pipeline ETL — DAGs Airflow

### Vue d'ensemble

Les DAGs d'ingestion se déclenchent **manuellement**. Les DAGs de transformation s'enchaînent **automatiquement** via Airflow Datasets.

```
[Manuel]  ingest_raw_csv      ──► (Dataset RAW_MULTI) ──► transform_staging
[Manuel]  ingest_lineA_batch  ──► (Dataset RAW_LINEA) ──► transform_lineA_batch
```

### Détail des DAGs

| DAG | Déclenchement | Source | Destination | Pattern |
|-----|--------------|--------|-------------|---------|
| `ingest_raw_csv` | Manuel | `data/` LineB→E | `raw/` CSV | 4 tâches statiques |
| `ingest_lineA_batch` | Manuel | `data/` LineA | `raw/` CSV/jour + manifest | Dynamic task mapping |
| `transform_staging` | Auto (Dataset) | `raw/` LineB→E | `staging/` Parquet gzip | 4 tâches statiques |
| `transform_lineA_batch` | Auto (Dataset) | `raw/` LineA chunks | `staging/` Parquet gzip/jour | Dynamic task mapping |

### Schéma Silver unifié (staging/)

Toutes les lignes partagent le même schéma en staging :

| Colonne | Type | Note |
|---------|------|------|
| `timestamp` | TIMESTAMP | ISO 8601 |
| `line_id` | VARCHAR | ex: `lineA` |
| `temperature` | FLOAT | °C |
| `pressure` | FLOAT | bar |
| `elapsed_time` | FLOAT | NULL sur LineC/D/E |
| `label` | INT | 0 = nominal, 1 = anomalie |
| `source_file` | VARCHAR | fichier CSV source |
| `ingested_at` | TIMESTAMP | UTC |

## Catalogue OpenMetadata

Les fiches métadonnées sont créées via script :

```bash
.venv/bin/python openmetadata/setup_metadata.py
```

Crée pour chaque ligne (lineA→E) : service S3 MinIO, équipe Maintenance, description, schéma Silver, source et fréquence de collecte.

## Ingestion manuelle (CLI)

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt

AWS_ENDPOINT_URL=http://localhost:9000 .venv/bin/python ingestion/upload_raw.py --dry-run
AWS_ENDPOINT_URL=http://localhost:9000 .venv/bin/python ingestion/upload_raw.py
AWS_ENDPOINT_URL=http://localhost:9000 .venv/bin/python ingestion/upload_raw.py --verify
```

## Structure du projet

```
.
├── airflow/
│   ├── dags/
│   │   ├── dag_ingest_raw.py            # Ingestion LineB→E → raw/
│   │   ├── dag_ingest_lineA_batch.py    # Ingestion LineA par chunks → raw/
│   │   ├── dag_transform_staging.py     # Transform LineB→E raw/ → staging/
│   │   └── dag_transform_lineA_batch.py # Transform LineA chunks raw/ → staging/
│   └── plugins/
│       ├── datalake_datasets.py         # Datasets Airflow (RAW_LINEA, RAW_MULTI)
│       └── minio_utils.py              # Utilitaires boto3 + apply_silver_schema()
├── checksums/          # MD5 de référence des CSV sources
├── data/               # CSV bruts (gitignorés)
├── docs/               # Schémas, gouvernance, documentation
├── ingestion/
│   └── upload_raw.py   # Upload boto3 CLI + vérification MD5
├── minio/
│   ├── init-minio.sh       # Init hot MinIO : buckets, policies, users, ILM tier
│   ├── init-minio-cold.sh  # Init cold MinIO : bucket archive + ILM expiration 730j
│   └── policies/           # JSON IAM par rôle
├── openmetadata/
│   └── setup_metadata.py   # Création des fiches métadonnées via API
├── spark/
├── docker-compose.yml
├── requirements.txt
└── .env                # Credentials (gitignored)
```

## Variables d'environnement

```
MINIO_ROOT_USER / MINIO_ROOT_PASSWORD
MINIO_COLD_ROOT_USER / MINIO_COLD_ROOT_PASSWORD
MINIO_USER_INGESTION / MINIO_PASS_INGESTION
MINIO_USER_ETL / MINIO_PASS_ETL
MINIO_USER_ENGINEER / MINIO_PASS_ENGINEER
MINIO_USER_ANALYST / MINIO_PASS_ANALYST
MINIO_USER_OPENMETADATA / MINIO_PASS_OPENMETADATA
OM_ADMIN_EMAIL / OM_ADMIN_PASSWORD
AWS_ENDPOINT_URL=http://minio:9000        # dans Docker
AWS_ENDPOINT_URL=http://localhost:9000    # depuis l'hôte (.env.local)
```
