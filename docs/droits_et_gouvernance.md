# Droits d'accès et gouvernance — DataLake MinIO

## 1. Matrice des droits d'accès

| Rôle | Bucket `raw/` | Bucket `staging/` | Bucket `curated/` | Bucket `archive/` |
|------|:---:|:---:|:---:|:---:|
| `ingestion` | R / W | — | — | — |
| `etl` | R | R / W | R / W | — |
| `engineer` | R / W | R / W | R / W | — |
| `analyst` | — | — | R | — |
| `openmetadata` | R | R | R | R |
| `minioadmin` *(root)* | R / W | R / W | R / W | R / W |

**Légende :** R = lecture (ListBucket + GetObject) · W = écriture (PutObject + DeleteObject) · — = aucun accès

### Détail par rôle

| Rôle | Périmètre | Justification |
|------|-----------|---------------|
| `ingestion` | Écriture `raw/` uniquement | Process d'ingestion isolé ; ne peut pas altérer les données traitées |
| `etl` | Lecture `raw/`, lecture/écriture `staging/` et `curated/` | Jobs Spark/Airflow qui transforment et promuvent les données |
| `engineer` | Lecture/écriture `raw/`, `staging/`, `curated/` | Développement, debug et correction de données ; accès `archive/` via root si besoin exceptionnel |
| `analyst` | Lecture `curated/` uniquement | Consommation des données certifiées ; pas d'accès aux données brutes |
| `openmetadata` | Lecture tous les buckets | Crawling du catalogue ; aucune modification de données |
| `minioadmin` | Accès total (root) | Administration système uniquement ; ne pas utiliser pour les traitements applicatifs |

---

## 2. Politique de gouvernance

### 2.1 Gestion des credentials

- Les mots de passe sont définis dans le fichier `.env` (exclu du dépôt git via `.gitignore`).
- Le fichier `.env` ne doit jamais être commité, partagé par mail, ni stocké en clair dans un gestionnaire de tickets.
- En production, remplacer les secrets par un gestionnaire de secrets (HashiCorp Vault, AWS Secrets Manager…).
- Le compte `minioadmin` (root) est réservé à l'administration MinIO ; les applications utilisent exclusivement les comptes applicatifs.

### 2.2 Principe du moindre privilège

- Chaque rôle dispose uniquement des droits nécessaires à son usage.
- L'accès au bucket `archive/` n'est accordé à aucun compte applicatif ; seul le root y accède pour les opérations de maintenance.
- Toute demande d'élargissement de droits doit être justifiée et tracée (PR + commentaire).

### 2.3 Cycle de vie des données (ILM)

| Règle | Déclencheur | Action |
|-------|-------------|--------|
| Archivage automatique | Objet dans `raw/` ou `staging/` > 180 jours | Déplacement vers `archive/` |
| Suppression automatique | Objet dans `archive/` > 730 jours (2 ans) | Suppression définitive |
| Rétention `curated/` | Indéfinie | Suppression manuelle après validation métier |

> Les règles ILM sont configurées dans MinIO via la console (Lifecycle Rules) ou `mc ilm rule add`.

### 2.4 Partitionnement des données

Les objets dans `raw/`, `staging/` et `curated/` suivent un partitionnement Hive-style :

```
<bucket>/year=YYYY/month=MM/line=<lineX>/<fichier>.csv
```

Exemple : `raw/year=2024/month=06/line=lineA/sensors_2024-06-03.csv`

Ce schéma permet un pruning de partitions natif avec Spark/Hive et facilite le filtrage temporel lors du crawling OpenMetadata.

### 2.5 Traçabilité et audit

- Chaque modification de policy ou de user MinIO doit faire l'objet d'un commit sur la branche `feature/` correspondante avec un message explicite.
- Les logs MinIO (accès, erreurs) sont accessibles via la console MinIO (port 9001) ou `mc admin logs`.
- OpenMetadata assure la traçabilité des métadonnées : propriétaire, description, fréquence de collecte et historique des modifications de schéma.

### 2.6 Propriété des données

| Ligne de production | Propriétaire métier | Fréquence de collecte |
|---------------------|---------------------|-----------------------|
| LineA | Responsable maintenance | 1 mesure / minute |
| LineB | Responsable maintenance | 1 mesure / minute |
| LineC | Responsable maintenance | 1 mesure / minute |
| LineD | Responsable maintenance | 1 mesure / minute |
| LineE | Responsable maintenance | 1 mesure / minute |

---

## 3. Fichiers de référence

| Fichier | Rôle |
|---------|------|
| `minio/policies/policy-ingestion.json` | Policy IAM — rôle ingestion |
| `minio/policies/policy-etl.json` | Policy IAM — rôle ETL |
| `minio/policies/policy-engineer.json` | Policy IAM — rôle engineer |
| `minio/policies/policy-analyst.json` | Policy IAM — rôle analyst |
| `minio/policies/policy-openmetadata.json` | Policy IAM — rôle OpenMetadata |
| `minio/init-minio.sh` | Script d'initialisation buckets / policies / users |
| `.env` | Credentials (gitignored) |
