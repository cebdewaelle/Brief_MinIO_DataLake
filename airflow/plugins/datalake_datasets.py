"""
Datasets Airflow partagés entre les DAGs du DataLake.

Un Dataset représente une unité de données logique. Quand une tâche productrice
déclare `outlets=[DATASET]`, Airflow marque le dataset comme mis à jour.
Les DAGs consommateurs déclarant `schedule=[DATASET]` sont alors déclenchés
automatiquement — sans couplage direct entre les DAGs.

Visualisation dans l'UI Airflow : onglet "Datasets" → graphe de dépendances.
"""

from airflow.datasets import Dataset

# Chunks journaliers LineA dans raw/
RAW_LINEA = Dataset("s3://raw/production_lines/line=lineA/")

# Fichiers monolithiques LineB/C/D/E dans raw/
RAW_MULTI = Dataset("s3://raw/production_lines/lines=BCDE/")
