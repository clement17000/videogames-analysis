"""
dags/dlt_pipelines/vgsales_pipeline.py

Pipeline dlt — ingestion du dataset Video Games Sales (Kaggle)
Source  : kagglehub (ibriiee/video-games-sales-dataset-2022-updated-extra-feat)
Dest    : Snowflake — schéma vgsales_csv, table vgsales
Credentials : lus automatiquement depuis .dlt/secrets.toml
"""

import dlt
import kagglehub
import pandas as pd
from dlt.common import logger
from kagglehub import KaggleDatasetAdapter

logger.info("Démarrage de l'ingestion du dataset Video Games Sales...")

DATASET_HANDLE = "ibriiee/video-games-sales-dataset-2022-updated-extra-feat"
FILE_NAME = "Video_Games.csv"
TABLE_NAME = "vgsales"


def load_vgsales() -> pd.DataFrame:
    """Télécharge le dataset Kaggle et retourne un DataFrame pandas."""
    logger.info(f"Chargement du dataset Kaggle : {DATASET_HANDLE}")
    df = kagglehub.dataset_load(
        KaggleDatasetAdapter.PANDAS,
        DATASET_HANDLE,
        path=FILE_NAME,
    )
    logger.info(f"Dataset chargé — {df.shape[0]} lignes, {df.shape[1]} colonnes")
    return df


def run_pipeline() -> None:
    """Configure et exécute le pipeline dlt vers Snowflake.

    write_disposition='replace' (et non 'merge') car le CSV Kaggle est un
    snapshot complet — on recharge la table entière à chaque run pour éviter
    d'accumuler des doublons sur un dataset sans clé naturelle fiable.
    """
    pipeline = dlt.pipeline(
        pipeline_name="vgsales_pipeline",
        destination="snowflake",
        dataset_name="vgsales_csv",
    )

    df = load_vgsales()

    load_info = pipeline.run(
        df,
        table_name=TABLE_NAME,
        write_disposition="replace",
    )

    logger.info(f"Chargement terminé — {load_info} lignes insérées")


if __name__ == "__main__":
    run_pipeline()
