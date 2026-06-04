"""
tests/test_vgsales_pipeline.py

Tests unitaires de vgsales_pipeline — aucun appel Kaggle réel.
kagglehub.dataset_load est mocké pour retourner un DataFrame de test.
"""

from unittest.mock import patch

import pandas as pd
from dlt_pipelines.vgsales_pipeline import DATASET_HANDLE, FILE_NAME, load_vgsales

# Colonnes présentes dans le CSV Kaggle source (avant staging dbt)
COLONNES_SOURCE = [
    "NAME",
    "PLATFORM",
    "YEAR_OF_RELEASE",
    "GENRE",
    "PUBLISHER",
    "DEVELOPER",
    "RATING",
    "NA_SALES",
    "EU_SALES",
    "JP_SALES",
    "OTHER_SALES",
    "GLOBAL_SALES",
    "CRITIC_SCORE",
    "CRITIC_COUNT",
    "USER_SCORE",
    "USER_COUNT",
]


def make_sample_df(n_rows: int = 5) -> pd.DataFrame:
    """Construit un DataFrame minimal imitant le CSV vgsales."""
    return pd.DataFrame(
        {
            "NAME": ["Game A"] * n_rows,
            "PLATFORM": ["PS3"] * n_rows,
            "YEAR_OF_RELEASE": [2010] * n_rows,
            "GENRE": ["Action"] * n_rows,
            "PUBLISHER": ["Publisher X"] * n_rows,
            "DEVELOPER": ["Dev Y"] * n_rows,
            "RATING": ["M"] * n_rows,
            "NA_SALES": [1.0] * n_rows,
            "EU_SALES": [0.5] * n_rows,
            "JP_SALES": [0.2] * n_rows,
            "OTHER_SALES": [0.1] * n_rows,
            "GLOBAL_SALES": [1.8] * n_rows,
            "CRITIC_SCORE": [80] * n_rows,
            "CRITIC_COUNT": [50] * n_rows,
            "USER_SCORE": [7.5] * n_rows,
            "USER_COUNT": [200] * n_rows,
        }
    )


class TestLoadVgsales:
    @patch("dlt_pipelines.vgsales_pipeline.kagglehub.dataset_load")
    def test_retourne_un_dataframe(self, mock_load):
        mock_load.return_value = make_sample_df()

        result = load_vgsales()

        assert isinstance(result, pd.DataFrame)

    @patch("dlt_pipelines.vgsales_pipeline.kagglehub.dataset_load")
    def test_appelle_le_bon_dataset(self, mock_load):
        mock_load.return_value = make_sample_df()

        load_vgsales()

        args = str(mock_load.call_args)
        assert DATASET_HANDLE in args

    @patch("dlt_pipelines.vgsales_pipeline.kagglehub.dataset_load")
    def test_appelle_le_bon_fichier(self, mock_load):
        mock_load.return_value = make_sample_df()

        load_vgsales()

        args = str(mock_load.call_args)
        assert FILE_NAME in args

    @patch("dlt_pipelines.vgsales_pipeline.kagglehub.dataset_load")
    def test_colonnes_source_presentes(self, mock_load):
        mock_load.return_value = make_sample_df()

        df = load_vgsales()

        manquantes = [c for c in COLONNES_SOURCE if c not in df.columns]
        assert manquantes == [], f"Colonnes manquantes : {manquantes}"

    @patch("dlt_pipelines.vgsales_pipeline.kagglehub.dataset_load")
    def test_dataframe_non_vide(self, mock_load):
        mock_load.return_value = make_sample_df(n_rows=3)

        df = load_vgsales()

        assert len(df) == 3
