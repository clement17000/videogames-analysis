"""
tests/test_rawg_pipeline.py

Tests unitaires de rawg_pipeline — aucun appel réseau réel.
Tous les appels à requests et à fetch_games_page sont mockés.
"""

from unittest.mock import MagicMock, patch

import pytest
from dlt_pipelines.rawg_pipeline import (
    MAX_PAGES,
    PAGE_SIZE,
    RAWG_API_BASE,
    fetch_games_page,
    rawg_games,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

GAME_FIXTURE = {
    "id": 3498,
    "name": "Grand Theft Auto V",
    "slug": "grand-theft-auto-v",
    "released": "2013-09-17",
    "rating": 4.47,
    "rating_top": 5,
    "ratings_count": 6149,
    "metacritic": 97,
    "playtime": 74,
    "updated": "2023-11-30T00:00:00Z",
    "esrb_rating": {"name": "Mature"},
    "genres": [{"name": "Action"}, {"name": "Adventure"}],
    "parent_platforms": [
        {"platform": {"name": "PC"}},
        {"platform": {"name": "PlayStation"}},
        {"platform": {"name": "Xbox"}},
    ],
}


def make_api_response(results: list, next_url: str | None = None) -> MagicMock:
    """Construit une réponse mock imitant l'API RAWG."""
    mock = MagicMock()
    mock.json.return_value = {"results": results, "next": next_url}
    return mock


# ---------------------------------------------------------------------------
# fetch_games_page
# ---------------------------------------------------------------------------


class TestFetchGamesPage:
    @patch("dlt_pipelines.rawg_pipeline.requests")
    def test_appelle_le_bon_endpoint(self, mock_requests):
        mock_requests.get.return_value = make_api_response([GAME_FIXTURE])

        fetch_games_page(page=1)

        url_appelée = mock_requests.get.call_args.args[0]
        assert url_appelée == RAWG_API_BASE

    @patch("dlt_pipelines.rawg_pipeline.requests")
    def test_envoie_les_bons_params(self, mock_requests):
        mock_requests.get.return_value = make_api_response([GAME_FIXTURE])

        fetch_games_page(page=5)

        params = mock_requests.get.call_args.kwargs["params"]
        assert params["page"] == 5
        assert params["page_size"] == PAGE_SIZE
        assert params["ordering"] == "-rating"

    @patch("dlt_pipelines.rawg_pipeline.requests")
    def test_retourne_le_json_de_reponse(self, mock_requests):
        mock_requests.get.return_value = make_api_response([GAME_FIXTURE])

        result = fetch_games_page(page=1)

        assert "results" in result
        assert result["results"][0]["id"] == GAME_FIXTURE["id"]

    @patch("dlt_pipelines.rawg_pipeline.requests")
    def test_leve_une_exception_sur_erreur_http(self, mock_requests):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("403 Forbidden")
        mock_requests.get.return_value = mock_response

        with pytest.raises(Exception, match="403"):
            fetch_games_page(page=1)


# ---------------------------------------------------------------------------
# rawg_games — conditions d'arrêt de la pagination
# ---------------------------------------------------------------------------


class TestRawgGamesPagination:
    @patch("dlt_pipelines.rawg_pipeline.fetch_games_page")
    def test_sarrête_quand_next_est_none(self, mock_fetch):
        mock_fetch.return_value = {"results": [GAME_FIXTURE], "next": None}

        list(rawg_games())

        assert mock_fetch.call_count == 1

    @patch("dlt_pipelines.rawg_pipeline.fetch_games_page")
    def test_sarrête_quand_results_est_vide(self, mock_fetch):
        mock_fetch.return_value = {"results": [], "next": "http://next-page"}

        games = list(rawg_games())

        assert games == []
        assert mock_fetch.call_count == 1

    @patch("dlt_pipelines.rawg_pipeline.fetch_games_page")
    def test_pagine_sur_plusieurs_pages(self, mock_fetch):
        mock_fetch.side_effect = [
            {"results": [GAME_FIXTURE], "next": "http://page2"},
            {"results": [GAME_FIXTURE], "next": "http://page3"},
            {"results": [GAME_FIXTURE], "next": None},
        ]

        games = list(rawg_games())

        assert len(games) == 3
        assert mock_fetch.call_count == 3

    @patch("dlt_pipelines.rawg_pipeline.fetch_games_page")
    def test_respecte_max_pages(self, mock_fetch):
        # Retourne toujours une page suivante pour déclencher le garde MAX_PAGES
        mock_fetch.return_value = {"results": [GAME_FIXTURE], "next": "http://next"}

        list(rawg_games())

        assert mock_fetch.call_count == MAX_PAGES


# ---------------------------------------------------------------------------
# rawg_games — extraction des champs imbriqués
# ---------------------------------------------------------------------------


class TestRawgGamesFieldExtraction:
    @patch("dlt_pipelines.rawg_pipeline.fetch_games_page")
    def test_extrait_le_nom_esrb(self, mock_fetch):
        mock_fetch.return_value = {"results": [GAME_FIXTURE], "next": None}

        game = next(iter(rawg_games()))

        assert game["esrb_rating"] == "Mature"

    @patch("dlt_pipelines.rawg_pipeline.fetch_games_page")
    def test_esrb_none_quand_absent(self, mock_fetch):
        game_sans_esrb = {**GAME_FIXTURE, "esrb_rating": None}
        mock_fetch.return_value = {"results": [game_sans_esrb], "next": None}

        game = next(iter(rawg_games()))

        assert game["esrb_rating"] is None

    @patch("dlt_pipelines.rawg_pipeline.fetch_games_page")
    def test_extrait_les_noms_de_genres(self, mock_fetch):
        mock_fetch.return_value = {"results": [GAME_FIXTURE], "next": None}

        game = next(iter(rawg_games()))

        assert game["genres"] == ["Action", "Adventure"]

    @patch("dlt_pipelines.rawg_pipeline.fetch_games_page")
    def test_genres_vide_quand_absent(self, mock_fetch):
        game_sans_genres = {**GAME_FIXTURE, "genres": []}
        mock_fetch.return_value = {"results": [game_sans_genres], "next": None}

        game = next(iter(rawg_games()))

        assert game["genres"] == []

    @patch("dlt_pipelines.rawg_pipeline.fetch_games_page")
    def test_extrait_les_noms_de_plateformes(self, mock_fetch):
        mock_fetch.return_value = {"results": [GAME_FIXTURE], "next": None}

        game = next(iter(rawg_games()))

        assert game["platforms"] == ["PC", "PlayStation", "Xbox"]

    @patch("dlt_pipelines.rawg_pipeline.fetch_games_page")
    def test_contient_tous_les_champs_attendus(self, mock_fetch):
        mock_fetch.return_value = {"results": [GAME_FIXTURE], "next": None}

        game = next(iter(rawg_games()))

        attendus = {
            "id",
            "name",
            "slug",
            "released",
            "rating",
            "rating_top",
            "ratings_count",
            "metacritic",
            "playtime",
            "updated",
            "esrb_rating",
            "genres",
            "platforms",
        }
        assert set(game.keys()) == attendus
