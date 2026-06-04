"""
dags/dlt_pipelines/rawg_pipeline.py

Pipeline dlt — ingestion de l'API RAWG Video Games Database
Source  : https://api.rawg.io/api/games (REST paginée)
Dest    : Snowflake — schéma rawg_api, table games
Credentials : lus automatiquement depuis .dlt/secrets.toml
"""

import os
from typing import Iterator

import dlt
from dlt.sources.helpers import requests
from dotenv import load_dotenv

load_dotenv()

RAWG_API_BASE = "https://api.rawg.io/api/games"
RAWG_API_KEY = os.getenv("RAWG_API_KEY")
PAGE_SIZE = 40
MAX_PAGES = 250


def fetch_games_page(page: int) -> dict:
    """Récupère une page de jeux depuis l'API RAWG."""
    response = requests.get(
        RAWG_API_BASE,
        params={
            "key": RAWG_API_KEY,
            "page": page,
            "page_size": PAGE_SIZE,
            "ordering": "-rating",  # les mieux notés en premier
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


@dlt.resource(
    name="games",
    write_disposition="merge",
    primary_key="id",
)
def rawg_games() -> Iterator[dict]:
    """
    Générateur dlt — pagine l'API RAWG et yield chaque jeu.
    write_disposition='merge' : met à jour les jeux existants à chaque run.
    primary_key='id'          : dlt utilise l'id RAWG pour la déduplication.
    """
    page = 1

    while page <= MAX_PAGES:
        print(f"Page {page}/{MAX_PAGES}...")
        data = fetch_games_page(page)

        results = data.get("results", [])
        if not results:
            print("Plus de résultats, fin de la pagination.")
            break

        for game in results:
            yield {
                "id": game.get("id"),
                "name": game.get("name"),
                "slug": game.get("slug"),
                "released": game.get("released"),
                "rating": game.get("rating"),
                "rating_top": game.get("rating_top"),
                "ratings_count": game.get("ratings_count"),
                "metacritic": game.get("metacritic"),
                "playtime": game.get("playtime"),
                "updated": game.get("updated"),
                "esrb_rating": game.get("esrb_rating", {}).get("name")
                if game.get("esrb_rating")
                else None,
                # Genres : on garde uniquement les noms sous forme de liste
                "genres": [g["name"] for g in game.get("genres", [])],
                # Plateformes parentes : PC, PlayStation, Xbox...
                "platforms": [p["platform"]["name"] for p in game.get("parent_platforms", [])],
            }

        # Vérifie s'il y a une page suivante
        if not data.get("next"):
            print("Dernière page atteinte.")
            break

        page += 1


def run_pipeline() -> None:
    """Configure et exécute le pipeline dlt vers Snowflake."""

    pipeline = dlt.pipeline(
        pipeline_name="rawg_pipeline",
        destination="snowflake",
        dataset_name="rawg_api",
    )

    print("Démarrage de l'ingestion RAWG API...")

    load_info = pipeline.run(rawg_games())

    print(load_info)


if __name__ == "__main__":
    run_pipeline()
