DBT_DIR     = dbt_videogames
PROFILES    = --profiles-dir .
TESTS_DIR   = tests/

# ── dbt ───────────────────────────────────────────────────────────────────────

dbt-build:
	cd $(DBT_DIR) && dbt build $(PROFILES)

dbt-build-rawg:
	cd $(DBT_DIR) && dbt build --select +fct_rawg_games $(PROFILES)

dbt-build-vgsales:
	cd $(DBT_DIR) && dbt build --select +fct_vgsales $(PROFILES)

dbt-test:
	cd $(DBT_DIR) && dbt test --select marts $(PROFILES)

dbt-docs:
	cd $(DBT_DIR) && dbt docs generate $(PROFILES) && dbt docs serve --port 8081

# ── Qualité du code ───────────────────────────────────────────────────────────

lint:
	uv run ruff check dags/ $(TESTS_DIR)

format:
	uv run ruff format dags/ $(TESTS_DIR)

# ── Tests unitaires ───────────────────────────────────────────────────────────

test:
	uv run pytest $(TESTS_DIR) -v --tb=short

# ── Pipeline complet ──────────────────────────────────────────────────────────

run-rawg:
	uv run python dags/dlt_pipelines/rawg_pipeline.py

run-vgsales:
	uv run python dags/dlt_pipelines/vgsales_pipeline.py

.PHONY: dbt-build dbt-build-rawg dbt-build-vgsales dbt-test dbt-docs lint format test run-rawg run-vgsales
