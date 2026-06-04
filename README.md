# 🎮 VIDEOGAMES ANALYSIS

🇬🇧 English | 🇫🇷 [Français](README.fr.md)

> **An end-to-end data engineering project that collects, transforms and visualises video game data — genre trends, platform dominance, score/sales correlation and regional preferences.**

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker%20Compose-2496ED?style=for-the-badge&logo=Docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![CI](https://github.com/clement17000/videogames-analysis/actions/workflows/ci-cd.yml/badge.svg)

---

## 📋 Table of Contents

1. [Why this project exists](#-why-this-project-exists)
2. [The Modern Data Stack](#-the-modern-data-stack)
3. [Architecture](#-architecture)
4. [Data Sources](#-data-sources)
5. [Tech Stack](#-tech-stack)
6. [Project Structure](#-project-structure)
7. [dbt — Transformations](#️-dbt--transformations)
8. [Snowflake Setup](#️-snowflake-setup)
9. [Data Visualisation](#-data-visualisation)
10. [Tests](#-tests)
11. [Quick Start](#-quick-start)

---

## 🎯 Why this project exists

This project was built as a **personal data engineering portfolio** — a modern data platform built end-to-end on real public sources (REST API + CSV), not pre-cleaned sample datasets.

The goal was deliberate: design, build and operate a **production-grade pipeline** from scratch — real orchestration, schema evolution handling, data quality contracts and a layered transformation model.

Every architectural decision reflects how a data engineering team would approach this problem in a real company.

---

## 🛠 The Modern Data Stack

- **🚀 Apache Airflow 3 (Orchestration)** — Orchestrates the extraction → transformation sequence for each source. Two independent DAGs, triggered manually, built with the Airflow Task SDK (`@dag` / `@task` decorators).

- **📥 dlt — dlthub (Ingestion)** — Handles extraction and loading (EL) for both sources. Automatically infers schemas from JSON responses, normalises nested structures and deduplicates records on each run via primary key (`merge` for the RAWG API, `replace` for the Kaggle CSV).

- **🏛️ dbt Core (Transformation)** — Transforms raw data into analysis-ready tables through a Silver → Gold architecture. Each layer is tested (unique, not_null) and documented.

- **❄️ Snowflake (Data Warehouse)** — Cloud data warehouse. Raw schemas (`rawg_api`, `vgsales_csv`) are managed by dlt; transformed schemas (`staging`, `marts`) are managed by dbt.

- **📊 Metabase (Visualisation)** — Connects directly to Gold tables in Snowflake. All dashboards are built on pre-computed marts — no SQL is written on the BI side.

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                           │
│                                                                 │
│   RAWG API (paginated REST)    VGSales CSV (Kaggle)             │
│   500k+ games, sorted by rating  ~16k games, sales by region   │
└────────────┬───────────────────────┬────────────────────────────┘
             │                       │
             ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                       INGESTION — dlt                           │
│                                                                 │
│   Pagination + merge           CSV load + replace               │
│   (primary key: RAWG id)       (full snapshot)                  │
└────────────┬───────────────────────┬────────────────────────────┘
             │                       │
             ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE — RAW Layer                        │
│                                                                 │
│   DW_VIDEOGAMES.RAWG_API            DW_VIDEOGAMES.VGSALES_CSV   │
│   └── games                         └── vgsales                 │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│              SNOWFLAKE — dbt Layer (Silver → Gold)              │
│                                                                 │
│  ⚪ Silver   stg__rawg_games        stg__vgsales                 │
│  (views)    cleaning, typing, renaming, outliers                │
│                          │                                      │
│  🟡 Gold    Dimensions: dim_platform  dim_genre  dim_publisher   │
│  (tables)   Facts:      fct_rawg_games                          │
│                          fct_vgsales                            │
│                          fct_games_enriched  ← cross-source join│
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                  VISUALISATION — Metabase                       │
│                                                                 │
│   Connected directly to Gold tables in Snowflake                │
└─────────────────────────────────────────────────────────────────┘

Orchestration: Apache Airflow 3 (two independent DAGs)
```

### 🏅 dbt Layers

| Layer | Prefix | Materialisation | Role |
|-------|--------|-----------------|------|
| ⚪ **Silver** | `stg__*` | View | Cleaning, typing, renaming, deduplication. No business logic. |
| 🟡 **Gold — Dimensions** | `dim_*` | Table | Reference tables (platform, genre, publisher) enriched via seeds. |
| 🟡 **Gold — Facts** | `fct_*` | Table | Analysis-ready metrics, joined with dimensions. |
| 🟡 **Gold — Facts** | `fct_rawg_games` | Incremental | Only processes new/updated games since last run (watermark: `updated_at`). |

### 🔁 DAG Structure

Both DAGs share the same simple two-task structure:

```
trigger_dlt_*       (extraction + Snowflake load via dlt)
       ↓
run_dbt             (dbt build --select +<target_model>)
```

| DAG | dbt selection | Trigger |
|-----|---------------|---------|
| `elt_rawg_api_dag` | `+fct_rawg_games` | Manual |
| `elt_vgsales_csv_dag` | `+fct_vgsales` | Manual |

---

## 📊 Data Sources

| Source | Type | Volume | Description |
|--------|------|--------|-------------|
| **RAWG Video Games Database** | REST API | 500k+ games | Genres, platforms, release dates, Metacritic scores, user ratings, average playtime |
| **Video Game Sales (Kaggle)** | CSV | ~16k games | Sales by region (NA, EU, JP), platform, publisher, developer, critic score — up to 2016 |

---

## 💻 Tech Stack

| Layer | Tool | Version |
|-------|------|---------|
| Orchestration | Apache Airflow | `3.0` |
| Ingestion | dlt (dlthub) | `≥ 1.26` |
| Data Warehouse | Snowflake | — |
| Transformations | dbt Core | `1.9` |
| dbt package | dbt_utils | `≥ 1.0` |
| Visualisation | Metabase | latest |
| Tests | pytest | `≥ 9.0` |
| Linting / Formatting | ruff | `≥ 0.11` |
| Package manager | uv | latest |
| Git hooks | pre-commit | `≥ 4.0` |
| Language | Python | `≥ 3.12` |
| Infrastructure | Docker Compose | — |

---

## 📁 Project Structure

```
videogames-analysis/
├── dags/
│   ├── dlt_pipelines/
│   │   ├── rawg_pipeline.py           # RAWG API ingestion via dlt (merge)
│   │   └── vgsales_pipeline.py        # Kaggle CSV ingestion via dlt (replace)
│   ├── elt_rawg_api.py                # Airflow DAG — RAWG API source
│   └── elt_vgsales.py                 # Airflow DAG — VGSales CSV source
├── dbt_videogames/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml                   # Snowflake source declarations
│   │   │   ├── rawg/
│   │   │   │   ├── stg__rawg_games.sql       # ⚪ Silver — RAWG API
│   │   │   │   └── stg__rawg_games.yml       # Tests & documentation
│   │   │   └── vgsales/
│   │   │       ├── stg__vgsales.sql          # ⚪ Silver — VGSales CSV
│   │   │       └── stg__vgsales.yml          # Tests & documentation
│   │   └── marts/
│   │       ├── exposures.yml                 # Metabase declared as downstream consumer
│   │       ├── dimensions/
│   │       │   ├── _dims.yml                 # Column docs & tests for all dims
│   │       │   ├── dim_genre.sql             # 🟡 Distinct genres
│   │       │   ├── dim_platform.sql          # 🟡 Platforms + manufacturer (seed)
│   │       │   └── dim_publisher.sql         # 🟡 Distinct publishers
│   │       └── facts/
│   │           ├── _facts.yml                # Column docs & tests for all facts
│   │           ├── fct_rawg_games.sql        # 🟡 RAWG metrics — incremental
│   │           ├── fct_vgsales.sql           # 🟡 Sales per game + platform
│   │           └── fct_games_enriched.sql    # 🟡 Cross-source join sales ↔ RAWG scores
│   ├── seeds/
│   │   ├── _seeds.yml                        # Seed documentation & tests
│   │   └── platform_mapping.csv             # Code → full name + manufacturer
│   ├── dbt_project.yml
│   └── profiles.yml
├── tests/
│   ├── conftest.py                    # pytest configuration (sys.path)
│   ├── test_rawg_pipeline.py          # 14 unit tests (no network calls)
│   └── test_vgsales_pipeline.py       # 5 unit tests (no Kaggle calls)
├── setup/
│   └── snowflake_setup.sql            # Warehouse, DB, schemas, roles, grants
├── .github/
│   └── workflows/
│       └── ci-cd.yml                  # CI: ruff lint → pytest (triggered on push/PR)
├── docker-compose.yaml                # Airflow 3 + PostgreSQL + Metabase
├── Dockerfile                         # Custom Airflow image with dlt, dbt, kagglehub
├── Makefile                           # Shortcuts: make dbt-build, make test, etc.
├── .pre-commit-config.yaml            # ruff runs automatically before each commit
├── .env.example                       # Environment variables template
├── pyproject.toml
└── requirements.txt
```

---

## 🏛️ dbt — Transformations

### Silver Models (views)

| Model | Source | Key transformations |
|-------|--------|---------------------|
| `stg__rawg_games` | `rawg_api.games` | Surrogate key, metacritic cleaning (< 0 or > 100 → NULL), playtime = 0 filter, outlier > 8760h → NULL |
| `stg__vgsales` | `vgsales_csv.vgsales` | Deduplication (best row per game+platform), surrogate keys, type casting |

### Gold Models (tables)

| Model | Grain | Description |
|-------|-------|-------------|
| `dim_platform` | Platform | Code + full name + manufacturer (via `platform_mapping` seed) |
| `dim_genre` | Genre | Distinct genres from vgsales |
| `dim_publisher` | Publisher | Distinct publishers from vgsales |
| `fct_rawg_games` | Game (RAWG) | Scores and engagement: Metacritic, user_rating, playtime |
| `fct_vgsales` | Game × Platform | NA/EU/JP sales + critic scores, joined with dimensions |
| `fct_games_enriched` | Game × Platform | Cross-source join vgsales ↔ RAWG on normalised game name (games ≤ 2016) |

### Exposures

Metabase is declared as a downstream consumer of all Gold models via `exposures.yml`.
This makes it visible as a terminal node in the dbt lineage graph, showing the full
data chain from raw sources to the BI dashboard.

### Main Commands

```bash
cd dbt_videogames

dbt deps                                        # Install packages (dbt_utils)
dbt build --select +fct_vgsales --profiles-dir .   # Full vgsales pipeline
dbt build --select +fct_rawg_games --profiles-dir . # Full RAWG pipeline
dbt build --profiles-dir .                      # All models

dbt test --select marts --profiles-dir .        # Run data quality tests (unique, not_null, relationships)

dbt docs generate --profiles-dir .             # Generate data catalogue
dbt docs serve --port 8081                      # Serve at http://localhost:8081
```

---

## ❄️ Snowflake Setup

### Schema Organisation

```
DW_VIDEOGAMES
├── RAWG_API                    ← Raw data from dlt (RAWG API)
│   └── games
├── VGSALES_CSV                 ← Raw data from dlt (Kaggle CSV)
│   └── vgsales
├── DBT_VIDEOGAMES_STAGING      ← Silver views from dbt (stg__*)
└── DBT_VIDEOGAMES_MARTS        ← Gold tables from dbt (dim_*, fct_*)
```

### Roles

| Role | Usage | Service account |
|------|-------|-----------------|
| `LOADER_ROLE` | Read/write on RAW schemas | `LOADER_USER` (dlt / Airflow) |
| `TRANSFORMER_ROLE` | Read RAW + read/write dbt | `TRANSFORMER_USER` (dbt) |
| `REPORTER_ROLE` | Read-only on Gold tables | `REPORTER_USER` (Metabase) |

The `setup/snowflake_setup.sql` script creates all these resources (warehouse, database, schemas, roles, grants).

### dbt Profile

The profile is fully driven by environment variables (see `.env.example`):

```yaml
# dbt_videogames/profiles.yml
dbt_videogames:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_TRANSFORMER_USER') }}"
      password: "{{ env_var('SNOWFLAKE_TRANSFORMER_PASSWORD') }}"
      role: TRANSFORMER_ROLE
      database: DW_VIDEOGAMES
      schema: DBT_VIDEOGAMES
      warehouse: VIDEOGAMES_WH
      threads: 1
```

---

## 📈 Data Visualisation

Metabase connects directly to Gold tables in Snowflake via `REPORTER_USER`. All aggregations are computed upstream by dbt — no SQL is written on the BI side.

### Global Sales — VGSales up to 2016

![Ventes VGSales 2016](<assets/images/Ventes vgsales 2016.png>)

The dashboard shows **8,911 million units** sold across the full catalogue (up to 2016), broken down across four dimensions:

- **Top games** — Wii Sports leads with 82.5 M units, followed by GTA V (56.6 M) and Super Mario Bros (45.3 M). The top 10 is shared between Nintendo franchises and Activision titles (Call of Duty).
- **Genres** — Action alone accounts for nearly a fifth of all sales (19.6%), ahead of Sports (14.9%) and Shooter (11.8%). Niche genres (Puzzle, Adventure, Strategy) collectively represent only 7%.
- **Manufacturers** — Sony and Nintendo are neck and neck (≈ 3.5–3.6 Bn units each), far ahead of Microsoft (1.4 Bn). Other manufacturers (Sega, Atari, SNK…) are marginal at catalogue scale.
- **Regions** — North America accounts for nearly half of worldwide sales (4,400 M), followed by Europe at 2,423 M, Japan at 1,297 M and the rest of the world at 791 M.

---

## 🧪 Tests

Unit tests cover the dlt pipeline logic **without any real network calls** — all external calls (RAWG API, Kaggle) are mocked.

```bash
uv run pytest tests/ -v
```

| File | Tests | Coverage |
|------|-------|----------|
| `test_rawg_pipeline.py` | 14 | Endpoint, parameters, HTTP errors, pagination stop conditions, nested field extraction (esrb_rating, genres, platforms) |
| `test_vgsales_pipeline.py` | 5 | DataFrame return, correct Kaggle dataset/file, source columns present |

---

## 🚦 Quick Start

### Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) — Python package manager
- Docker and Docker Compose
- A Snowflake account *(free trial available)*
- A RAWG API key *(free at [rawg.io](https://rawg.io/apidocs))*
- A Kaggle account *(for automatic download via kagglehub)*

### Installation

```bash
git clone https://github.com/clement17000/videogames-analysis.git
cd videogames-analysis

# Install Python dependencies (requires uv — https://docs.astral.sh/uv/)
uv sync

# Enable pre-commit hooks (ruff runs automatically before each commit)
uv run pre-commit install

# Set environment variables
cp .env.example .env
# Fill in RAWG_API_KEY, SNOWFLAKE_ACCOUNT, SNOWFLAKE_*_USER, etc.

# Set dlt credentials
cp .dlt/secrets.example.toml .dlt/secrets.toml
# Fill in your Snowflake credentials in secrets.toml

# Start Airflow + PostgreSQL + Metabase
docker compose up -d
```

**Airflow**: [http://localhost:8080](http://localhost:8080)  
**Metabase**: [http://localhost:3000](http://localhost:3000)

### Initialise Snowflake

Run the `setup/snowflake_setup.sql` script from an `ACCOUNTADMIN` account to create the warehouse, database, schemas, roles and grants.

### Configure dbt

`dbt_videogames/profiles.yml` is already in the repo and reads credentials from environment variables — no manual edit needed if your `.env` is correctly filled in.

Install dbt packages:

```bash
cd dbt_videogames
dbt deps --profiles-dir .
```

### Run a pipeline

From the Airflow UI, manually trigger:
- `elt_vgsales_csv_dag` — loads the Kaggle CSV and transforms up to `fct_vgsales`
- `elt_rawg_api_dag` — paginates the RAWG API and transforms up to `fct_rawg_games`

Or use the Makefile shortcuts:
```bash
make run-vgsales   # dlt pipeline — Kaggle CSV → Snowflake
make dbt-build     # dbt build — all models
make test          # unit tests
```

---

## 👤 Author

**Clément Gréau** — Data Engineer  
[github.com/clement17000](https://github.com/clement17000) · greauclement@gmail.com
