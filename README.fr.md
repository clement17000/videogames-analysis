# 🎮 VIDEOGAMES ANALYSIS

🇬🇧 [English](README.md) | 🇫🇷 Français

> **Un projet d'ingénierie des données de bout en bout qui collecte, transforme et visualise des données sur les jeux vidéo — évolution des genres, domination des plateformes, corrélation scores / ventes et préférences régionales.**

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker%20Compose-2496ED?style=for-the-badge&logo=Docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![CI](https://github.com/clement17000/videogames-analysis/actions/workflows/ci-cd.yml/badge.svg)

---

## 📋 Table des matières

1. [Pourquoi ce projet existe](#-pourquoi-ce-projet-existe)
2. [La Modern Data Stack](#-la-modern-data-stack)
3. [Architecture](#-architecture)
4. [Sources de données](#-sources-de-données)
5. [Stack technique](#-stack-technique)
6. [Structure du projet](#-structure-du-projet)
7. [dbt — Transformations](#️-dbt--transformations)
8. [Configuration Snowflake](#️-configuration-snowflake)
9. [Visualisation des données](#-visualisation-des-données)
10. [Tests](#-tests)
11. [Démarrage rapide](#-démarrage-rapide)

---

## 🎯 Pourquoi ce projet existe

Ce projet a été construit comme un **portfolio personnel d'ingénierie des données** — une plateforme de données moderne de bout en bout, bâtie sur de vraies sources publiques (API REST + CSV) et non sur des jeux de données pré-nettoyés.

L'objectif était délibéré : concevoir, construire et opérer un **pipeline de qualité production** depuis zéro — orchestration réelle, gestion de l'évolution des schémas, contrats de qualité des données et modèle de transformation en couches.

Chaque décision architecturale reflète la manière dont une équipe d'ingénierie des données aborderait ce problème dans une vraie entreprise.

---

## 🛠 La Modern Data Stack

- **🚀 Apache Airflow 3 (Orchestration)** — Orchestre la séquence extraction → transformation pour chaque source. Deux DAGs indépendants, déclenchés manuellement, structurés avec l'Airflow Task SDK (décorateurs `@dag` / `@task`).

- **📥 dlt — dlthub (Ingestion)** — Gère l'extraction et le chargement (EL) des deux sources. Infère automatiquement les schémas à partir des réponses JSON, normalise les structures imbriquées et déduplique les données à chaque ré-exécution via clé primaire (`merge` pour l'API RAWG, `replace` pour le CSV Kaggle).

- **🏛️ dbt Core (Transformation)** — Transforme les données brutes en tables prêtes pour l'analyse via une architecture Silver → Gold. Chaque couche est testée (unique, not_null) et documentée.

- **❄️ Snowflake (Data Warehouse)** — Entrepôt de données cloud. Les schémas bruts (`rawg_api`, `vgsales_csv`) sont gérés par dlt ; les schémas transformés (`staging`, `marts`) sont gérés par dbt.

- **📊 Metabase (Visualisation)** — Se connecte directement aux tables Gold de Snowflake. Tous les dashboards sont construits sur les marts pré-calculés — aucun SQL n'est écrit côté BI.

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCES DE DONNÉES                       │
│                                                                 │
│   RAWG API (REST paginée)      VGSales CSV (Kaggle)             │
│   500k+ jeux, tri par rating   ~16k jeux, ventes par région     │
└────────────┬───────────────────────┬────────────────────────────┘
             │                       │
             ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                     INGESTION — dlt                             │
│                                                                 │
│   Pagination + merge           Lecture CSV + replace            │
│   (clé primaire : id RAWG)     (snapshot complet)               │
└────────────┬───────────────────────┬────────────────────────────┘
             │                       │
             ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                  SNOWFLAKE — Couche RAW                         │
│                                                                 │
│   DW_VIDEOGAMES.RAWG_API            DW_VIDEOGAMES.VGSALES_CSV   │
│   └── games                         └── vgsales                 │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│              SNOWFLAKE — Couche dbt (Silver → Gold)             │
│                                                                 │
│  ⚪ Silver   stg__rawg_games        stg__vgsales                 │
│  (vues)     nettoyage, typage, renommage, outliers              │
│                          │                                      │
│  🟡 Gold    Dimensions : dim_platform  dim_genre  dim_publisher  │
│  (tables)   Facts      : fct_rawg_games                         │
│                          fct_vgsales                            │
│                          fct_games_enriched  ← jointure croisée │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                   VISUALISATION — Metabase                      │
│                                                                 │
│   Connecté directement aux tables Gold de Snowflake             │
└─────────────────────────────────────────────────────────────────┘

Orchestration : Apache Airflow 3 (deux DAGs indépendants)
```

### 🏅 Couches dbt

| Couche | Préfixe | Matérialisation | Rôle |
|--------|---------|-----------------|------|
| ⚪ **Silver** | `stg__*` | Vue | Nettoyage, typage, renommage, déduplication. Pas de logique métier. |
| 🟡 **Gold — Dimensions** | `dim_*` | Table | Référentiels (plateforme, genre, éditeur) enrichis via seeds. |
| 🟡 **Gold — Facts** | `fct_*` | Table | Métriques prêtes pour l'analyse, jointures avec les dimensions. |

### 🔁 Structure des DAGs

Les deux DAGs partagent la même structure simple à deux tâches :

```
trigger_dlt_*       (extraction + chargement Snowflake via dlt)
       ↓
run_dbt             (dbt build --select +<modèle_cible>)
```

| DAG | Sélection dbt | Déclenchement |
|-----|---------------|---------------|
| `elt_rawg_api_dag` | `+fct_rawg_games` | Manuel |
| `elt_vgsales_csv_dag` | `+fct_vgsales` | Manuel |

---

## 📊 Sources de données

| Source | Type | Volume | Description |
|--------|------|--------|-------------|
| **RAWG Video Games Database** | API REST | 500k+ jeux | Genres, plateformes, dates de sortie, scores Metacritic, rating utilisateurs, temps de jeu moyen |
| **Video Game Sales (Kaggle)** | CSV | ~16k jeux | Ventes par région (NA, EU, JP), plateforme, éditeur, développeur, score critique — jusqu'à 2016 |

---

## 💻 Stack technique

| Couche | Outil | Version |
|--------|-------|---------|
| Orchestration | Apache Airflow | `3.0` |
| Ingestion | dlt (dlthub) | `≥ 1.26` |
| Data Warehouse | Snowflake | — |
| Transformations | dbt Core | `1.9` |
| Package dbt | dbt_utils | `≥ 1.0` |
| Visualisation | Metabase | latest |
| Tests | pytest | `≥ 9.0` |
| Linting / Formatage | ruff | `≥ 0.11` |
| Gestionnaire de paquets | uv | latest |
| Hooks Git | pre-commit | `≥ 4.0` |
| Langage | Python | `≥ 3.12` |
| Infrastructure | Docker Compose | — |

---

## 📁 Structure du projet

```
videogames-analysis/
├── dags/
│   ├── dlt_pipelines/
│   │   ├── rawg_pipeline.py           # Ingestion RAWG API via dlt (merge)
│   │   └── vgsales_pipeline.py        # Ingestion CSV Kaggle via dlt (replace)
│   ├── elt_rawg_api.py                # DAG Airflow — source API RAWG
│   └── elt_vgsales.py                 # DAG Airflow — source CSV VGSales
├── dbt_videogames/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml                   # Déclaration des sources Snowflake
│   │   │   ├── rawg/
│   │   │   │   ├── stg__rawg_games.sql       # ⚪ Silver — API RAWG
│   │   │   │   └── stg__rawg_games.yml       # Tests & documentation
│   │   │   └── vgsales/
│   │   │       ├── stg__vgsales.sql          # ⚪ Silver — CSV VGSales
│   │   │       └── stg__vgsales.yml          # Tests & documentation
│   │   └── marts/
│   │       ├── exposures.yml                 # Metabase déclaré comme consommateur final
│   │       ├── dimensions/
│   │       │   ├── _dims.yml                 # Docs & tests pour toutes les dims
│   │       │   ├── dim_genre.sql             # 🟡 Genres distincts
│   │       │   ├── dim_platform.sql          # 🟡 Plateformes + fabricant (seed)
│   │       │   └── dim_publisher.sql         # 🟡 Éditeurs distincts
│   │       └── facts/
│   │           ├── _facts.yml                # Docs & tests pour toutes les facts
│   │           ├── fct_rawg_games.sql        # 🟡 Métriques RAWG — incrémental
│   │           ├── fct_vgsales.sql           # 🟡 Ventes par jeu + plateforme
│   │           └── fct_games_enriched.sql    # 🟡 Jointure ventes ↔ scores RAWG
│   ├── seeds/
│   │   ├── _seeds.yml                        # Documentation & tests du seed
│   │   └── platform_mapping.csv             # Code → nom complet + fabricant
│   ├── dbt_project.yml
│   └── profiles.yml
├── tests/
│   ├── conftest.py                    # Configuration pytest (sys.path)
│   ├── test_rawg_pipeline.py          # 14 tests unitaires (sans réseau)
│   └── test_vgsales_pipeline.py       # 5 tests unitaires (sans Kaggle)
├── setup/
│   └── snowflake_setup.sql            # Création warehouse, DB, schémas, rôles, grants
├── .github/
│   └── workflows/
│       └── ci-cd.yml                  # CI : ruff lint → pytest (push/PR)
├── docker-compose.yaml                # Airflow 3 + PostgreSQL + Metabase
├── Dockerfile                         # Image Airflow custom avec dlt, dbt, kagglehub
├── Makefile                           # Raccourcis : make dbt-build, make test, etc.
├── .pre-commit-config.yaml            # ruff tourne automatiquement avant chaque commit
├── .env.example                       # Variables d'environnement à renseigner
├── pyproject.toml
└── requirements.txt
```

---

## 🏛️ dbt — Transformations

### Modèles Silver (vues)

| Modèle | Source | Transformations clés |
|--------|--------|----------------------|
| `stg__rawg_games` | `rawg_api.games` | Surrogate key, nettoyage metacritic (< 0 ou > 100 → NULL), filtre playtime = 0, outlier > 8760h → NULL |
| `stg__vgsales` | `vgsales_csv.vgsales` | Déduplication (meilleure ligne par jeu+plateforme), surrogate keys, cast des types |

### Modèles Gold (tables)

| Modèle | Grain | Description |
|--------|-------|-------------|
| `dim_platform` | Plateforme | Code + nom complet + fabricant (via seed `platform_mapping`) |
| `dim_genre` | Genre | Liste distincte des genres vgsales |
| `dim_publisher` | Éditeur | Liste distincte des éditeurs vgsales |
| `fct_rawg_games` | Jeu (RAWG) | Scores et engagement : Metacritic, user_rating, playtime — **incrémental** |
| `fct_vgsales` | Jeu × Plateforme | Ventes NA/EU/JP + scores presse, jointure avec dimensions |
| `fct_games_enriched` | Jeu × Plateforme | Jointure croisée vgsales ↔ RAWG sur nom normalisé (jeux ≤ 2016) |

### Exposures

Metabase est déclaré comme consommateur final de tous les modèles Gold via `exposures.yml`.
Il apparaît comme nœud terminal dans le graphe de lignage dbt, rendant visible la chaîne
complète des données — des sources brutes jusqu'au dashboard BI.

### Commandes principales

```bash
cd dbt_videogames

dbt deps                                        # Installer les packages (dbt_utils)
dbt build --select +fct_vgsales --profiles-dir .   # Pipeline vgsales complet
dbt build --select +fct_rawg_games --profiles-dir . # Pipeline RAWG complet
dbt build --profiles-dir .                      # Tous les modèles

dbt test --select marts --profiles-dir .        # Tests qualité (unique, not_null, relations)

dbt docs generate --profiles-dir .             # Générer le catalogue de données
dbt docs serve --port 8081                      # Servir sur http://localhost:8081
```

---

## ❄️ Configuration Snowflake

### Organisation des schémas

```
DW_VIDEOGAMES
├── RAWG_API                    ← Données brutes dlt (API RAWG)
│   └── games
├── VGSALES_CSV                 ← Données brutes dlt (CSV Kaggle)
│   └── vgsales
├── DBT_VIDEOGAMES_STAGING      ← Vues Silver dbt (stg__*)
└── DBT_VIDEOGAMES_MARTS        ← Tables Gold dbt (dim_*, fct_*)
```

### Rôles

| Rôle | Usage | Compte de service |
|------|-------|-------------------|
| `LOADER_ROLE` | Lecture/écriture schemas RAW | `LOADER_USER` (dlt / Airflow) |
| `TRANSFORMER_ROLE` | Lecture RAW + lecture/écriture dbt | `TRANSFORMER_USER` (dbt) |
| `REPORTER_ROLE` | Lecture seule tables Gold | `REPORTER_USER` (Metabase) |

Le script `setup/snowflake_setup.sql` crée l'ensemble de ces ressources (warehouse, database, schemas, rôles, grants).

### Profil dbt

Le profil est entièrement piloté par variables d'environnement (voir `.env.example`) :

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

## 📈 Visualisation des données

Metabase se connecte directement aux tables Gold de Snowflake via `REPORTER_USER`. Toutes les agrégations sont calculées en amont par dbt — aucun SQL n'est écrit côté BI.

### Ventes mondiales — VGSales jusqu'à 2016

![Ventes VGSales 2016](<assets/images/Ventes vgsales 2016.png>)

Le dashboard expose **8 911 millions d'unités** vendues sur l'ensemble du catalogue (jusqu'à 2016), croisées sur quatre axes :

- **Top jeux** — Wii Sports domine avec 82,5 M d'unités, suivi de GTA V (56,6 M) et Super Mario Bros (45,3 M). Le top 10 est partagé entre franchises Nintendo et licences Activision (Call of Duty).
- **Genres** — L'Action représente à elle seule près d'un cinquième des ventes (19,6 %), devant Sports (14,9 %) et Shooter (11,8 %). Les genres de niche (Puzzle, Adventure, Strategy) ne pèsent collectivement que 7 %.
- **Fabricants** — Sony et Nintendo se tiennent au coude-à-coude (≈ 3,5–3,6 Md d'unités chacun), loin devant Microsoft (1,4 Md). Les autres fabricants (Sega, Atari, SNK…) sont marginaux à l'échelle du catalogue total.
- **Régions** — L'Amérique du Nord concentre presque la moitié des ventes mondiales (4 400 M), l'Europe suit à 2 423 M, le Japon à 1 297 M et le reste du monde à 791 M.

---

## 🧪 Tests

Les tests unitaires couvrent la logique des pipelines dlt **sans appels réseau réels** — tous les appels externes (API RAWG, Kaggle) sont mockés.

```bash
uv run pytest tests/ -v
```

| Fichier | Tests | Ce qui est couvert |
|---------|-------|--------------------|
| `test_rawg_pipeline.py` | 14 | Endpoint, paramètres, erreurs HTTP, conditions d'arrêt de pagination, extraction des champs imbriqués (esrb_rating, genres, plateformes) |
| `test_vgsales_pipeline.py` | 5 | Retour DataFrame, dataset/fichier Kaggle correct, colonnes source présentes |

---

## 🚦 Démarrage rapide

### Prérequis

- [uv](https://docs.astral.sh/uv/getting-started/installation/) — gestionnaire de paquets Python
- Docker et Docker Compose
- Un compte Snowflake *(trial gratuit disponible)*
- Une clé API RAWG *(gratuite sur [rawg.io](https://rawg.io/apidocs))*
- Un compte Kaggle *(pour le téléchargement automatique via kagglehub)*

### Installation

```bash
git clone https://github.com/clement17000/videogames-analysis.git
cd videogames-analysis

# Installer les dépendances Python (nécessite uv — https://docs.astral.sh/uv/)
uv sync

# Activer les pre-commit hooks (ruff tourne automatiquement avant chaque commit)
uv run pre-commit install

# Configurer les variables d'environnement
cp .env.example .env
# Renseigner RAWG_API_KEY, SNOWFLAKE_ACCOUNT, SNOWFLAKE_*_USER, etc.

# Configurer les credentials dlt
cp .dlt/secrets.example.toml .dlt/secrets.toml
# Renseigner les credentials Snowflake dans secrets.toml

# Démarrer Airflow + PostgreSQL + Metabase
docker compose up -d
```

**Airflow** : [http://localhost:8080](http://localhost:8080)  
**Metabase** : [http://localhost:3000](http://localhost:3000)

### Initialiser Snowflake

Exécuter le script `setup/snowflake_setup.sql` depuis un compte `ACCOUNTADMIN` pour créer le warehouse, la database, les schémas, les rôles et les grants.

### Configurer dbt

`dbt_videogames/profiles.yml` est déjà dans le repo et lit les credentials depuis les variables d'environnement — aucune modification manuelle nécessaire si le `.env` est correctement renseigné.

Installer les packages dbt :

```bash
cd dbt_videogames
dbt deps --profiles-dir .
```

### Lancer un pipeline

Depuis l'interface Airflow, déclencher manuellement :
- `elt_vgsales_csv_dag` — charge le CSV Kaggle et transforme jusqu'à `fct_vgsales`
- `elt_rawg_api_dag` — pagine l'API RAWG et transforme jusqu'à `fct_rawg_games`

Ou via les raccourcis Makefile :
```bash
make run-vgsales   # pipeline dlt — CSV Kaggle → Snowflake
make dbt-build     # dbt build — tous les modèles
make test          # tests unitaires
```

---

## 👤 Auteur

**Clément Gréau** — Data Engineer  
[github.com/clement17000](https://github.com/clement17000) · greauclement@gmail.com
