-- =============================================================================
-- SNOWFLAKE SETUP — videogames-analysis (Version Finale Améliorée)
-- =============================================================================
-- À exécuter en une seule fois depuis un compte ACCOUNTADMIN
-- Ordre : Warehouse → Database → Schemas → Roles & Hierarchy → Users → Grants
-- =============================================================================

-- =============================================================================
-- 0. CONTEXTE D'EXÉCUTION
-- =============================================================================
USE ROLE ACCOUNTADMIN;


-- =============================================================================
-- 1. WAREHOUSE
-- =============================================================================
CREATE WAREHOUSE IF NOT EXISTS VIDEOGAMES_WH
    WAREHOUSE_SIZE    = 'X-SMALL'
    AUTO_SUSPEND      = 60
    AUTO_RESUME       = TRUE
    COMMENT           = 'Warehouse principal du projet videogames-analysis';


-- =============================================================================
-- 2. DATABASE
-- =============================================================================
CREATE DATABASE IF NOT EXISTS DW_VIDEOGAMES
    COMMENT = 'Data Warehouse principal — projet videogames-analysis';

USE DATABASE DW_VIDEOGAMES;


-- =============================================================================
-- 3. SCHEMAS
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS DW_VIDEOGAMES.RAWG_API
    COMMENT = 'Données brutes ingérées par dlt depuis l API RAWG';

CREATE SCHEMA IF NOT EXISTS DW_VIDEOGAMES.VGSALES_CSV
    COMMENT = 'Données brutes ingérées par dlt depuis le CSV VGSales (Kaggle)';

CREATE SCHEMA IF NOT EXISTS DW_VIDEOGAMES.DBT_VIDEOGAMES
    COMMENT = 'Transformations dbt — couches Bronze (base_*), Silver (stg__*), Gold (mart__*)';


-- =============================================================================
-- 4. ROLES & HIERARCHY
-- =============================================================================
CREATE ROLE IF NOT EXISTS LOADER_ROLE      COMMENT = 'Utilisé par dlt et Airflow pour ingérer les données brutes';
CREATE ROLE IF NOT EXISTS TRANSFORMER_ROLE comment = 'Utilisé par dbt pour lire le raw et écrire les transformations';
CREATE ROLE IF NOT EXISTS REPORTER_ROLE    COMMENT = 'Utilisé par Metabase pour lire les tables Gold en lecture seule';

-- Rattachement de la hiérarchie au SYSADMIN pour éviter les objets orphelins
GRANT ROLE LOADER_ROLE      TO ROLE SYSADMIN;
GRANT ROLE TRANSFORMER_ROLE TO ROLE SYSADMIN;
GRANT ROLE REPORTER_ROLE    TO ROLE SYSADMIN;


-- =============================================================================
-- 5. USERS & PASSWORDS (Alignés sur ton fichier .env)
-- =============================================================================

CREATE USER IF NOT EXISTS LOADER_USER
    PASSWORD          = 'LoaderUserFrom17'
    DEFAULT_ROLE      = LOADER_ROLE
    DEFAULT_WAREHOUSE = VIDEOGAMES_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT           = 'Compte de service dlt / Airflow';

CREATE USER IF NOT EXISTS TRANSFORMER_USER
    PASSWORD          = 'TransformerUser17'
    DEFAULT_ROLE      = TRANSFORMER_ROLE
    DEFAULT_WAREHOUSE = VIDEOGAMES_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT           = 'Compte de service dbt';

CREATE USER IF NOT EXISTS REPORTER_USER
    PASSWORD          = 'ReporterUser17'
    DEFAULT_ROLE      = REPORTER_ROLE
    DEFAULT_WAREHOUSE = VIDEOGAMES_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT           = 'Compte de service Metabase';


-- =============================================================================
-- 6. GRANTS — Warehouse
-- =============================================================================
GRANT USAGE ON WAREHOUSE VIDEOGAMES_WH TO ROLE LOADER_ROLE;
GRANT USAGE ON WAREHOUSE VIDEOGAMES_WH TO ROLE TRANSFORMER_ROLE;
GRANT USAGE ON WAREHOUSE VIDEOGAMES_WH TO ROLE REPORTER_ROLE;


-- =============================================================================
-- 7. GRANTS — Database
-- =============================================================================
GRANT USAGE ON DATABASE DW_VIDEOGAMES TO ROLE LOADER_ROLE;
GRANT CREATE SCHEMA ON DATABASE DW_VIDEOGAMES TO ROLE LOADER_ROLE;
GRANT USAGE ON DATABASE DW_VIDEOGAMES TO ROLE TRANSFORMER_ROLE;
GRANT USAGE ON DATABASE DW_VIDEOGAMES TO ROLE REPORTER_ROLE;


-- =============================================================================
-- 8. GRANTS — Schemas & Tables
-- =============================================================================

-- ── LOADER_ROLE (dlt) ────────────────────────────────────────────────────────
GRANT USAGE, CREATE TABLE, CREATE STAGE ON SCHEMA DW_VIDEOGAMES.RAWG_API      TO ROLE LOADER_ROLE;
GRANT USAGE, CREATE TABLE, CREATE STAGE ON SCHEMA DW_VIDEOGAMES.VGSALES_CSV    TO ROLE LOADER_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA DW_VIDEOGAMES.RAWG_API    TO ROLE LOADER_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA DW_VIDEOGAMES.VGSALES_CSV TO ROLE LOADER_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA DW_VIDEOGAMES.RAWG_API    TO ROLE LOADER_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA DW_VIDEOGAMES.VGSALES_CSV TO ROLE LOADER_ROLE;


-- ── TRANSFORMER_ROLE (dbt) ───────────────────────────────────────────────────
GRANT USAGE ON SCHEMA DW_VIDEOGAMES.RAWG_API     TO ROLE TRANSFORMER_ROLE;
GRANT USAGE ON SCHEMA DW_VIDEOGAMES.VGSALES_CSV  TO ROLE TRANSFORMER_ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA DW_VIDEOGAMES.RAWG_API    TO ROLE TRANSFORMER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DW_VIDEOGAMES.VGSALES_CSV TO ROLE TRANSFORMER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DW_VIDEOGAMES.RAWG_API    TO ROLE TRANSFORMER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DW_VIDEOGAMES.VGSALES_CSV TO ROLE TRANSFORMER_ROLE;

GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA DW_VIDEOGAMES.DBT_VIDEOGAMES TO ROLE TRANSFORMER_ROLE;
GRANT CREATE SCHEMA ON DATABASE DW_VIDEOGAMES TO ROLE TRANSFORMER_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA DW_VIDEOGAMES.DBT_VIDEOGAMES    TO ROLE TRANSFORMER_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA DW_VIDEOGAMES.DBT_VIDEOGAMES TO ROLE TRANSFORMER_ROLE;


-- ── REPORTER_ROLE (Metabase) ─────────────────────────────────────────────────
GRANT USAGE ON SCHEMA DW_VIDEOGAMES.DBT_VIDEOGAMES TO ROLE REPORTER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DW_VIDEOGAMES.DBT_VIDEOGAMES TO ROLE REPORTER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DW_VIDEOGAMES.DBT_VIDEOGAMES TO ROLE REPORTER_ROLE;

-- Metabase doit aussi pouvoir lire les futures VUES générées par dbt
GRANT SELECT ON FUTURE VIEWS IN SCHEMA DW_VIDEOGAMES.DBT_VIDEOGAMES TO ROLE REPORTER_ROLE;


-- =============================================================================
-- 9. ASSIGNATION DES ROLES AUX USERS
-- =============================================================================
GRANT ROLE LOADER_ROLE      TO USER LOADER_USER;
GRANT ROLE TRANSFORMER_ROLE TO USER TRANSFORMER_USER;
GRANT ROLE REPORTER_ROLE    TO USER REPORTER_USER;


-- =============================================================================
-- 10. VÉRIFICATION
-- =============================================================================
SHOW SCHEMAS IN DATABASE DW_VIDEOGAMES;
SHOW ROLES;
SHOW WAREHOUSES;