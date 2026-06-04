-- =============================================================================
-- stg__vgsales  |  Couche Staging (vue Silver)
-- Source : vgsales_csv.vgsales — ingéré via dlt depuis le CSV Kaggle
-- Rôle   : déduplication (meilleure ligne par jeu+plateforme), typage, renommage
-- Grain  : une ligne par (jeu, plateforme) = game_platform_id
-- =============================================================================

with source as (

    select * from {{ source('vgsales_source', 'vgsales') }}

),

deduped as (
    
    -- select * intentionnel : volume faible (~16k lignes) et toutes les colonnes
    -- sont utilisées en aval. Sur un dataset large, on restreindrait ici.
    select *,
        row_number() over (
            partition by trim(NAME), trim(PLATFORM)
            order by GLOBAL_SALES desc nulls last
        ) as rn
    from source
    where nullif(trim(NAME), '') is not null
      and nullif(trim(PLATFORM), '') is not null

),

staged as (

    select
        -- Identifiants & Clés
        {{ dbt_utils.generate_surrogate_key(['NAME', 'PLATFORM']) }} as game_platform_id,
        {{ dbt_utils.generate_surrogate_key(['NAME']) }}             as game_id,

        -- Informations descriptives
        trim(NAME)                           as game_name,
        trim(PLATFORM)                       as platform,
        cast(YEAR_OF_RELEASE as integer)     as release_year,
        trim(GENRE)                          as genre,
        trim(PUBLISHER)                      as publisher,
        trim(DEVELOPER)                      as developer,
        trim(RATING)                         as esrb_rating,

        -- Métriques financières (en millions d'unités)
        NA_SALES                             as na_sales,
        EU_SALES                             as eu_sales,
        JP_SALES                             as jp_sales,
        OTHER_SALES                          as other_sales,

        -- Scores et Comptes
        cast(CRITIC_SCORE as integer)        as critic_score,
        cast(CRITIC_COUNT as integer)        as critic_count,
        try_cast(USER_SCORE as double)       as user_score,
        cast(USER_COUNT as integer)          as user_count

    from deduped
    where rn = 1

)

select * from staged