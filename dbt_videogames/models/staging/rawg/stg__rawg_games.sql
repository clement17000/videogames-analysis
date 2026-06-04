-- =============================================================================
-- stg__rawg_games  |  Couche Staging (vue Silver)
-- Source : rawg_api.games — ingéré via dlt depuis l'API RAWG (REST paginée)
-- Rôle   : typage, renommage et nettoyage métier avant exposition aux marts
-- =============================================================================

with source_games as (
    select * from {{ source('rawg_source', 'games') }}
),

staged as (
    select
        -- Clés
        {{ dbt_utils.generate_surrogate_key(['id']) }} as game_id,

        -- Informations textuelles
        trim(name)                as game_name,
        slug                      as game_slug,
        trim(esrb_rating)         as esrb_rating,

        -- Scores
        case
            when metacritic < 0 or metacritic > 100 then null
            else metacritic
        end                       as metacritic_score,

        rating                    as user_rating,
        rating_top                as max_user_rating,
        ratings_count,

        -- Temps de jeu (contrôle des outliers supérieurs uniquement)
        case
            when playtime > 8760 then null  -- > 1 an : outlier
            else playtime
        end                       as average_playtime_hours,

        -- Dates
        cast(released as date)      as release_date,
        cast(updated as timestamp)  as updated_at

    from source_games
    where nullif(trim(name), '') is not null
    -- playtime = 0 signifie "non renseigné" dans RAWG, pas un vrai temps de jeu nul
    and playtime > 0
)

select * from staged