-- =============================================================================
-- fct_rawg_games  |  Table de faits incrémentale (table Gold)
-- Source : stg__rawg_games
-- Rôle   : expose les métriques de notation et d'engagement RAWG (Metacritic,
--          user_rating, playtime) prêtes à l'analyse et à la jointure avec fct_vgsales.
-- Grain  : une ligne par jeu unique (game_id = surrogate key sur l'id RAWG)
-- Incrémental : seuls les jeux dont updated_at dépasse le max déjà en table
--   sont traités. Cohérent avec write_disposition="merge" côté dlt.
-- =============================================================================

{{
    config(
        materialized='incremental',
        unique_key='game_id'
    )
}}

with games as (

    select * from {{ ref('stg__rawg_games') }}

    {% if is_incremental() %}
        -- En mode incremental, ne traiter que les jeux nouveaux ou mis à jour
        -- depuis le dernier run. updated_at est fourni par l'API RAWG.
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}

),

final as (

    select
        -- Clés
        game_id,

        -- Attributs descriptifs
        game_name,
        game_slug,
        esrb_rating,
        release_date,
        extract(year from release_date)  as release_year,

        -- Scores
        metacritic_score,
        user_rating,
        max_user_rating,
        ratings_count,

        -- Engagement
        average_playtime_hours,

        -- Watermark pour la logique incrémentale
        updated_at

    from games

)

select * from final