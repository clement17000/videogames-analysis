-- =============================================================================
-- fct_vgsales  |  Table de faits (table Gold)
-- Source : stg__vgsales + dim_platform + dim_genre + dim_publisher
-- Rôle   : table de faits principale pour l'analyse des ventes de jeux vidéo.
--          Joint les clés dimensionnelles aux métriques de ventes et de scores.
-- Grain  : une ligne par (jeu, plateforme) = game_platform_id
-- =============================================================================

with games as (

    select * from {{ ref('stg__vgsales') }}

),

final as (

    select
        -- Clés
        g.game_platform_id,
        g.game_id,
        pl.platform_code,
        pl.platform_name,
        pl.manufacturer,
        ge.genre_id,
        pu.publisher_id,

        -- Attributs descriptifs
        g.game_name,
        g.developer,
        g.release_year,
        g.esrb_rating,

        -- Métriques de ventes
        g.na_sales,
        g.eu_sales,
        g.jp_sales,
        g.other_sales,
        round(g.na_sales + g.eu_sales + g.jp_sales + g.other_sales, 2) as global_sales,

        -- Scores
        g.critic_score,
        g.critic_count,
        g.user_score,
        g.user_count

    from games g
    left join {{ ref('dim_platform') }}  pl on g.platform      = pl.platform_code
    left join {{ ref('dim_genre') }}     ge on g.genre         = ge.genre_name
    left join {{ ref('dim_publisher') }} pu on g.publisher      = pu.publisher_name

)

select * from final