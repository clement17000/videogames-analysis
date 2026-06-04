-- =============================================================================
-- fct_games_enriched  |  Table de faits enrichie (table Gold)
-- Sources : fct_vgsales (ventes, scores presse) + fct_rawg_games (scores RAWG)
-- Rôle    : combine les données de ventes historiques avec les scores de notation
--           RAWG pour permettre une analyse croisée ventes ↔ popularité joueurs.
-- Grain   : une ligne par (jeu, plateforme) présent dans les DEUX sources
-- INNER JOIN intentionnel : seuls les jeux présents dans les deux sources sont
--   conservés. Un LEFT JOIN garderait des lignes sans données RAWG, inutilisables
--   pour les analyses de corrélation ventes/scores.
-- Filtre release_year <= 2016 : le dataset vgsales ne couvre que jusqu'à 2016 ;
--   les jeux RAWG plus récents n'ont pas de données de ventes comparables.
-- =============================================================================

with vgsales as (

    select * from {{ ref('fct_vgsales') }}

),

rawg as (

    select * from {{ ref('fct_rawg_games') }}

),

final as (

    select
        -- Clés
        v.game_platform_id,
        v.game_id,

        -- Attributs descriptifs
        v.game_name,
        v.platform_code,
        v.platform_name,
        v.manufacturer,
        v.release_year,
        v.esrb_rating,
        v.genre_id,
        v.publisher_id,
        v.developer,

        -- Ventes (source vgsales)
        v.na_sales,
        v.eu_sales,
        v.jp_sales,
        v.other_sales,
        v.global_sales,

        -- Scores presse et joueurs (source RAWG)
        r.metacritic_score,
        r.user_rating,
        r.ratings_count,
        r.average_playtime_hours

    from vgsales v
    inner join rawg r
        -- Jointure sur le nom normalisé : les deux sources n'ont pas de clé commune.
        -- La mise en minuscules et le trim absorbent les variations de casse/espaces.
        on lower(trim(v.game_name)) = lower(trim(r.game_name))
    -- Le dataset vgsales s'arrête à 2016 ; exclure les années suivantes évite
    -- de polluer les agrégats avec des lignes sans données de ventes.
    where v.release_year <= 2016

)

select * from final