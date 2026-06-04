-- =============================================================================
-- dim_genre  |  Dimension (table Gold)
-- Source : stg__vgsales — liste des genres distincts du catalogue vgsales
-- Rôle   : table de référence pour filtrer et regrouper les faits par genre
-- =============================================================================

with genres as (

    select distinct
        genre as genre_name
    from {{ ref('stg__vgsales') }}
    where nullif(trim(genre), '') is not null

)

select
    {{ dbt_utils.generate_surrogate_key(['genre_name']) }} as genre_id,
    genre_name
from genres