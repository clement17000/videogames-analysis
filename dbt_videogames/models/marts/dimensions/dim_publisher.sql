-- =============================================================================
-- dim_publisher  |  Dimension (table Gold)
-- Source : stg__vgsales — liste des éditeurs distincts du catalogue vgsales
-- Rôle   : table de référence pour regrouper les ventes par éditeur
-- =============================================================================

with publishers as (

    select distinct
        publisher as publisher_name
    from {{ ref('stg__vgsales') }}
    where nullif(trim(publisher), '') is not null

)

select
    {{ dbt_utils.generate_surrogate_key(['publisher_name']) }} as publisher_id,
    publisher_name
from publishers