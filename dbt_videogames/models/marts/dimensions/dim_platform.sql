-- =============================================================================
-- dim_platform  |  Dimension (table Gold)
-- Source : stg__vgsales + seed platform_mapping
-- Rôle   : enrichit les codes plateforme courts (ex: "PS3") avec le nom complet
--          et le fabricant (Sony, Microsoft, Nintendo...)
-- =============================================================================

with platforms as (

    select distinct
        platform as platform_code
    from {{ ref('stg__vgsales') }}

),

mapped as (

    select
        p.platform_code,
        m.platform_name,
        m.manufacturer
    from platforms p
    -- LEFT JOIN car le seed platform_mapping peut ne pas couvrir tous les codes
    -- (ex: plateformes rares ou mal orthographiées dans le CSV source)
    left join {{ ref('platform_mapping') }} m
        on p.platform_code = m.platform_code

)

select * from mapped