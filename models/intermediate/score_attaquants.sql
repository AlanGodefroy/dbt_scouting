with kpi as (
    select * from {{ ref('attack_kpi_new') }}
),

scored as (
    select
        player,
        poste,

        -- KPIs bruts utiles pour debug
        buts_par_match,
        xG_par_match,
        taux_conversion,
        pd_par_match,
        dribbles_par_match,
        tirs_cadres_par_match,

        -- Score attaque sur 60
        ROUND(
            (COALESCE(buts_par_match, 0)        / NULLIF(MAX(buts_par_match) OVER(), 0)        * 20) +
            (COALESCE(xG_par_match, 0)          / NULLIF(MAX(xG_par_match) OVER(), 0)          * 15) +
            (COALESCE(taux_conversion, 0)       / NULLIF(MAX(taux_conversion) OVER(), 0)       * 10) +
            (COALESCE(pd_par_match, 0)          / NULLIF(MAX(pd_par_match) OVER(), 0)          * 5)  +
            (COALESCE(dribbles_par_match, 0)    / NULLIF(MAX(dribbles_par_match) OVER(), 0)    * 5)  +
            (COALESCE(tirs_cadres_par_match, 0) / NULLIF(MAX(tirs_cadres_par_match) OVER(), 0) * 5)
        , 1) AS score_attaque

    from kpi
)

select * from scored
order by score_attaque DESC