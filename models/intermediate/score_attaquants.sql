with kpi as (
    select * from {{ ref('attack_kpi_new') }}
),

normalized as (
    select
        player,
        poste,

        -- Normalisation (Base 100)
        ROUND((buts_par_match - MIN(buts_par_match) OVER()) / NULLIF(MAX(buts_par_match) OVER() - MIN(buts_par_match) OVER(), 0) * 100, 1) AS note_buts,
        ROUND((xG_par_match - MIN(xG_par_match) OVER()) / NULLIF(MAX(xG_par_match) OVER() - MIN(xG_par_match) OVER(), 0) * 100, 1) AS note_xG,
        ROUND((taux_conversion - MIN(taux_conversion) OVER()) / NULLIF(MAX(taux_conversion) OVER() - MIN(taux_conversion) OVER(), 0) * 100, 1) AS note_conversion,
        ROUND((pd_par_match - MIN(pd_par_match) OVER()) / NULLIF(MAX(pd_par_match) OVER() - MIN(pd_par_match) OVER(), 0) * 100, 1) AS note_pd,
        ROUND((dribbles_par_match - MIN(dribbles_par_match) OVER()) / NULLIF(MAX(dribbles_par_match) OVER() - MIN(dribbles_par_match) OVER(), 0) * 100, 1) AS note_dribbles,
        ROUND((tirs_cadres_par_match - MIN(tirs_cadres_par_match) OVER()) / NULLIF(MAX(tirs_cadres_par_match) OVER() - MIN(tirs_cadres_par_match) OVER(), 0) * 100, 1) AS note_tirs_cadres,
        ROUND((passes_par_match - MIN(passes_par_match) OVER()) / NULLIF(MAX(passes_par_match) OVER() - MIN(passes_par_match) OVER(), 0) * 100, 1) AS note_passes,
        ROUND((pass_through_ball_per_match - MIN(pass_through_ball_per_match) OVER()) / NULLIF(MAX(pass_through_ball_per_match) OVER() - MIN(pass_through_ball_per_match) OVER(), 0) * 100, 1) AS note_through_ball,
        ROUND((interceptions_par_match - MIN(interceptions_par_match) OVER()) / NULLIF(MAX(interceptions_par_match) OVER() - MIN(interceptions_par_match) OVER(), 0) * 100, 1) AS note_interceptions,
        ROUND((duels_par_match - MIN(duels_par_match) OVER()) / NULLIF(MAX(duels_par_match) OVER() - MIN(duels_par_match) OVER(), 0) * 100, 1) AS note_duels
    from kpi
),

scored as (
    select
        *,
        -- Score Attack SUR 100 (Moyenne pure des 6 KPIs)
        ROUND(
            (COALESCE(note_buts, 0) + COALESCE(note_xG, 0) + COALESCE(note_conversion, 0) + 
             COALESCE(note_pd, 0) + COALESCE(note_dribbles, 0) + COALESCE(note_tirs_cadres, 0)) / 6
        , 1) AS score_attack,

        -- Score Middle SUR 100 (Moyenne pure des 2 KPIs)
        ROUND(
            (COALESCE(note_passes, 0) + COALESCE(note_through_ball, 0)) / 2
        , 1) AS score_middle,

        -- Score Defense SUR 100 (Moyenne pure des 2 KPIs)
        ROUND(
            (COALESCE(note_interceptions, 0) + COALESCE(note_duels, 0)) / 2
        , 1) AS score_defense

    from normalized
)

select 
    *,
    -- Note Finale : Application des coefficients (60% / 20% / 20%) sur les scores en base 100
    ROUND((score_attack * 0.60) + (score_middle * 0.20) + (score_defense * 0.20), 0) AS note_fifa
from scored
order by note_fifa DESC