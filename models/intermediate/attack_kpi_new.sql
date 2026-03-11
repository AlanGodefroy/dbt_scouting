with events as (
    select * from {{ ref('stg_Raw_data__Events_Leverkusen') }}
),

poste as (
    select * from {{ ref('stg_Raw_data__Poste_Leverkusen') }}
),

joined as (
    select
        events.*,
        poste.poste
    from events
    left join poste
        on events.player = poste.player_name
),  -- ✅ virgule manquante ici

kpi as (
    select
        player,
        poste,

        -- Volume
        COUNT(DISTINCT match_id)                                AS matchs_joues,

        -- Buts
        COUNTIF(shot_outcome = 'Goal')                          AS buts,
        ROUND(COUNTIF(shot_outcome = 'Goal') 
              / NULLIF(COUNT(DISTINCT match_id), 0) * 90, 2)   AS buts_par_90,

        -- xG
        ROUND(SUM(shot_statsbomb_xg), 2)                       AS xG_total,
        ROUND(SUM(shot_statsbomb_xg) 
              / NULLIF(COUNT(DISTINCT match_id), 0) * 90, 2)   AS xG_par_90,

        -- Qualité de finition
        ROUND(COUNTIF(shot_outcome = 'Goal') 
              / NULLIF(COUNTIF(shot_outcome IS NOT NULL), 0), 2) AS taux_conversion,
        ROUND(SUM(shot_statsbomb_xg) 
              / NULLIF(COUNTIF(shot_outcome IS NOT NULL), 0), 2) AS xG_par_tir,

        -- Surperformance
        ROUND(COUNTIF(shot_outcome = 'Goal') 
              - SUM(shot_statsbomb_xg), 2)                     AS buts_vs_xG,

        -- Création
        COUNTIF(pass_goal_assist = TRUE)                       AS passes_decisives,
        ROUND(COUNTIF(pass_goal_assist = TRUE) 
              / NULLIF(COUNT(DISTINCT match_id), 0) * 90, 2)   AS pd_par_90,

        -- Dribbles
        COUNTIF(dribble_outcome = 'Complete')                  AS dribbles_reussis,

        -- Tirs cadrés
        COUNTIF(shot_outcome IN ('Goal', 'Saved'))             AS tirs_cadres

    from joined
    where team = 'Bayer Leverkusen'
    and poste = 'Attack'
    group by player, poste
    order by xG_par_90 DESC)

    select * from kpi 