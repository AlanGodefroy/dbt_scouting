with events as (
    select * from {{ ref('stg_Raw_data__Events_Leverkusen') }}
),

poste as (
    select * from {{ ref('stg_Raw_data__Poste_Leverkusen') }}
),

minutes as (
    select
        player,
        SUM(minutes_played) AS total_minutes
    from {{ ref('int_collective_kpis') }}
    group by player
),

joined as (
    select
        events.*,
        poste.poste
    from events
    left join poste
        on events.player = poste.player_name
),

kpi as (
    select
        joined.player,
        joined.poste,

        -- Volume
        COUNT(DISTINCT match_id)                                AS matchs_joues,
        ROUND(MAX(minutes.total_minutes)
              / NULLIF(COUNT(DISTINCT match_id), 0), 2)        AS minutes_par_match,
        -- Buts
        COUNTIF(shot_outcome = 'Goal')                          AS buts,
        ROUND(COUNTIF(shot_outcome = 'Goal') 
              / NULLIF(COUNT(DISTINCT match_id), 0), 2)        AS buts_par_match,

        -- xG
        ROUND(SUM(shot_statsbomb_xg), 2)                       AS xG_total,
        ROUND(SUM(shot_statsbomb_xg) 
              / NULLIF(COUNT(DISTINCT match_id), 0), 2)        AS xG_par_match,

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
              / NULLIF(COUNT(DISTINCT match_id), 0), 2)        AS pd_par_match,

       -- Dribbles
        COUNTIF(dribble_outcome = 'Complete')                  AS dribbles_reussis,
        ROUND(COUNTIF(dribble_outcome = 'Complete')
              / NULLIF(COUNT(DISTINCT match_id), 0), 2)        AS dribbles_par_match,

        -- Tirs cadrés
        COUNTIF(shot_outcome IN ('Goal', 'Saved'))             AS tirs_cadres,
        ROUND(COUNTIF(shot_outcome IN ('Goal', 'Saved'))
              / NULLIF(COUNT(DISTINCT match_id), 0), 2)        AS tirs_cadres_par_match,
        -- Volume
        MAX(minutes.total_minutes)                              AS total_minutes

    from joined
    left join minutes on joined.player = minutes.player
    where team = 'Bayer Leverkusen'
    and poste = 'Attack'
    group by joined.player, joined.poste
    order by xG_par_match DESC
)

select * from kpi