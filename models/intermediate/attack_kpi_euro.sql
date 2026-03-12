with events as (
    select * from {{ ref('stg_Raw_data__Events_euro_2024') }}
),

poste as (
    select * from {{ ref('stg_Raw_data__Poste_euro_2024') }}
),

joined as (
    select
        events.*,
        poste.poste
    from events
    left join poste
        on events.player = poste.player_name
),

-- ✅ Clé du changement : granularité match x joueur
per_match as (
    select
        player,
        poste,
        match_id,

        -- Minutes (si disponible au niveau événement, sinon à joindre séparément)
        -- MAX(minute)                                             AS minutes_jouees,

        -- Finition
        COUNTIF(shot_outcome = 'Goal')                         AS buts,
        SUM(shot_statsbomb_xg)                                 AS xG,
        COUNTIF(shot_outcome IN ('Goal', 'Saved'))             AS tirs_cadres,
        SAFE_DIVIDE(
            COUNTIF(shot_outcome = 'Goal'),
            COUNTIF(shot_outcome IS NOT NULL)
        )                                                       AS taux_conversion,
        SAFE_DIVIDE(
            SUM(shot_statsbomb_xg),
            COUNTIF(shot_outcome IS NOT NULL)
        )                                                       AS xG_par_tir,

        -- Surperformance
        COUNTIF(shot_outcome = 'Goal') 
            - SUM(shot_statsbomb_xg)                           AS buts_vs_xG,

        -- Création
        COUNTIF(pass_goal_assist = TRUE)                       AS passes_decisives,

        -- Technique
        COUNTIF(dribble_outcome = 'Complete')                  AS dribbles_reussis,
        COUNTIF(pass_outcome IS NULL)                          AS passes_reussies,
        COUNTIF(pass_through_ball = TRUE)                      AS through_balls,

        -- Défense
        COUNTIF(interception_outcome IS NOT NULL)              AS interceptions,
        COUNTIF(duel_outcome = 'Won')                          AS duels_gagnes

    from joined
    where poste = 'Attack'
    group by player, poste, match_id
),

-- ✅ Agrégation finale : moyenne des matchs
kpi as (
    select
        player,
        poste,

        -- Volume
        COUNT(match_id)                                        AS matchs_joues,
        ROUND(AVG(minutes_jouees), 1)                          AS minutes_par_match,

        -- Finition
        ROUND(SUM(buts), 0)                                    AS buts,
        ROUND(AVG(buts), 2)                                    AS buts_par_match,
        ROUND(SUM(xG), 2)                                      AS xG_total,
        ROUND(AVG(xG), 2)                                      AS xG_par_match,
        ROUND(AVG(taux_conversion), 2)                         AS taux_conversion,
        ROUND(AVG(xG_par_tir), 2)                              AS xG_par_tir,
        ROUND(SUM(buts_vs_xG), 2)                              AS buts_vs_xG,

        -- Création
        ROUND(SUM(passes_decisives), 0)                        AS passes_decisives,
        ROUND(AVG(passes_decisives), 2)                        AS pd_par_match,
        ROUND(AVG(tirs_cadres), 2)                             AS tirs_cadres_par_match,

        -- Technique
        ROUND(SUM(dribbles_reussis), 0)                        AS dribbles_reussis,
        ROUND(AVG(dribbles_reussis), 2)                        AS dribbles_par_match,
        ROUND(AVG(passes_reussies), 2)                         AS passes_par_match,
        ROUND(AVG(through_balls), 2)                           AS pass_through_ball_per_match,

        -- Défense
        ROUND(SUM(interceptions), 0)                           AS interceptions,
        ROUND(AVG(interceptions), 2)                           AS interceptions_par_match,
        ROUND(SUM(duels_gagnes), 0)                            AS duels_gagnes,
        ROUND(AVG(duels_gagnes), 2)                            AS duels_par_match

    from per_match
    group by player, poste
    order by xG_par_match DESC
)

select * from kpi