/*WITH time_played AS (
    SELECT 
        player,
        SUM(minutes_played) AS total_minutes
    FROM  {{ ref('int_collective_kpis') }}
    GROUP BY player
), */

WITH stats_middle AS (
    SELECT
    player_id,
    player,
    poste,
    COUNT(DISTINCT match_id) AS nb_match,

    -- Volume et qualité de passes
    COUNTIF(event_type = "Pass" AND pass_outcome IS NULL) AS pass_complete,
    ROUND(COUNTIF(event_type = "Pass" AND pass_outcome IS NULL) / COUNT(DISTINCT match_id), 2) AS pass_complete_per_match,
    ROUND(COUNTIF(event_type = "Pass" AND pass_outcome IS NULL) / NULLIF(COUNTIF(event_type = "Pass"), 0), 2) AS taux_passes_reussies,

    -- Passes offensives
    COUNTIF(pass_through_ball = TRUE) AS pass_through_ball,
    ROUND(COUNTIF(pass_through_ball = TRUE) / COUNT(DISTINCT match_id), 2) AS pass_through_ball_per_match,
    COUNTIF(pass_goal_assist = TRUE) AS pass_goal_assist,
    ROUND(COUNTIF(pass_goal_assist = TRUE) / COUNT(DISTINCT match_id), 2) AS pass_goal_assist_per_match,
    COUNTIF(pass_shot_assist = TRUE) AS pass_shot_assist,
    ROUND(COUNTIF(pass_shot_assist = TRUE) / COUNT(DISTINCT match_id), 2) AS pass_shot_assist_per_match,
    COUNTIF(pass_cross = TRUE) AS pass_cross,
    ROUND(COUNTIF(pass_cross = TRUE) / COUNT(DISTINCT match_id), 2) AS pass_cross_per_match,

    -- Passes aériennes
    COUNTIF(pass_aerial_won = TRUE) AS pass_aerial_won,
    ROUND(COUNTIF(pass_aerial_won = TRUE) / COUNT(DISTINCT match_id), 2) AS pass_aerial_won_per_match,

    -- Résistance au pressing
    COUNTIF(event_type = "Pass" AND under_pressure = TRUE) AS pass_under_pressure,
    ROUND(COUNTIF(event_type = "Pass" AND under_pressure = TRUE) / COUNT(DISTINCT match_id), 2) AS pass_under_pressure_per_match,

        -- Attaque
    COUNTIF(shot_outcome = "Goal") AS goals,
    ROUND(COUNTIF(shot_outcome = "Goal") / COUNT(DISTINCT match_id), 2) AS goals_per_match,
    ROUND(SUM(shot_statsbomb_xg), 2) AS xg_total,
    ROUND(SUM(shot_statsbomb_xg) / COUNT(DISTINCT match_id), 2) AS xg_per_match,

    -- Défensif
    COUNTIF(interception_outcome IN ("Won", "Success In Play")) AS interceptions,
    ROUND(COUNTIF(interception_outcome IN ("Won", "Success In Play")) / COUNT(DISTINCT match_id), 2) AS interceptions_per_match,
    COUNTIF(duel_outcome = "Won") AS duel_win,
    ROUND(COUNTIF(duel_outcome = "Won") / COUNT(DISTINCT match_id), 2) AS duel_win_per_match,
    COUNTIF(clearance_aerial_won = TRUE) AS clearance_aerial_won,
    ROUND(COUNTIF(clearance_aerial_won = TRUE) / COUNT(DISTINCT match_id), 2) AS clearance_aerial_won_per_match,
    COUNTIF(ball_recovery_offensive = TRUE) AS ball_recovery_offensive,
    ROUND(COUNTIF(ball_recovery_offensive = TRUE) / COUNT(DISTINCT match_id), 2) AS ball_recovery_offensive_per_match,
    COUNTIF(event_type = 'Pressure' AND counterpress = TRUE) AS contre_pressing,
    ROUND(COUNTIF(event_type = 'Pressure' AND counterpress = TRUE) / COUNT(DISTINCT match_id), 2) AS contre_pressing_per_match,

    -- Influence
    COUNTIF(event_type = "Foul Won") AS foul_won,
    ROUND(COUNTIF(event_type = "Foul Won") / COUNT(DISTINCT match_id), 2) AS foul_won_per_match

FROM {{ ref('Event_euro_all') }}
WHERE poste = "Middle"
GROUP BY player_id, player, poste
),

-- Normalisation min-max de chaque métrique
normalized AS (
    SELECT
        player_id,
        player,
        poste,
        nb_match,

        -- Normalisation Attaque
        SAFE_DIVIDE(goals_per_match - MIN(goals_per_match) OVER(), MAX(goals_per_match) OVER() - MIN(goals_per_match) OVER()) AS n_goals,
        SAFE_DIVIDE(xg_per_match - MIN(xg_per_match) OVER(), MAX(xg_per_match) OVER() - MIN(xg_per_match) OVER()) AS n_xg,

        -- Normalisation Milieu
        SAFE_DIVIDE(taux_passes_reussies - MIN(taux_passes_reussies) OVER(), MAX(taux_passes_reussies) OVER() - MIN(taux_passes_reussies) OVER()) AS n_taux_passes,
        SAFE_DIVIDE(pass_through_ball_per_match - MIN(pass_through_ball_per_match) OVER(), MAX(pass_through_ball_per_match) OVER() - MIN(pass_through_ball_per_match) OVER()) AS n_pass_through_ball,
        SAFE_DIVIDE(pass_goal_assist_per_match - MIN(pass_goal_assist_per_match) OVER(), MAX(pass_goal_assist_per_match) OVER() - MIN(pass_goal_assist_per_match) OVER()) AS n_pass_goal_assist,
        SAFE_DIVIDE(pass_shot_assist_per_match - MIN(pass_shot_assist_per_match) OVER(), MAX(pass_shot_assist_per_match) OVER() - MIN(pass_shot_assist_per_match) OVER()) AS n_pass_shot_assist,
        SAFE_DIVIDE(pass_cross_per_match - MIN(pass_cross_per_match) OVER(), MAX(pass_cross_per_match) OVER() - MIN(pass_cross_per_match) OVER()) AS n_pass_cross,
        SAFE_DIVIDE(pass_aerial_won_per_match - MIN(pass_aerial_won_per_match) OVER(), MAX(pass_aerial_won_per_match) OVER() - MIN(pass_aerial_won_per_match) OVER()) AS n_pass_aerial,
        SAFE_DIVIDE(pass_under_pressure_per_match - MIN(pass_under_pressure_per_match) OVER(), MAX(pass_under_pressure_per_match) OVER() - MIN(pass_under_pressure_per_match) OVER()) AS n_pass_under_pressure,
        SAFE_DIVIDE(ball_recovery_offensive_per_match - MIN(ball_recovery_offensive_per_match) OVER(), MAX(ball_recovery_offensive_per_match) OVER() - MIN(ball_recovery_offensive_per_match) OVER()) AS n_ball_recovery,
        SAFE_DIVIDE(foul_won_per_match - MIN(foul_won_per_match) OVER(), MAX(foul_won_per_match) OVER() - MIN(foul_won_per_match) OVER()) AS n_foul_won,
        SAFE_DIVIDE(duel_win_per_match - MIN(duel_win_per_match) OVER(), MAX(duel_win_per_match) OVER() - MIN(duel_win_per_match) OVER()) AS n_duel_win,
        SAFE_DIVIDE(interceptions_per_match - MIN(interceptions_per_match) OVER(), MAX(interceptions_per_match) OVER() - MIN(interceptions_per_match) OVER()) AS n_interceptions,

        -- Normalisation Défense
        SAFE_DIVIDE(contre_pressing_per_match - MIN(contre_pressing_per_match) OVER(), MAX(contre_pressing_per_match) OVER() - MIN(contre_pressing_per_match) OVER()) AS n_contre_pressing,
        SAFE_DIVIDE(clearance_aerial_won_per_match - MIN(clearance_aerial_won_per_match) OVER(), MAX(clearance_aerial_won_per_match) OVER() - MIN(clearance_aerial_won_per_match) OVER()) AS n_clearance_aerial

    FROM stats_middle
),

scores AS (
    SELECT
        *,
        -- Score attaque (moyenne des métriques normalisées)
        ROUND((n_goals + n_xg) / 2, 4) AS score_attaque,

        -- Score milieu (moyenne des métriques normalisées)
        ROUND((n_taux_passes + n_pass_through_ball + n_pass_goal_assist + 
               n_pass_shot_assist + n_pass_cross + n_pass_aerial + 
               n_pass_under_pressure + n_ball_recovery + n_foul_won + 
               n_duel_win + n_interceptions) / 11, 4) AS score_milieu,

        -- Score défense (moyenne des métriques normalisées)
        ROUND((n_contre_pressing + n_clearance_aerial) / 2, 4) AS score_defense

    FROM normalized
)

-- Score final pondéré
SELECT
    s.player_id,
    s.player,
    s.poste,
    s.nb_match,
    /*t.total_minutes,*/
    /*ROUND(t.total_minutes / s.nb_match, 2) AS total_minutes_per_match,*/

    
    -- Scores
    sc.score_attaque,
    sc.score_milieu,
    sc.score_defense,
    ROUND((sc.score_attaque * 0.2) + (sc.score_milieu * 0.6) + (sc.score_defense * 0.2), 4) AS score_final,

    -- KPI Attaque
    s.goals,
    s.goals_per_match,
    s.xg_total,
    s.xg_per_match,

    -- KPI Milieu
    s.taux_passes_reussies,
    s.pass_through_ball_per_match,
    s.pass_goal_assist_per_match,
    s.pass_shot_assist_per_match,
    s.pass_cross_per_match,
    s.pass_aerial_won_per_match,
    s.pass_under_pressure_per_match,
    s.ball_recovery_offensive_per_match,
    s.foul_won_per_match,
    s.duel_win_per_match,
    s.interceptions_per_match,

    -- KPI Défense
    s.contre_pressing_per_match,
    s.clearance_aerial_won_per_match

FROM stats_middle AS s
LEFT JOIN scores AS sc
ON s.player = sc.player
/*LEFT JOIN time_played AS t
ON s.player = t.player*/
ORDER BY score_final DESC






