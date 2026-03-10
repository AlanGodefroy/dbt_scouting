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

    -- Défensif
    COUNTIF(interception_outcome IN ("Won", "Success In Play")) AS interceptions,
    ROUND(COUNTIF(interception_outcome IN ("Won", "Success In Play")) / COUNT(DISTINCT match_id), 2) AS interceptions_per_match,
    COUNTIF(duel_outcome = "Won") AS duel_win,
    ROUND(COUNTIF(duel_outcome = "Won") / COUNT(DISTINCT match_id), 2) AS duel_win_per_match,
    COUNTIF(ball_recovery_offensive = TRUE) AS ball_recovery_offensive,
    ROUND(COUNTIF(ball_recovery_offensive = TRUE) / COUNT(DISTINCT match_id), 2) AS ball_recovery_offensive_per_match,

    -- Influence
    COUNTIF(event_type = "Foul Won") AS foul_won,
    ROUND(COUNTIF(event_type = "Foul Won") / COUNT(DISTINCT match_id), 2) AS foul_won_per_match

FROM {{ ref("Event_Leverkusen_all") }}
WHERE poste = "Middle"
GROUP BY player_id, player, poste

