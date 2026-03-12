

WITH stats_middle AS (
    SELECT
    match_id,
    player_id,
    player,
    poste,

    -- Volume et qualité de passes
    COUNTIF(event_type = "Pass" AND pass_outcome IS NULL) AS pass_complete,
    ROUND(COUNTIF(event_type = "Pass" AND pass_outcome IS NULL) / NULLIF(COUNTIF(event_type = "Pass"), 0), 2) AS taux_passes_reussies,

    -- Passes offensives
    COUNTIF(pass_through_ball = TRUE) AS pass_through_ball,
    COUNTIF(pass_goal_assist = TRUE) AS pass_goal_assist,
    COUNTIF(pass_shot_assist = TRUE) AS pass_shot_assist,
    COUNTIF(pass_cross = TRUE) AS pass_cross,

    -- Passes aériennes
    COUNTIF(pass_aerial_won = TRUE) AS pass_aerial_won,

    -- Résistance au pressing
    COUNTIF(event_type = "Pass" AND under_pressure = TRUE) AS pass_under_pressure,

        -- Attaque
    COUNTIF(shot_outcome = "Goal") AS goals,
    ROUND(SUM(shot_statsbomb_xg), 2) AS xg_total,

    -- Défensif
    COUNTIF(interception_outcome IN ("Won", "Success In Play")) AS interceptions,
    COUNTIF(duel_outcome = "Won") AS duel_win,
    COUNTIF(clearance_aerial_won = TRUE) AS clearance_aerial_won,
    COUNTIF(ball_recovery_offensive = TRUE) AS ball_recovery_offensive,
    COUNTIF(event_type = 'Pressure' AND counterpress = TRUE) AS contre_pressing,

    -- Influence
    COUNTIF(event_type = "Foul Won") AS foul_won

FROM {{ ref('Event_euro_all') }}
WHERE poste = "Middle"
GROUP BY match_id, player_id, player, poste
HAVING COUNT(*) > 10
),

-- Normalisation min-max de chaque métrique
normalized AS (
    SELECT
        match_id,
        player_id,
        player,
        poste,

        -- Attaque
        COALESCE(SAFE_DIVIDE(goals - MIN(goals) OVER(PARTITION BY match_id),
            MAX(goals) OVER(PARTITION BY match_id) - MIN(goals) OVER(PARTITION BY match_id)), 0) AS n_goals,
        COALESCE(SAFE_DIVIDE(xg_total - MIN(xg_total) OVER(PARTITION BY match_id),
            MAX(xg_total) OVER(PARTITION BY match_id) - MIN(xg_total) OVER(PARTITION BY match_id)), 0) AS n_xg,

        -- Milieu
        COALESCE(SAFE_DIVIDE(taux_passes_reussies - MIN(taux_passes_reussies) OVER(PARTITION BY match_id),
            MAX(taux_passes_reussies) OVER(PARTITION BY match_id) - MIN(taux_passes_reussies) OVER(PARTITION BY match_id)), 0) AS n_taux_passes,
        COALESCE(SAFE_DIVIDE(pass_through_ball - MIN(pass_through_ball) OVER(PARTITION BY match_id),
            MAX(pass_through_ball) OVER(PARTITION BY match_id) - MIN(pass_through_ball) OVER(PARTITION BY match_id)), 0) AS n_pass_through_ball,
        COALESCE(SAFE_DIVIDE(pass_goal_assist - MIN(pass_goal_assist) OVER(PARTITION BY match_id),
            MAX(pass_goal_assist) OVER(PARTITION BY match_id) - MIN(pass_goal_assist) OVER(PARTITION BY match_id)), 0) AS n_pass_goal_assist,
        COALESCE(SAFE_DIVIDE(pass_shot_assist - MIN(pass_shot_assist) OVER(PARTITION BY match_id),
            MAX(pass_shot_assist) OVER(PARTITION BY match_id) - MIN(pass_shot_assist) OVER(PARTITION BY match_id)), 0) AS n_pass_shot_assist,
        COALESCE(SAFE_DIVIDE(pass_cross - MIN(pass_cross) OVER(PARTITION BY match_id),
            MAX(pass_cross) OVER(PARTITION BY match_id) - MIN(pass_cross) OVER(PARTITION BY match_id)), 0) AS n_pass_cross,
        COALESCE(SAFE_DIVIDE(pass_aerial_won - MIN(pass_aerial_won) OVER(PARTITION BY match_id),
            MAX(pass_aerial_won) OVER(PARTITION BY match_id) - MIN(pass_aerial_won) OVER(PARTITION BY match_id)), 0) AS n_pass_aerial,
        COALESCE(SAFE_DIVIDE(pass_under_pressure - MIN(pass_under_pressure) OVER(PARTITION BY match_id),
            MAX(pass_under_pressure) OVER(PARTITION BY match_id) - MIN(pass_under_pressure) OVER(PARTITION BY match_id)), 0) AS n_pass_under_pressure,
        COALESCE(SAFE_DIVIDE(ball_recovery_offensive - MIN(ball_recovery_offensive) OVER(PARTITION BY match_id),
            MAX(ball_recovery_offensive) OVER(PARTITION BY match_id) - MIN(ball_recovery_offensive) OVER(PARTITION BY match_id)), 0) AS n_ball_recovery,
        COALESCE(SAFE_DIVIDE(foul_won - MIN(foul_won) OVER(PARTITION BY match_id),
            MAX(foul_won) OVER(PARTITION BY match_id) - MIN(foul_won) OVER(PARTITION BY match_id)), 0) AS n_foul_won,
        COALESCE(SAFE_DIVIDE(duel_win - MIN(duel_win) OVER(PARTITION BY match_id),
            MAX(duel_win) OVER(PARTITION BY match_id) - MIN(duel_win) OVER(PARTITION BY match_id)), 0) AS n_duel_win,
        COALESCE(SAFE_DIVIDE(interceptions - MIN(interceptions) OVER(PARTITION BY match_id),
            MAX(interceptions) OVER(PARTITION BY match_id) - MIN(interceptions) OVER(PARTITION BY match_id)), 0) AS n_interceptions,

        -- Défense
        COALESCE(SAFE_DIVIDE(contre_pressing - MIN(contre_pressing) OVER(PARTITION BY match_id),
            MAX(contre_pressing) OVER(PARTITION BY match_id) - MIN(contre_pressing) OVER(PARTITION BY match_id)), 0) AS n_contre_pressing,
        COALESCE(SAFE_DIVIDE(clearance_aerial_won - MIN(clearance_aerial_won) OVER(PARTITION BY match_id),
            MAX(clearance_aerial_won) OVER(PARTITION BY match_id) - MIN(clearance_aerial_won) OVER(PARTITION BY match_id)), 0) AS n_clearance_aerial
    FROM stats_middle
),

scores AS (
    SELECT
       player_id,
        player,
        poste,
        match_id,
        -- Score attaque (moyenne des métriques normalisées)
        ROUND((n_goals + n_xg) / 2, 4) AS score_attaque_match,

        -- Score milieu (moyenne des métriques normalisées)
        ROUND((n_taux_passes + n_pass_through_ball + n_pass_goal_assist +
               n_pass_shot_assist + n_pass_cross + n_pass_aerial +
               n_pass_under_pressure + n_ball_recovery + n_foul_won +
               n_duel_win + n_interceptions) / 11, 4) AS score_milieu_match,

        -- Score défense (moyenne des métriques normalisées)
        ROUND((n_contre_pressing + n_clearance_aerial) / 2, 4) AS score_defense_match
    FROM normalized
)

-- Score final pondéré
SELECT
    sm.match_id,
    sm.player_id,
    sm.player,
    sm.poste,

    -- KPI bruts du match
    sm.goals,
    sm.xg_total,
    sm.taux_passes_reussies,
    sm.pass_through_ball,
    sm.pass_goal_assist,
    sm.pass_shot_assist,
    sm.pass_cross,
    sm.pass_aerial_won,
    sm.pass_under_pressure,
    sm.ball_recovery_offensive,
    sm.foul_won,
    sm.duel_win,
    sm.interceptions,
    sm.contre_pressing,
    sm.clearance_aerial_won,

    -- Scores par match
    sc.score_attaque_match,
    sc.score_milieu_match,
    sc.score_defense_match,
    ROUND(
        (sc.score_attaque_match * 0.2) +
        (sc.score_milieu_match * 0.6) +
        (sc.score_defense_match * 0.2)
    , 4) AS score_total_match

FROM stats_middle AS sm
LEFT JOIN scores AS sc
    ON sm.player = sc.player AND sm.match_id = sc.match_id
ORDER BY sm.player, sm.match_id