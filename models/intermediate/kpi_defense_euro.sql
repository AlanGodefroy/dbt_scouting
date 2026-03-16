WITH sq1 AS (
    SELECT
        evl.player_id,
        evl.player,
        pl.poste,
        evl.match_id,
        COUNTIF(evl.duel_outcome IN ('Success','Success in play','Won'))         AS nb_duel_outcome,
        COUNTIF(evl.interception_outcome IN ('Success','Success in play','Won')) AS nb_interception_outcome,
        (COUNT(evl.block_deflection) + COUNT(evl.block_save_block))             AS nb_block,
        COUNT(evl.clearance_aerial_won)                                         AS nb_clearance_aerial_won,
        COUNT(evl.under_pressure)                                               AS nb_under_pressure_succes,
        COUNTIF(evl.pass_outcome IS NULL AND evl.event_type = "Pass")           AS nb_pass_outcome_complete,
        COUNT(evl.pass_cross)                                                   AS nb_pass_cross,
        COUNT(evl.pass_goal_assist)                                             AS nb_pass_goal_assist,
        COUNT(evl.shot_statsbomb_xg)                                            AS nb_shot_statsbomb_xg,
        COUNTIF(evl.shot_outcome = "Goal")                                      AS nb_shot_outcome_goal
    FROM {{ ref('stg_Raw_data__Events_euro_2024') }} AS evl
    LEFT JOIN {{ ref('stg_Raw_data__Poste_euro_2024') }} AS pl
        ON evl.player = pl.player_name
    WHERE evl.player_id IS NOT NULL AND pl.poste LIKE 'Defense'
    GROUP BY 1, 2, 3, 4
),

normalized_kpis AS (
    SELECT
        player_id,
        player,
        poste,
        match_id,

        -- KPIs bruts
        nb_duel_outcome,
        nb_interception_outcome,
        nb_block,
        nb_clearance_aerial_won,
        nb_under_pressure_succes,
        nb_pass_outcome_complete,
        nb_pass_cross,
        nb_pass_goal_assist,
        nb_shot_statsbomb_xg,
        nb_shot_outcome_goal,

        -- KPIs normalisés par match
        COALESCE(SAFE_DIVIDE(nb_duel_outcome - MIN(nb_duel_outcome) OVER (PARTITION BY match_id),
            MAX(nb_duel_outcome) OVER (PARTITION BY match_id) - MIN(nb_duel_outcome) OVER (PARTITION BY match_id)), 0)
            AS nb_duel_outcome_norm,

        COALESCE(SAFE_DIVIDE(nb_interception_outcome - MIN(nb_interception_outcome) OVER (PARTITION BY match_id),
            MAX(nb_interception_outcome) OVER (PARTITION BY match_id) - MIN(nb_interception_outcome) OVER (PARTITION BY match_id)), 0)
            AS nb_interception_outcome_norm,

        COALESCE(SAFE_DIVIDE(nb_block - MIN(nb_block) OVER (PARTITION BY match_id),
            MAX(nb_block) OVER (PARTITION BY match_id) - MIN(nb_block) OVER (PARTITION BY match_id)), 0)
            AS nb_block_norm,

        COALESCE(SAFE_DIVIDE(nb_clearance_aerial_won - MIN(nb_clearance_aerial_won) OVER (PARTITION BY match_id),
            MAX(nb_clearance_aerial_won) OVER (PARTITION BY match_id) - MIN(nb_clearance_aerial_won) OVER (PARTITION BY match_id)), 0)
            AS nb_clearance_aerial_won_norm,

        COALESCE(SAFE_DIVIDE(nb_under_pressure_succes - MIN(nb_under_pressure_succes) OVER (PARTITION BY match_id),
            MAX(nb_under_pressure_succes) OVER (PARTITION BY match_id) - MIN(nb_under_pressure_succes) OVER (PARTITION BY match_id)), 0)
            AS nb_under_pressure_succes_norm,

        COALESCE(SAFE_DIVIDE(nb_pass_outcome_complete - MIN(nb_pass_outcome_complete) OVER (PARTITION BY match_id),
            MAX(nb_pass_outcome_complete) OVER (PARTITION BY match_id) - MIN(nb_pass_outcome_complete) OVER (PARTITION BY match_id)), 0)
            AS nb_pass_outcome_complete_norm,

        COALESCE(SAFE_DIVIDE(nb_pass_cross - MIN(nb_pass_cross) OVER (PARTITION BY match_id),
            MAX(nb_pass_cross) OVER (PARTITION BY match_id) - MIN(nb_pass_cross) OVER (PARTITION BY match_id)), 0)
            AS nb_pass_cross_norm,

        COALESCE(SAFE_DIVIDE(nb_pass_goal_assist - MIN(nb_pass_goal_assist) OVER (PARTITION BY match_id),
            MAX(nb_pass_goal_assist) OVER (PARTITION BY match_id) - MIN(nb_pass_goal_assist) OVER (PARTITION BY match_id)), 0)
            AS nb_pass_goal_assist_norm,

        COALESCE(SAFE_DIVIDE(nb_shot_statsbomb_xg - MIN(nb_shot_statsbomb_xg) OVER (PARTITION BY match_id),
            MAX(nb_shot_statsbomb_xg) OVER (PARTITION BY match_id) - MIN(nb_shot_statsbomb_xg) OVER (PARTITION BY match_id)), 0)
            AS nb_shot_statsbomb_xg_norm,

        COALESCE(SAFE_DIVIDE(nb_shot_outcome_goal - MIN(nb_shot_outcome_goal) OVER (PARTITION BY match_id),
            MAX(nb_shot_outcome_goal) OVER (PARTITION BY match_id) - MIN(nb_shot_outcome_goal) OVER (PARTITION BY match_id)), 0)
            AS nb_shot_outcome_goal_norm

    FROM sq1
),

scores AS (
    SELECT
        *,
        ROUND(0.6 * SAFE_DIVIDE(
            nb_duel_outcome_norm + nb_interception_outcome_norm + nb_block_norm
            + nb_clearance_aerial_won_norm + nb_under_pressure_succes_norm + nb_pass_outcome_complete_norm,
            6), 4) AS score_defense,

        ROUND(0.3 * SAFE_DIVIDE(
            nb_pass_cross_norm + nb_pass_goal_assist_norm,
            2), 4) AS score_middle,

        ROUND(0.1 * SAFE_DIVIDE(
            nb_shot_statsbomb_xg_norm + nb_shot_outcome_goal_norm,
            2), 4) AS score_attaque
    FROM normalized_kpis
),

scores_with_final AS (
    SELECT
        *,
        ROUND(score_defense + score_middle + score_attaque, 4) AS score_final
    FROM scores
),

sfinal AS (

SELECT
    player_id,
    player,
    poste,

    COUNT(match_id)                AS nb_matches,
    SUM(nb_under_pressure_succes)  AS nb_under_pressure_succes,
    SUM(nb_pass_outcome_complete)  AS nb_pass_outcome_complete,
    SUM(nb_duel_outcome)           AS nb_duel_outcome,
    SUM(nb_interception_outcome)   AS nb_interception_outcome,
    SUM(nb_block)                  AS nb_block,
    SUM(nb_clearance_aerial_won)   AS nb_clearance_aerial_won,
    SUM(nb_pass_cross)             AS nb_pass_cross,
    SUM(nb_pass_goal_assist)       AS nb_pass_goal,
    SUM(nb_shot_statsbomb_xg)      AS nb_shot_xg,
    SUM(nb_shot_outcome_goal)      AS nb_shot_outcome_goal,

    ROUND(AVG(score_defense), 2)   AS score_defense,
    ROUND(AVG(score_middle), 2)    AS score_middle,
    ROUND(AVG(score_attaque), 2)   AS score_attack,
    ROUND(AVG(score_final), 2)     AS score_final

FROM scores_with_final
GROUP BY 1, 2, 3
)

SELECT
    sf.*,
    europ.team,
    europ.market_value,
    europ.current_club_name,
    europ.age
FROM sfinal AS sf
LEFT JOIN {{ ref('stg_Raw_data__euro_24_global_data_players') }} AS europ
ON sf.player_id = europ.player_id
WHERE europ.market_value <=30000000 
    AND sf.nb_matches > 2 
    AND europ.age < 27
ORDER BY sf.score_final DESC