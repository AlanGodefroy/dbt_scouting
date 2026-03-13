WITH sq1 AS (
    SELECT
        evl.player_id,
        evl.player,
        pl.poste,
        evl.match_id,
        COUNTIF(evl.duel_outcome IN ('Success','Success in play','Won'))          AS nb_duel_outcome,
        COUNTIF(evl.interception_outcome IN ('Success','Success in play','Won'))  AS nb_interception_outcome,
        (COUNT(evl.block_deflection) + COUNT(evl.block_save_block))              AS nb_block,
        COUNT(evl.clearance_aerial_won)                                          AS nb_clearance_aerial_won,
        COUNT(evl.under_pressure)                                                AS nb_under_pressure_succes,
        COUNTIF(evl.pass_outcome IS NULL AND evl.event_type = "Pass")            AS nb_pass_outcome_complete,
        COUNT(evl.pass_cross)                                                    AS nb_pass_cross,
        COUNT(evl.pass_goal_assist)                                              AS nb_pass_goal_assist,
        COUNT(evl.shot_statsbomb_xg)                                             AS nb_shot_statsbomb_xg,
        COUNTIF(evl.shot_outcome = "Goal")                                       AS nb_shot_outcome_goal
    FROM {{ ref('stg_Raw_data__Events_Leverkusen') }} AS evl
    LEFT JOIN {{ ref('stg_Raw_data__Poste_Leverkusen') }} AS pl
        ON evl.player = pl.player_name
    WHERE evl.player_id IS NOT NULL AND pl.poste LIKE 'Defense'
    GROUP BY 1, 2, 3, 4
),

minutes_join AS (
    SELECT
        sq1.player_id,
        sq1.match_id,
        SUM(coll.minutes_played) AS total_min
    FROM sq1
    LEFT JOIN {{ ref('int_collective_kpis') }} coll
        ON  sq1.player_id = coll.player_id
        AND sq1.match_id  = coll.match_id
    GROUP BY 1, 2
),

-- Normalisation min-max de chaque KPI sur l'ensemble des lignes (tous joueurs, tous matchs)
normalized_kpis AS (
    SELECT
        sq1.player_id,
        sq1.player,
        sq1.poste,
        sq1.match_id,
        mj.total_min,

        -- KPIs bruts (conservés pour l'agrégation finale)
        sq1.nb_duel_outcome,
        sq1.nb_interception_outcome,
        sq1.nb_block,
        sq1.nb_clearance_aerial_won,
        sq1.nb_under_pressure_succes,
        sq1.nb_pass_outcome_complete,
        sq1.nb_pass_cross,
        sq1.nb_pass_goal_assist,
        sq1.nb_shot_statsbomb_xg,
        sq1.nb_shot_outcome_goal,

        -- KPIs normalisés individuellement
        ROUND(SAFE_DIVIDE(sq1.nb_duel_outcome - MIN(sq1.nb_duel_outcome) OVER (),
            NULLIF(MAX(sq1.nb_duel_outcome) OVER () - MIN(sq1.nb_duel_outcome) OVER (), 0)), 4)
            AS nb_duel_outcome_norm,

        ROUND(SAFE_DIVIDE(sq1.nb_interception_outcome - MIN(sq1.nb_interception_outcome) OVER (),
            NULLIF(MAX(sq1.nb_interception_outcome) OVER () - MIN(sq1.nb_interception_outcome) OVER (), 0)), 4)
            AS nb_interception_outcome_norm,

        ROUND(SAFE_DIVIDE(sq1.nb_block - MIN(sq1.nb_block) OVER (),
            NULLIF(MAX(sq1.nb_block) OVER () - MIN(sq1.nb_block) OVER (), 0)), 4)
            AS nb_block_norm,

        ROUND(SAFE_DIVIDE(sq1.nb_clearance_aerial_won - MIN(sq1.nb_clearance_aerial_won) OVER (),
            NULLIF(MAX(sq1.nb_clearance_aerial_won) OVER () - MIN(sq1.nb_clearance_aerial_won) OVER (), 0)), 4)
            AS nb_clearance_aerial_won_norm,

        ROUND(SAFE_DIVIDE(sq1.nb_under_pressure_succes - MIN(sq1.nb_under_pressure_succes) OVER (),
            NULLIF(MAX(sq1.nb_under_pressure_succes) OVER () - MIN(sq1.nb_under_pressure_succes) OVER (), 0)), 4)
            AS nb_under_pressure_succes_norm,

        ROUND(SAFE_DIVIDE(sq1.nb_pass_outcome_complete - MIN(sq1.nb_pass_outcome_complete) OVER (),
            NULLIF(MAX(sq1.nb_pass_outcome_complete) OVER () - MIN(sq1.nb_pass_outcome_complete) OVER (), 0)), 4)
            AS nb_pass_outcome_complete_norm,

        ROUND(SAFE_DIVIDE(sq1.nb_pass_cross - MIN(sq1.nb_pass_cross) OVER (),
            NULLIF(MAX(sq1.nb_pass_cross) OVER () - MIN(sq1.nb_pass_cross) OVER (), 0)), 4)
            AS nb_pass_cross_norm,

        ROUND(SAFE_DIVIDE(sq1.nb_pass_goal_assist - MIN(sq1.nb_pass_goal_assist) OVER (),
            NULLIF(MAX(sq1.nb_pass_goal_assist) OVER () - MIN(sq1.nb_pass_goal_assist) OVER (), 0)), 4)
            AS nb_pass_goal_assist_norm,

        ROUND(SAFE_DIVIDE(sq1.nb_shot_statsbomb_xg - MIN(sq1.nb_shot_statsbomb_xg) OVER (),
            NULLIF(MAX(sq1.nb_shot_statsbomb_xg) OVER () - MIN(sq1.nb_shot_statsbomb_xg) OVER (), 0)), 4)
            AS nb_shot_statsbomb_xg_norm,

        ROUND(SAFE_DIVIDE(sq1.nb_shot_outcome_goal - MIN(sq1.nb_shot_outcome_goal) OVER (),
            NULLIF(MAX(sq1.nb_shot_outcome_goal) OVER () - MIN(sq1.nb_shot_outcome_goal) OVER (), 0)), 4)
            AS nb_shot_outcome_goal_norm

    FROM sq1
    LEFT JOIN minutes_join mj
        ON  sq1.player_id = mj.player_id
        AND sq1.match_id  = mj.match_id
),

-- Calcul des scores à partir des KPIs normalisés
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
)

SELECT
    player_id,
    player,
    poste,

    COUNT(match_id)                         AS nb_matches,
    SUM(total_min)                          AS total_minutes_played,
    SUM(nb_under_pressure_succes)           AS nb_under_pressure_succes,
    SUM(nb_pass_outcome_complete)           AS nb_pass_outcome_complete,
    SUM(nb_duel_outcome)                    AS nb_duel_outcome,
    SUM(nb_interception_outcome)            AS nb_interception_outcome,
    SUM(nb_block)                           AS nb_block,
    SUM(nb_clearance_aerial_won)            AS nb_clearance_aerial_won,
    SUM(nb_pass_cross)                      AS nb_pass_cross,
    SUM(nb_pass_goal_assist)                AS nb_pass_goal,
    SUM(nb_shot_statsbomb_xg)               AS nb_shot_xg,
    SUM(nb_shot_outcome_goal)               AS nb_shot_outcome_goal,

    ROUND(AVG(score_defense), 2)            AS score_defense,
    ROUND(AVG(score_middle), 2)             AS score_middle,
    ROUND(AVG(score_attaque), 2)            AS score_attack,
    ROUND(AVG(score_final), 2)              AS score_final

FROM scores_with_final
GROUP BY 1, 2, 3