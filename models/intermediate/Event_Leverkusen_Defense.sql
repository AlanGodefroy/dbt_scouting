WITH sq1 AS (
    SELECT
        evl.player_id,
        evl.player,
        pl.poste,
        COUNT(DISTINCT(evl.match_id)) AS nb_matches,
        COUNTIF(evl.duel_outcome IN ('Success','Success in play','Won')) as nb_duel_outcome,
        COUNTIF(evl.interception_outcome IN ('Success','Success in play','Won')) as nb_interception_outcome,
        (COUNT(evl.block_deflection) + COUNT(evl.block_save_block)) as nb_block,
        COUNT(evl.clearance_aerial_won) as nb_clearance_aerial_won,
        COUNT(evl.under_pressure) as nb_under_pressure_succes,
        COUNTIF(evl.pass_outcome IS NULL AND evl.event_type = "Pass") as nb_pass_outcome_complete,
        COUNT(evl.pass_cross) as pass_cross,
        COUNT(evl.pass_goal_assist) as pass_goal_assist,
        COUNT(evl.shot_statsbomb_xg) as shot_statsbomb_xg,
        COUNTIF(evl.shot_outcome = "Goal") as shot_outcome_goal,
    FROM {{ ref('stg_Raw_data__Events_Leverkusen') }} AS evl
    LEFT JOIN {{ ref('stg_Raw_data__Poste_Leverkusen') }} AS pl
        ON evl.player = pl.player_name
    WHERE evl.player_id IS NOT NULL AND pl.poste LIKE 'Defense'
    GROUP BY 1,2,3
),

minutes_join AS (
    SELECT 
        player_id, 
        SUM(minutes_played) as total_min 
    FROM {{ ref('int_collective_kpis') }} 
    GROUP BY 1
),

final AS (

SELECT 
    sq1.*,
    ROUND(coll.total_min, 2) as minutes_played_total,
    ROUND(SAFE_DIVIDE(coll.total_min, sq1.nb_matches), 2) as minutes_played_per_matches,
    ROUND(SAFE_DIVIDE(sq1.nb_under_pressure_succes, sq1.nb_matches), 2) as nb_under_pressure_succes_per_matches,
    ROUND(SAFE_DIVIDE(sq1.nb_pass_outcome_complete, sq1.nb_matches), 2) as nb_pass_outcome_complete_per_matches,
    ROUND(SAFE_DIVIDE(sq1.nb_duel_outcome, sq1.nb_matches), 2) as duel_outcome_per_matches,
    ROUND(SAFE_DIVIDE(sq1.nb_interception_outcome, sq1.nb_matches), 2) as interception_outcome_per_matches,
    ROUND(SAFE_DIVIDE(sq1.nb_block, sq1.nb_matches), 2) as block_per_matches,
    ROUND(SAFE_DIVIDE(sq1.nb_clearance_aerial_won, sq1.nb_matches), 2) as clearance_aerial_won_per_matches,
    ROUND(SAFE_DIVIDE(sq1.pass_cross, sq1.nb_matches), 2) as pass_cross_per_matches,
    ROUND(SAFE_DIVIDE(sq1.pass_goal_assist, sq1.nb_matches), 2) as pass_goal_assist_per_matches,
    ROUND(SAFE_DIVIDE(sq1.shot_statsbomb_xg, sq1.nb_matches), 2) as shot_statsbomb_xg_per_matches,
    ROUND(SAFE_DIVIDE(sq1.shot_outcome_goal, sq1.nb_matches), 2) as shot_outcome_goal_per_matches
FROM sq1
LEFT JOIN minutes_join as coll
    ON sq1.player_id = coll.player_id
),

normalized AS (
    SELECT
        *,

        SAFE_DIVIDE(duel_outcome_per_matches - MIN(duel_outcome_per_matches) OVER(),        
            NULLIF(MAX(duel_outcome_per_matches) OVER() - MIN(duel_outcome_per_matches) OVER(), 0)) as duel_norm,

        SAFE_DIVIDE(interception_outcome_per_matches - MIN(interception_outcome_per_matches) OVER(),
            NULLIF(MAX(interception_outcome_per_matches) OVER() - MIN(interception_outcome_per_matches) OVER(), 0)) as interception_norm,

        SAFE_DIVIDE(block_per_matches - MIN(block_per_matches) OVER(),
            NULLIF(MAX(block_per_matches) OVER() - MIN(block_per_matches) OVER(), 0)) as block_norm,

        SAFE_DIVIDE(clearance_aerial_won_per_matches - MIN(clearance_aerial_won_per_matches) OVER(),
            NULLIF(MAX(clearance_aerial_won_per_matches) OVER() - MIN(clearance_aerial_won_per_matches) OVER(), 0)) as clearance_norm,

        SAFE_DIVIDE(nb_under_pressure_succes_per_matches - MIN(nb_under_pressure_succes_per_matches) OVER(),
            NULLIF(MAX(nb_under_pressure_succes_per_matches) OVER() - MIN(nb_under_pressure_succes_per_matches) OVER(), 0)) as under_pressure_norm,

        SAFE_DIVIDE(nb_pass_outcome_complete_per_matches - MIN(nb_pass_outcome_complete_per_matches) OVER(),
            NULLIF(MAX(nb_pass_outcome_complete_per_matches) OVER() - MIN(nb_pass_outcome_complete_per_matches) OVER(), 0)) as pass_complete_norm,

        SAFE_DIVIDE(pass_cross_per_matches - MIN(pass_cross_per_matches) OVER(),
            NULLIF(MAX(pass_cross_per_matches) OVER() - MIN(pass_cross_per_matches) OVER(), 0)) as pass_cross_norm,

        SAFE_DIVIDE(pass_goal_assist_per_matches - MIN(pass_goal_assist_per_matches) OVER(),
            NULLIF(MAX(pass_goal_assist_per_matches) OVER() - MIN(pass_goal_assist_per_matches) OVER(), 0)) as assist_norm,

        SAFE_DIVIDE(shot_statsbomb_xg_per_matches - MIN(shot_statsbomb_xg_per_matches) OVER(),
            NULLIF(MAX(shot_statsbomb_xg_per_matches) OVER() - MIN(shot_statsbomb_xg_per_matches) OVER(), 0)) as xg_norm,

        SAFE_DIVIDE(shot_outcome_goal_per_matches - MIN(shot_outcome_goal_per_matches) OVER(),
            NULLIF(MAX(shot_outcome_goal_per_matches) OVER() - MIN(shot_outcome_goal_per_matches) OVER(), 0)) as goal_norm
    FROM final
), 

score_inter as (

SELECT
    player_id,
    player,
    poste,
    nb_matches,
    minutes_played_total,
    minutes_played_per_matches,
    nb_under_pressure_succes_per_matches,
    nb_pass_outcome_complete_per_matches,
    duel_outcome_per_matches,
    interception_outcome_per_matches,
    block_per_matches,
    clearance_aerial_won_per_matches,
    pass_cross_per_matches,
    pass_goal_assist_per_matches,
    shot_statsbomb_xg_per_matches,
    shot_outcome_goal_per_matches,
    (0.6 * SAFE_DIVIDE(duel_norm + interception_norm + block_norm + clearance_norm + under_pressure_norm + pass_complete_norm, 6)) as score_defense,
    (0.3 * SAFE_DIVIDE(pass_cross_norm + assist_norm, 2)) as score_middle,
    (0.1 * SAFE_DIVIDE(xg_norm + goal_norm,2)) as score_attaque

FROM normalized
)

SELECT 
    *,
    ROUND(score_defense + score_middle + score_attaque,4) as score_final
FROM score_inter
ORDER BY score_final DESC
