WITH sq1 AS (
    SELECT
        CAST(evl.player_id AS INT64) AS player_id,
        evl.player,
        pl.poste,
        evl.match_id,
        COUNTIF(evl.duel_outcome IN ('Success','Success in play','Won')) as nb_duel_outcome,
        COUNTIF(evl.interception_outcome IN ('Success','Success in play','Won')) as nb_interception_outcome,
        (COUNT(evl.block_deflection) + COUNT(evl.block_save_block)) as nb_block,
        COUNT(evl.clearance_aerial_won) as nb_clearance_aerial_won,
        COUNT(evl.under_pressure) as nb_under_pressure_succes,
        COUNTIF(evl.pass_outcome IS NULL AND evl.event_type = "Pass") as nb_pass_outcome_complete,
        COUNT(evl.pass_cross) as nb_pass_cross,
        COUNT(evl.pass_goal_assist) as nb_pass_goal_assist,
        COUNT(evl.shot_statsbomb_xg) as nb_shot_statsbomb_xg,
        COUNTIF(evl.shot_outcome = "Goal") as nb_shot_outcome_goal
    FROM {{ ref('stg_Raw_data__Events_euro_2024') }} AS evl
    LEFT JOIN {{ ref('stg_Raw_data__Poste_euro_2024') }} AS pl
        ON evl.player = pl.player_name
    WHERE evl.player_id IS NOT NULL AND pl.poste LIKE 'Defense'
    GROUP BY 1,2,3,4
),

minutes_join AS (

SELECT 
    sq1.player_id,
    sq1.match_id, 
    SUM(coll.minutes_played) as total_min 
FROM sq1
LEFT JOIN {{ ref('int_euro_kpis') }} coll
    ON sq1.player_id = coll.player_id
    AND sq1.match_id = coll.match_id
GROUP BY 1,2
),

score_inter as (

SELECT
        sq1.player_id,       
        sq1.player,
        sq1.poste,
        sq1.match_id,
        coll.total_min,
        sq1.nb_under_pressure_succes,
        sq1.nb_pass_outcome_complete,
        sq1.nb_duel_outcome,
        sq1.nb_interception_outcome,
        sq1.nb_block,
        sq1.nb_clearance_aerial_won,
        sq1.nb_pass_cross,
        sq1.nb_pass_goal_assist,
        sq1.nb_shot_statsbomb_xg,
        sq1.nb_shot_outcome_goal,
        ROUND((0.6 * SAFE_DIVIDE(sq1.nb_duel_outcome + sq1.nb_interception_outcome + sq1.nb_block + sq1.nb_clearance_aerial_won + sq1.nb_under_pressure_succes + sq1.nb_pass_outcome_complete, 6)),2) AS score_defense,
        ROUND((0.3 * SAFE_DIVIDE(sq1.nb_pass_cross + sq1.nb_pass_goal_assist, 2)),2) AS score_middle,
        ROUND((0.1 * SAFE_DIVIDE(sq1.nb_shot_statsbomb_xg + sq1.nb_shot_outcome_goal, 2)),2) AS score_attaque
FROM sq1
LEFT JOIN minutes_join as coll
    ON sq1.player_id = coll.player_id
    AND sq1.match_id = coll.match_id
),

score_final as (

SELECT  
        sci.player_id,
        sci.player,
        sci.poste,
        sci.match_id,
        sci.total_min,
        sci.nb_under_pressure_succes,
        sci.nb_pass_outcome_complete,
        sci.nb_duel_outcome,
        sci.nb_interception_outcome,
        sci.nb_block,
        sci.nb_clearance_aerial_won,
        sci.nb_pass_cross,
        sci.nb_pass_goal_assist,
        sci.nb_shot_statsbomb_xg,
        sci.nb_shot_outcome_goal,
        sci.score_defense,
        sci.score_middle,
        sci.score_attaque,
        ROUND(score_defense + score_middle + score_attaque, 4) AS score_final

FROM score_inter as sci
),


-- normalisation min-max par match sur l'ensemble des joueurs
normalized AS (
    SELECT
        sf.*,
        ROUND(SAFE_DIVIDE(sf.score_defense - MIN(sf.score_defense) OVER(),
            NULLIF(MAX(sf.score_defense) OVER() - MIN(sf.score_defense) OVER(), 0)), 4) AS score_defense_norm,

        ROUND(SAFE_DIVIDE(sf.score_middle - MIN(sf.score_middle) OVER(),
            NULLIF(MAX(sf.score_middle) OVER() - MIN(sf.score_middle) OVER(), 0)), 4) AS score_middle_norm,

        ROUND(SAFE_DIVIDE(sf.score_attaque - MIN(sf.score_attaque) OVER(),
            NULLIF(MAX(sf.score_attaque) OVER() - MIN(sf.score_attaque) OVER(), 0)), 4) AS score_attack_norm,

        ROUND(SAFE_DIVIDE(sf.score_final - MIN(sf.score_final) OVER(),
            NULLIF(MAX(sf.score_final) OVER() - MIN(sf.score_final) OVER(), 0)), 4) AS score_final_norm
    FROM score_final as sf
)

SELECT 
    n.player_id,
    n.player,
    n.poste,
    
    COUNT(n.match_id) as nb_matches,
    SUM(n.total_min) as total_minutes_played,
    SUM(n.nb_under_pressure_succes) as nb_under_pressure_succes,
    SUM(n.nb_pass_outcome_complete) as nb_pass_outcome_complete,
    SUM(n.nb_duel_outcome) as nb_duel_outcome,
    SUM(n.nb_interception_outcome) as nb_interception_outcome,
    SUM(n.nb_block) as nb_block,
    SUM(n.nb_clearance_aerial_won) as nb_clearance_aerial_won,
    SUM(n.nb_pass_cross) as nb_pass_cross,
    SUM(n.nb_pass_goal_assist) as nb_pass_goal,
    SUM(n.nb_shot_statsbomb_xg) as nb_pass_shot_xg,
    SUM(n.nb_shot_outcome_goal) as nb_shot_outcome_goal,

    ROUND(AVG(n.score_defense_norm),2) as score_defense,
    ROUND(AVG(n.score_middle_norm),2) as score_middle,
    ROUND(AVG(n.score_attack_norm),2) as score_attack,
    ROUND(AVG(n.score_final_norm),2) as score_final

FROM normalized n
GROUP BY 1,2,3



