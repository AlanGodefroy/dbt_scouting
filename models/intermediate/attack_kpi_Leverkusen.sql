WITH sq1 AS (
    SELECT
        evl.player_id,
        evl.player,
        pl.poste,
        evl.match_id,

        COUNTIF(evl.shot_outcome = 'Goal')                                          AS nb_goals,
        COUNTIF(evl.play_pattern = 'From Counter')                                  AS nb_contre_attaques,
        COUNTIF(evl.shot_outcome IN ('Goal', 'Saved', 'Saved to Post'))             AS nb_tirs_cadres,
        SAFE_DIVIDE(
            COUNTIF(evl.shot_outcome = 'Goal'),
            COUNTIF(evl.shot_outcome IN ('Goal', 'Saved', 'Saved to Post', 'Off T'))
        )                                                                            AS taux_conversion,
        COUNT(evl.pass_goal_assist)                                                 AS nb_pass_goal_assist,
        COUNTIF(evl.dribble_outcome = 'Complete')                                   AS nb_dribbles,
        COUNTIF(evl.pass_outcome IS NULL AND evl.event_type = 'Pass')               AS nb_pass_complete,
        COUNT(evl.pass_through_ball)                                                AS nb_pass_through_ball,
        COUNTIF(evl.interception_outcome IN ('Success','Success in play','Won'))    AS nb_interceptions,
        COUNTIF(evl.duel_outcome IN ('Success','Success in play','Won'))            AS nb_duel_win

    FROM {{ ref('stg_Raw_data__Events_Leverkusen') }} AS evl
    LEFT JOIN {{ ref('stg_Raw_data__Poste_Leverkusen') }} AS pl
        ON evl.player = pl.player_name
    WHERE evl.player_id IS NOT NULL AND pl.poste LIKE 'Attack'
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

normalized_kpis AS (
    SELECT
        sq1.player_id,
        sq1.player,
        sq1.poste,
        sq1.match_id,
        mj.total_min,

        -- KPIs bruts
        sq1.nb_goals,
        sq1.nb_contre_attaques,
        sq1.nb_tirs_cadres,
        sq1.taux_conversion,
        sq1.nb_pass_goal_assist,
        sq1.nb_dribbles,
        sq1.nb_pass_complete,
        sq1.nb_pass_through_ball,
        sq1.nb_interceptions,
        sq1.nb_duel_win,

        -- KPIs normalisés par match
        COALESCE(SAFE_DIVIDE(sq1.nb_goals - MIN(sq1.nb_goals) OVER (PARTITION BY sq1.match_id),
            MAX(sq1.nb_goals) OVER (PARTITION BY sq1.match_id) - MIN(sq1.nb_goals) OVER (PARTITION BY sq1.match_id)), 0)
            AS nb_goals_norm,

        COALESCE(SAFE_DIVIDE(sq1.nb_contre_attaques - MIN(sq1.nb_contre_attaques) OVER (PARTITION BY sq1.match_id),
            MAX(sq1.nb_contre_attaques) OVER (PARTITION BY sq1.match_id) - MIN(sq1.nb_contre_attaques) OVER (PARTITION BY sq1.match_id)), 0)
            AS nb_contre_attaques_norm,

        COALESCE(SAFE_DIVIDE(sq1.nb_tirs_cadres - MIN(sq1.nb_tirs_cadres) OVER (PARTITION BY sq1.match_id),
            MAX(sq1.nb_tirs_cadres) OVER (PARTITION BY sq1.match_id) - MIN(sq1.nb_tirs_cadres) OVER (PARTITION BY sq1.match_id)), 0)
            AS nb_tirs_cadres_norm,

        COALESCE(SAFE_DIVIDE(sq1.taux_conversion - MIN(sq1.taux_conversion) OVER (PARTITION BY sq1.match_id),
            MAX(sq1.taux_conversion) OVER (PARTITION BY sq1.match_id) - MIN(sq1.taux_conversion) OVER (PARTITION BY sq1.match_id)), 0)
            AS taux_conversion_norm,

        COALESCE(SAFE_DIVIDE(sq1.nb_pass_goal_assist - MIN(sq1.nb_pass_goal_assist) OVER (PARTITION BY sq1.match_id),
            MAX(sq1.nb_pass_goal_assist) OVER (PARTITION BY sq1.match_id) - MIN(sq1.nb_pass_goal_assist) OVER (PARTITION BY sq1.match_id)), 0)
            AS nb_pass_goal_assist_norm,

        COALESCE(SAFE_DIVIDE(sq1.nb_dribbles - MIN(sq1.nb_dribbles) OVER (PARTITION BY sq1.match_id),
            MAX(sq1.nb_dribbles) OVER (PARTITION BY sq1.match_id) - MIN(sq1.nb_dribbles) OVER (PARTITION BY sq1.match_id)), 0)
            AS nb_dribbles_norm,

        COALESCE(SAFE_DIVIDE(sq1.nb_pass_complete - MIN(sq1.nb_pass_complete) OVER (PARTITION BY sq1.match_id),
            MAX(sq1.nb_pass_complete) OVER (PARTITION BY sq1.match_id) - MIN(sq1.nb_pass_complete) OVER (PARTITION BY sq1.match_id)), 0)
            AS nb_pass_complete_norm,

        COALESCE(SAFE_DIVIDE(sq1.nb_pass_through_ball - MIN(sq1.nb_pass_through_ball) OVER (PARTITION BY sq1.match_id),
            MAX(sq1.nb_pass_through_ball) OVER (PARTITION BY sq1.match_id) - MIN(sq1.nb_pass_through_ball) OVER (PARTITION BY sq1.match_id)), 0)
            AS nb_pass_through_ball_norm,

        COALESCE(SAFE_DIVIDE(sq1.nb_interceptions - MIN(sq1.nb_interceptions) OVER (PARTITION BY sq1.match_id),
            MAX(sq1.nb_interceptions) OVER (PARTITION BY sq1.match_id) - MIN(sq1.nb_interceptions) OVER (PARTITION BY sq1.match_id)), 0)
            AS nb_interceptions_norm,

        COALESCE(SAFE_DIVIDE(sq1.nb_duel_win - MIN(sq1.nb_duel_win) OVER (PARTITION BY sq1.match_id),
            MAX(sq1.nb_duel_win) OVER (PARTITION BY sq1.match_id) - MIN(sq1.nb_duel_win) OVER (PARTITION BY sq1.match_id)), 0)
            AS nb_duel_win_norm

    FROM sq1
    LEFT JOIN minutes_join mj
        ON  sq1.player_id = mj.player_id
        AND sq1.match_id  = mj.match_id
),

scores AS (
    SELECT
        *,
        ROUND(0.6 * SAFE_DIVIDE(
            nb_goals_norm + nb_contre_attaques_norm + nb_tirs_cadres_norm + taux_conversion_norm + nb_dribbles_norm + nb_pass_goal_assist_norm,
            6), 4) AS score_attaque,

        ROUND(0.3 * SAFE_DIVIDE(
            nb_pass_complete_norm + nb_pass_through_ball_norm,
            2), 4) AS score_middle,

        ROUND(0.1 * SAFE_DIVIDE(
            nb_interceptions_norm + nb_duel_win_norm,
            2), 4) AS score_defense
    FROM normalized_kpis
),

scores_with_final AS (
    SELECT
        *,
        ROUND(score_attaque + score_middle + score_defense, 4) AS score_final
    FROM scores
)

SELECT
    swf.player_id,
    swf.player,
    swf.poste,

    -- Infos joueur
    DATE_DIFF(CURRENT_DATE(), dob.date_of_birth, YEAR) AS age,

    COUNT(swf.match_id)                 AS nb_matches,
    SUM(swf.total_min)                  AS total_minutes_played,
    SUM(swf.nb_goals)                   AS nb_goals,
    SUM(swf.nb_contre_attaques)         AS nb_contre_attaques,
    SUM(swf.nb_tirs_cadres)             AS nb_tirs_cadres,
    ROUND(AVG(swf.taux_conversion), 2)  AS taux_conversion_moy,
    SUM(swf.nb_pass_goal_assist)        AS nb_pass_goal_assist,
    SUM(swf.nb_dribbles)                AS nb_dribbles,
    SUM(swf.nb_pass_complete)           AS nb_pass_complete,
    SUM(swf.nb_pass_through_ball)       AS nb_pass_through_ball,
    SUM(swf.nb_interceptions)           AS nb_interceptions,
    SUM(swf.nb_duel_win)                AS nb_duel_win,

    ROUND(AVG(swf.score_attaque), 2)    AS score_attaque,
    ROUND(AVG(swf.score_middle), 2)     AS score_middle,
    ROUND(AVG(swf.score_defense), 2)    AS score_defense,
    ROUND(AVG(swf.score_final), 2)      AS score_final

FROM scores_with_final AS swf
LEFT JOIN {{ ref('stg_Raw_data__leverkusen_players_date_birth') }} AS dob
    ON swf.player = dob.player_name
GROUP BY 1, 2, 3, dob.date_of_birth
ORDER BY score_final DESC