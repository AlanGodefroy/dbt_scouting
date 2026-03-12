WITH raw_events AS (
    SELECT 
        o.*,
        p.poste
    FROM {{ ref('stg_Raw_data__Events_Leverkusen') }} AS o
    LEFT JOIN {{ ref('stg_Raw_data__Poste_Leverkusen') }} AS p
    ON o.player = p.player_name
),

stats_attack AS (
    SELECT
        player AS player_id,
        player,
        poste,
        COUNT(DISTINCT match_id) AS nb_match,

        -- Attaque (Spécifique aux attaquants)
        COUNTIF(shot_outcome = "Goal") AS goals,
        ROUND(COUNTIF(shot_outcome = "Goal") / NULLIF(COUNT(DISTINCT match_id), 0), 2) AS goals_per_match,
        ROUND(SUM(shot_statsbomb_xg), 2) AS xg_total,
        ROUND(SUM(shot_statsbomb_xg) / NULLIF(COUNT(DISTINCT match_id), 0), 2) AS xg_per_match,
        COUNTIF(shot_outcome IN ('Goal', 'Saved')) AS tirs_cadres,
        ROUND(COUNTIF(shot_outcome IN ('Goal', 'Saved')) / NULLIF(COUNT(DISTINCT match_id), 0), 2) AS tirs_cadres_per_match,
        ROUND(COUNTIF(shot_outcome = 'Goal') / NULLIF(COUNTIF(shot_outcome IS NOT NULL), 0), 2) AS taux_conversion,

        -- Milieu / Création (Adapté aux attaquants)
        COUNTIF(pass_goal_assist = TRUE) AS pass_goal_assist,
        ROUND(COUNTIF(pass_goal_assist = TRUE) / NULLIF(COUNT(DISTINCT match_id), 0), 2) AS pass_goal_assist_per_match,
        COUNTIF(dribble_outcome = 'Complete') AS dribbles_reussis,
        ROUND(COUNTIF(dribble_outcome = 'Complete') / NULLIF(COUNT(DISTINCT match_id), 0), 2) AS dribbles_per_match,
        COUNTIF(event_type = "Pass" AND pass_outcome IS NULL) AS pass_complete,
        ROUND(COUNTIF(event_type = "Pass" AND pass_outcome IS NULL) / NULLIF(COUNT(DISTINCT match_id), 0), 2) AS pass_complete_per_match,
        COUNTIF(pass_through_ball = TRUE) AS pass_through_ball,
        ROUND(COUNTIF(pass_through_ball = TRUE) / NULLIF(COUNT(DISTINCT match_id), 0), 2) AS pass_through_ball_per_match,

        -- Défense (Le pressing haut)
        COUNTIF(interception_outcome IN ("Won", "Success In Play")) AS interceptions,
        ROUND(COUNTIF(interception_outcome IN ("Won", "Success In Play")) / NULLIF(COUNT(DISTINCT match_id), 0), 2) AS interceptions_per_match,
        COUNTIF(duel_outcome = "Won") AS duel_win,
        ROUND(COUNTIF(duel_outcome = "Won") / NULLIF(COUNT(DISTINCT match_id), 0), 2) AS duel_win_per_match

    FROM raw_events
    WHERE poste = "Attack"
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
        (goals_per_match - MIN(goals_per_match) OVER()) / NULLIF(MAX(goals_per_match) OVER() - MIN(goals_per_match) OVER(), 0) AS n_goals,
        (xg_per_match - MIN(xg_per_match) OVER()) / NULLIF(MAX(xg_per_match) OVER() - MIN(xg_per_match) OVER(), 0) AS n_xg,
        (tirs_cadres_per_match - MIN(tirs_cadres_per_match) OVER()) / NULLIF(MAX(tirs_cadres_per_match) OVER() - MIN(tirs_cadres_per_match) OVER(), 0) AS n_tirs_cadres,
        (taux_conversion - MIN(taux_conversion) OVER()) / NULLIF(MAX(taux_conversion) OVER() - MIN(taux_conversion) OVER(), 0) AS n_conversion,

        -- Normalisation Milieu
        (pass_goal_assist_per_match - MIN(pass_goal_assist_per_match) OVER()) / NULLIF(MAX(pass_goal_assist_per_match) OVER() - MIN(pass_goal_assist_per_match) OVER(), 0) AS n_pass_goal_assist,
        (dribbles_per_match - MIN(dribbles_per_match) OVER()) / NULLIF(MAX(dribbles_per_match) OVER() - MIN(dribbles_per_match) OVER(), 0) AS n_dribbles,
        (pass_complete_per_match - MIN(pass_complete_per_match) OVER()) / NULLIF(MAX(pass_complete_per_match) OVER() - MIN(pass_complete_per_match) OVER(), 0) AS n_pass_complete,
        (pass_through_ball_per_match - MIN(pass_through_ball_per_match) OVER()) / NULLIF(MAX(pass_through_ball_per_match) OVER() - MIN(pass_through_ball_per_match) OVER(), 0) AS n_pass_through_ball,

        -- Normalisation Défense
        (interceptions_per_match - MIN(interceptions_per_match) OVER()) / NULLIF(MAX(interceptions_per_match) OVER() - MIN(interceptions_per_match) OVER(), 0) AS n_interceptions,
        (duel_win_per_match - MIN(duel_win_per_match) OVER()) / NULLIF(MAX(duel_win_per_match) OVER() - MIN(duel_win_per_match) OVER(), 0) AS n_duel_win

    FROM stats_attack
),

scores AS (
    SELECT
        *,
        -- Score Attack (Moyenne pure des 6 KPIs : Buts, xG, Conversion, Passes dé, Dribbles, Tirs cadrés)
        ROUND(
            (COALESCE(n_goals, 0) + COALESCE(n_xg, 0) + COALESCE(n_conversion, 0) + 
             COALESCE(n_pass_goal_assist, 0) + COALESCE(n_dribbles, 0) + COALESCE(n_tirs_cadres, 0)) / 6
        , 4) AS score_attaque,

        -- Score Middle (Moyenne pure des 2 KPIs : Passes réussies, Passes cassant des lignes)
        ROUND(
            (COALESCE(n_pass_complete, 0) + COALESCE(n_pass_through_ball, 0)) / 2
        , 4) AS score_milieu,

        -- Score Defense (Moyenne pure des 2 KPIs : Interceptions, Duels gagnés)
        ROUND(
            (COALESCE(n_interceptions, 0) + COALESCE(n_duel_win, 0)) / 2
        , 4) AS score_defense

    FROM normalized
)

-- Score final pondéré
SELECT
    s.player_id,
    s.player,
    s.poste,
    s.nb_match,
    
    -- Scores (Pour un attaquant, l'attaque vaut 60%)
    sc.score_attaque,
    sc.score_milieu,
    sc.score_defense,
    ROUND((sc.score_attaque * 0.6) + (sc.score_milieu * 0.2) + (sc.score_defense * 0.2), 4) AS score_final,

    -- KPI Attaque
    s.goals,
    s.goals_per_match,
    s.xg_total,
    s.xg_per_match,
    s.tirs_cadres,
    s.tirs_cadres_per_match,
    s.taux_conversion,

    -- KPI Milieu / Création
    s.pass_goal_assist,
    s.pass_goal_assist_per_match,
    s.dribbles_reussis,
    s.dribbles_per_match,
    s.pass_complete,
    s.pass_complete_per_match,
    s.pass_through_ball,
    s.pass_through_ball_per_match,

    -- KPI Défense
    s.interceptions,
    s.interceptions_per_match,
    s.duel_win,
    s.duel_win_per_match

FROM stats_attack AS s
LEFT JOIN scores AS sc
ON s.player_id = sc.player_id
ORDER BY score_final DESC