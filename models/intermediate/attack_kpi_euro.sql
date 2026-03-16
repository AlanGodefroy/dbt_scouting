WITH stats_attack AS (
    SELECT
        evl.player_id,
        evl.player,
        pl.poste,
        COUNT(DISTINCT evl.match_id) AS nb_match,

        -- Attaque
        COUNTIF(evl.shot_outcome = "Goal") AS goals,
        ROUND(COUNTIF(evl.shot_outcome = "Goal") / COUNT(DISTINCT evl.match_id), 2) AS goals_per_match,
        COUNTIF(evl.play_pattern = 'From Counter') AS contre_attaques,
        ROUND(COUNTIF(evl.play_pattern = 'From Counter') / COUNT(DISTINCT evl.match_id), 2) AS contre_attaques_per_match,
        COUNTIF(evl.shot_outcome IN ('Goal', 'Saved')) AS tirs_cadres,
        ROUND(COUNTIF(evl.shot_outcome IN ('Goal', 'Saved')) / COUNT(DISTINCT evl.match_id), 2) AS tirs_cadres_per_match,
        ROUND(COUNTIF(evl.shot_outcome = 'Goal') / NULLIF(COUNTIF(evl.shot_outcome IS NOT NULL), 0), 2) AS taux_conversion,
        COUNTIF(evl.pass_goal_assist = TRUE) AS pass_goal_assist,
        ROUND(COUNTIF(evl.pass_goal_assist = TRUE) / COUNT(DISTINCT evl.match_id), 2) AS pass_goal_assist_per_match,
        COUNTIF(evl.dribble_outcome = 'Complete') AS dribbles_reussis,
        ROUND(COUNTIF(evl.dribble_outcome = 'Complete') / COUNT(DISTINCT evl.match_id), 2) AS dribbles_per_match,

        -- Milieu / Création
        COUNTIF(evl.event_type = "Pass" AND evl.pass_outcome IS NULL) AS pass_complete,
        ROUND(COUNTIF(evl.event_type = "Pass" AND evl.pass_outcome IS NULL) / COUNT(DISTINCT evl.match_id), 2) AS pass_complete_per_match,
        COUNTIF(evl.pass_through_ball = TRUE) AS pass_through_ball,
        ROUND(COUNTIF(evl.pass_through_ball = TRUE) / COUNT(DISTINCT evl.match_id), 2) AS pass_through_ball_per_match,

        -- Défense
        COUNTIF(evl.interception_outcome IN ("Won", "Success In Play")) AS interceptions,
        ROUND(COUNTIF(evl.interception_outcome IN ("Won", "Success In Play")) / COUNT(DISTINCT evl.match_id), 2) AS interceptions_per_match,
        COUNTIF(evl.duel_outcome = "Won") AS duel_win,
        ROUND(COUNTIF(evl.duel_outcome = "Won") / COUNT(DISTINCT evl.match_id), 2) AS duel_win_per_match

    FROM {{ ref('stg_Raw_data__Events_euro_2024') }} AS evl
    LEFT JOIN {{ ref('stg_Raw_data__Poste_euro_2024') }} AS pl
        ON evl.player = pl.player_name
    WHERE evl.player_id IS NOT NULL AND pl.poste = 'Attack'
    GROUP BY evl.player_id, evl.player, pl.poste
),

normalized AS (
    SELECT
        player_id,
        player,
        poste,
        nb_match,

        -- Normalisation Attaque (6 métriques)
        SAFE_DIVIDE(goals_per_match - MIN(goals_per_match) OVER(), MAX(goals_per_match) OVER() - MIN(goals_per_match) OVER()) AS n_goals,
        SAFE_DIVIDE(contre_attaques_per_match - MIN(contre_attaques_per_match) OVER(), MAX(contre_attaques_per_match) OVER() - MIN(contre_attaques_per_match) OVER()) AS n_contre_attaques,
        SAFE_DIVIDE(tirs_cadres_per_match - MIN(tirs_cadres_per_match) OVER(), MAX(tirs_cadres_per_match) OVER() - MIN(tirs_cadres_per_match) OVER()) AS n_tirs_cadres,
        SAFE_DIVIDE(taux_conversion - MIN(taux_conversion) OVER(), MAX(taux_conversion) OVER() - MIN(taux_conversion) OVER()) AS n_conversion,
        SAFE_DIVIDE(pass_goal_assist_per_match - MIN(pass_goal_assist_per_match) OVER(), MAX(pass_goal_assist_per_match) OVER() - MIN(pass_goal_assist_per_match) OVER()) AS n_pass_goal_assist,
        SAFE_DIVIDE(dribbles_per_match - MIN(dribbles_per_match) OVER(), MAX(dribbles_per_match) OVER() - MIN(dribbles_per_match) OVER()) AS n_dribbles,

        -- Normalisation Milieu (2 métriques)
        SAFE_DIVIDE(pass_complete_per_match - MIN(pass_complete_per_match) OVER(), MAX(pass_complete_per_match) OVER() - MIN(pass_complete_per_match) OVER()) AS n_pass_complete,
        SAFE_DIVIDE(pass_through_ball_per_match - MIN(pass_through_ball_per_match) OVER(), MAX(pass_through_ball_per_match) OVER() - MIN(pass_through_ball_per_match) OVER()) AS n_pass_through_ball,

        -- Normalisation Défense (2 métriques)
        SAFE_DIVIDE(interceptions_per_match - MIN(interceptions_per_match) OVER(), MAX(interceptions_per_match) OVER() - MIN(interceptions_per_match) OVER()) AS n_interceptions,
        SAFE_DIVIDE(duel_win_per_match - MIN(duel_win_per_match) OVER(), MAX(duel_win_per_match) OVER() - MIN(duel_win_per_match) OVER()) AS n_duel_win

    FROM stats_attack
),

scores AS (
    SELECT
        *,
        ROUND(
            (COALESCE(n_goals, 0) + COALESCE(n_contre_attaques, 0) + COALESCE(n_tirs_cadres, 0) +
             COALESCE(n_conversion, 0) + COALESCE(n_pass_goal_assist, 0) + COALESCE(n_dribbles, 0)) / 6
        , 4) AS score_attaque,

        ROUND(
            (COALESCE(n_pass_complete, 0) + COALESCE(n_pass_through_ball, 0)) / 2
        , 4) AS score_milieu,

        ROUND(
            (COALESCE(n_interceptions, 0) + COALESCE(n_duel_win, 0)) / 2
        , 4) AS score_defense

    FROM normalized
)

SELECT
    s.player_id,
    s.player,
    s.poste,
    s.nb_match,

    -- Infos joueur
    gd.team,
    gd.current_club_name,
    gd.market_value,
    DATE_DIFF(CURRENT_DATE(), gd.date_of_birth, YEAR) AS age,

    -- Scores
    sc.score_attaque,
    sc.score_milieu,
    sc.score_defense,
    ROUND((sc.score_attaque * 0.6) + (sc.score_milieu * 0.2) + (sc.score_defense * 0.2), 4) AS score_final,

    -- KPI Attaque
    s.goals,
    s.goals_per_match,
    s.contre_attaques,
    s.contre_attaques_per_match,
    s.tirs_cadres,
    s.tirs_cadres_per_match,
    s.taux_conversion,
    s.pass_goal_assist,
    s.pass_goal_assist_per_match,
    s.dribbles_reussis,
    s.dribbles_per_match,

    -- KPI Milieu / Création
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
LEFT JOIN {{ ref('stg_Raw_data__euro_24_global_data_players') }} AS gd
    ON s.player_id = gd.player_id
ORDER BY score_final DESC