
WITH euro_kpi AS (

SELECT
    player_id,
    player,
    poste,
    COUNT(DISTINCT match_id) AS nb_match,

    -- Scores finaux (moyenne des notes par match)
    ROUND(AVG(score_attaque_match), 4) AS score_attaque,
    ROUND(AVG(score_milieu_match), 4) AS score_milieu,
    ROUND(AVG(score_defense_match), 4) AS score_defense,
    ROUND(AVG(score_total_match),4) AS score_final,

    -- KPI bruts moyennés
    ROUND(SUM(goals), 0) AS goals,
    ROUND(AVG(goals), 2) AS goals_per_match,
    ROUND(SUM(xg_total), 2) AS xg_total,
    ROUND(AVG(xg_total), 2) AS xg_per_match,
    ROUND(AVG(taux_passes_reussies), 2) AS taux_passes_reussies,
    ROUND(AVG(pass_through_ball), 2) AS pass_through_ball_per_match,
    ROUND(AVG(pass_goal_assist), 2) AS pass_goal_assist_per_match,
    ROUND(AVG(pass_shot_assist), 2) AS pass_shot_assist_per_match,
    ROUND(AVG(pass_cross), 2) AS pass_cross_per_match,
    ROUND(AVG(pass_aerial_won), 2) AS pass_aerial_won_per_match,
    ROUND(AVG(pass_under_pressure), 2) AS pass_under_pressure_per_match,
    ROUND(AVG(ball_recovery_offensive), 2) AS ball_recovery_offensive_per_match,
    ROUND(AVG(foul_won), 2) AS foul_won_per_match,
    ROUND(AVG(duel_win), 2) AS duel_win_per_match,
    ROUND(AVG(interceptions), 2) AS interceptions_per_match,
    ROUND(AVG(contre_pressing), 2) AS contre_pressing_per_match,
    ROUND(AVG(clearance_aerial_won), 2) AS clearance_aerial_won_per_match

FROM {{ ref('Int_middle_euro') }}
GROUP BY  player_id, player, poste)

SELECT 
e.*,
g.age,
g.market_value,
g.team,
g.current_club_name
FROM euro_kpi AS e
LEFT JOIN {{ ref('stg_Raw_data__euro_24_global_data_players') }} AS g
ON e.player_id = g.player_id
WHERE market_value <= 30000000 AND nb_match >= 3 AND age < 27
ORDER BY score_final DESC
