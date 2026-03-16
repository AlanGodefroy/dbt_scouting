WITH middle_euro AS (
    SELECT 
    player_id,
    player,
    poste,
    score_attaque as score_attack,
    score_milieu as score_middle,
    score_defense,
    score_final,
    age,
    market_value,
    current_club_name
FROM {{ ref('kpi_middle_euro') }}
)
,
defense_euro AS (
    SELECT 
    player_id,
    player,
    poste,
    score_attack,
    score_middle,
    score_defense,
    score_final,
    age,
    market_value,
    current_club_name,
FROM {{ ref('kpi_defense_euro') }}
)

SELECT 
* 
FROM middle_euro
UNION ALL
SELECT 
* 
FROM defense_euro
