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
,
petite_table AS (
SELECT 
* 
FROM middle_euro
UNION ALL
SELECT 
* 
FROM defense_euro
)
,
attack_euro AS (
    SELECT 
    player_id,
    player,
    poste,
    score_attaque AS score_attack,
    score_milieu AS score_middle,
    score_defense,
    score_final,
    age,
    market_value,
    current_club_name,
    FROM {{ ref('attack_kpi_euro') }}
)
,
grosse_table AS (
    SELECT 
    * 
    FROM petite_table
    UNION ALL
    SELECT 
    * 
    FROM attack_euro
)

SELECT 
grosse_table.player_id,
player,
poste,
score_attack,
score_middle,
score_defense,
score_final,
grosse_table.age,
grosse_table.market_value,
grosse_table.current_club_name,
image_url,
FROM grosse_table
LEFT JOIN {{ ref('stg_Raw_data__euro2024_players_images_matched') }} AS img
ON grosse_table.player_id = img.player_id
