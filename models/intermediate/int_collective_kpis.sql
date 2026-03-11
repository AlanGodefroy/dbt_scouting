WITH starting_11 AS (
    -- Get starting XI from lineups table, entry at minute 0
  SELECT DISTINCT
    match_id, 
    player_name AS player, 
    player_id, 
    0 AS entry_minute
  FROM {{ ref('stg_Raw_data__lineup_leverkusen') }}
  WHERE REGEXP_CONTAINS(positions, r'Starting XI')
),

subs_on AS (
  -- Players coming on: entry at substitution minute
  SELECT
    match_id,
    substitution_replacement AS player,
    substitution_replacement_id AS player_id,
    (event_period - 1) * 45 + TIME_DIFF(timestamp, TIME '00:00:00', MINUTE) AS entry_minute
  FROM {{ ref('stg_Raw_data__Events_Leverkusen') }}
  WHERE event_type = 'Substitution'
),

all_entries AS (
  SELECT 
  match_id,
  player,
  player_id,
  starting_11.entry_minute
FROM starting_11
UNION ALL
SELECT 
  match_id,
  player,
  player_id,
  subs_on.entry_minute
FROM subs_on
),

subs_off AS (
  -- Players going off: exit at substitution minute
  SELECT
    match_id,
    player,
    player_id,
    (event_period - 1) * 45 + TIME_DIFF(timestamp, TIME '00:00:00', MINUTE) AS exit_minute
  FROM {{ ref('stg_Raw_data__Events_Leverkusen') }}
  WHERE event_type = 'Substitution'
)
,

entry_exit_times AS (
  SELECT 
    a.match_id,
    a.player,
    a.player_id,
    a.entry_minute,
    COALESCE(s.exit_minute, 90) AS exit_minute,
  FROM all_entries AS a
  LEFT JOIN subs_off AS s
  ON a.player_id = s.player_id AND a.match_id = s.match_id
)
,
add_dob AS (
    SELECT
     match_id,
     player,
     player_id,
     entry_minute,
     exit_minute,
     (exit_minute - entry_minute) AS minutes_played,
     DATE_DIFF(DATE '2024-09-01', date_of_birth, YEAR) AS age
    FROM entry_exit_times AS x
    LEFT JOIN {{ ref('stg_Raw_data__leverkusen_players_date_birth') }} AS dob
    ON x.player= dob.player_name)
,
agg_player AS (
    SELECT
        match_id,
        player,
        CAST(player_id AS INT64) AS player_id,
        COUNTIF(shot_outcome= "Goal") AS nb_goals,
        COUNTIF(event_type = '50/50') AS nb_5050_total,
        COUNTIF(event_type = 'Foul Won') AS nb_fouls_suffered,
        COUNTIF(event_type = '50/50' AND REGEXP_CONTAINS(`50_50`, r'Won|Succes To Team')) AS nb_5050_success,
        COUNTIF(event_type = 'Substitution' AND substitution_outcome = 'Injury') AS nb_injury
    FROM {{ ref('stg_Raw_data__Events_Leverkusen') }} 
    GROUP BY match_id, player, player_id
)
,
final AS (
SELECT 
    add_dob.match_id,
    add_dob.player,
    add_dob.player_id,
    entry_minute,
    exit_minute,
    minutes_played, 
    age,
    COALESCE(nb_goals, 0) AS nb_goals,
    COALESCE(nb_5050_total, 0) AS nb_5050_total,
    COALESCE(nb_5050_success, 0) AS nb_5050_success,
    COALESCE(nb_fouls_suffered, 0) AS nb_fouls_suffered,
    COALESCE(nb_injury, 0) AS nb_injury
FROM add_dob
LEFT JOIN agg_player
ON add_dob.player_id=agg_player.player_id
AND add_dob.match_id=agg_player.match_id
)
,
raw_stats AS (
  SELECT
    player,
    player_id,
    age,
    SUM(minutes_played)    AS total_minutes,
    SUM(nb_fouls_suffered) AS total_fouls,
    SUM(nb_injury)         AS total_injuries
  FROM final
  GROUP BY player, player_id, age
),

normalized AS (
  SELECT
    player,
    player_id,
    age,
    total_minutes,
    total_fouls,
    total_injuries,
    -- Normalize each metric between 0 and 1
    (total_minutes  - MIN(total_minutes)  OVER()) / NULLIF(MAX(total_minutes)  OVER() - MIN(total_minutes)  OVER(), 0) AS norm_minutes,
    (age            - MIN(age)            OVER()) / NULLIF(MAX(age)            OVER() - MIN(age)            OVER(), 0) AS norm_age,
    (total_fouls    - MIN(total_fouls)    OVER()) / NULLIF(MAX(total_fouls)    OVER() - MIN(total_fouls)    OVER(), 0) AS norm_fouls,
    (total_injuries - MIN(total_injuries) OVER()) / NULLIF(MAX(total_injuries) OVER() - MIN(total_injuries) OVER(), 0) AS norm_injuries
  FROM raw_stats
)

SELECT
  player,
  player_id,
  age,
  total_minutes,
  total_fouls,
  total_injuries,
  ROUND(
    0.4 * norm_minutes
  + 0.2 * norm_age
  + 0.3 * norm_fouls
  + 0.1 * norm_injuries
  , 2) AS fatigue_score
FROM normalized
ORDER BY fatigue_score DESC


