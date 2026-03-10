WITH starting_11 AS (
    -- Get starting XI from lineups table, entry at minute 0
  SELECT DISTINCT
    match_id, 
    player_name AS player, 
    player_id, 
    0 AS entry_minute
  FROM {{ ref('stg_Raw_data__lineup_leverkusen') }}
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
),

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

SELECT
  match_id,
  player,
  player_id,
  entry_minute,
  exit_minute,
  (exit_minute - entry_minute) AS minutes_played
FROM entry_exit_times