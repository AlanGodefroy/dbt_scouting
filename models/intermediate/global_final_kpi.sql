WITH raw_stats AS (
  SELECT
    player,
    player_id,
    age,
    nb_goals,
    SUM(minutes_played)    AS total_minutes,
    SUM(nb_fouls_suffered) AS total_fouls,
    SUM(nb_injury)         AS total_injuries
  FROM {{ ref('int_collective_kpis') }}
  GROUP BY player, player_id, age, nb_goals
),

normalized AS (
  SELECT
    player,
    player_id,
    age,
    nb_goals,
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
  nb_goals,
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