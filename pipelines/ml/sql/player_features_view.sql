CREATE OR REPLACE VIEW `data-feeder-lcd.curated.player_features_v` AS
SELECT
  p.player_id,
  p.position,
  p.league,
  SAFE_CAST(p.market_value AS FLOAT64)                    AS market_value,
  SAFE_CAST(s.appearances AS INT64)                       AS appearances,
  SAFE_CAST(s.matches_started AS INT64)                   AS matches_started,
  SAFE_CAST(s.minutes_played AS INT64)                    AS minutes_played,
  SAFE_CAST(s.goals AS INT64)                             AS goals,
  SAFE_CAST(s.assists AS INT64)                           AS assists,
  SAFE_CAST(s.expected_goals AS FLOAT64)                  AS expected_goals,
  SAFE_CAST(s.expected_assists AS FLOAT64)                AS expected_assists,
  SAFE_CAST(s.rating AS FLOAT64)                          AS rating,
  SAFE_CAST(s.total_shots AS INT64)                       AS total_shots,
  SAFE_CAST(s.shots_on_target AS INT64)                   AS shots_on_target,
  SAFE_CAST(s.yellow_cards AS INT64)                      AS yellow_cards,
  SAFE_CAST(s.red_cards AS INT64)                         AS red_cards,
  SAFE_CAST(s.tackles AS INT64)                           AS tackles,
  SAFE_CAST(s.interceptions AS INT64)                     AS interceptions,
  SAFE_CAST(s.saves AS INT64)                             AS saves,
  -- Derived features
  SAFE_DIVIDE(SAFE_CAST(s.goals AS FLOAT64), NULLIF(SAFE_CAST(s.appearances AS FLOAT64), 0))             AS goals_per_appearance,
  SAFE_DIVIDE(SAFE_CAST(s.assists AS FLOAT64), NULLIF(SAFE_CAST(s.appearances AS FLOAT64), 0))           AS assists_per_appearance,
  SAFE_DIVIDE(SAFE_CAST(s.tackles AS FLOAT64), NULLIF(SAFE_CAST(s.appearances AS FLOAT64), 0))           AS tackles_per_appearance,
  SAFE_DIVIDE(SAFE_CAST(s.shots_on_target AS FLOAT64), NULLIF(SAFE_CAST(s.total_shots AS FLOAT64), 0))   AS shot_accuracy,
  SAFE_CAST(s.goals AS FLOAT64) - SAFE_CAST(s.expected_goals AS FLOAT64)                                  AS goals_vs_expected
FROM
  `data-feeder-lcd.curated.all_player_profiles` p
INNER JOIN
  `data-feeder-lcd.curated.all_player_stats` s
ON
  p.player_id = s.player_id
WHERE
  SAFE_CAST(s.appearances AS INT64) > 0;
