-- Feature view: tactical coaching dimensions from NRL fixtures (1990-2025).
-- Computes consistency, momentum, seasonal splits, and rivalry metrics per team.
CREATE OR REPLACE VIEW `data-feeder-lcd.curated.nrl_coaching_features_v` AS
WITH matches AS (
  SELECT
    PARSE_DATE('%Y-%m-%d', date) AS match_date,
    EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', date)) AS year,
    round,
    home_team,
    away_team,
    home_score,
    away_score,
    venue
  FROM `data-feeder-lcd.curated.nrl_fixtures_1990_2025`
  WHERE home_score IS NOT NULL AND away_score IS NOT NULL
),

-- Unpivot to per-team rows with lag for momentum
team_matches AS (
  SELECT
    match_date, year, round,
    home_team AS team,
    away_team AS opponent,
    home_score AS pf,
    away_score AS pa,
    home_score - away_score AS margin,
    CASE WHEN home_score > away_score THEN 1 ELSE 0 END AS win,
    CASE WHEN home_score < away_score THEN 1 ELSE 0 END AS loss,
    1 AS is_home,
    venue
  FROM matches
  UNION ALL
  SELECT
    match_date, year, round,
    away_team, home_team, away_score, home_score,
    away_score - home_score,
    CASE WHEN away_score > home_score THEN 1 ELSE 0 END,
    CASE WHEN away_score < home_score THEN 1 ELSE 0 END,
    0, venue
  FROM matches
),

with_prev AS (
  SELECT *,
    LAG(win) OVER (PARTITION BY team ORDER BY match_date) AS prev_win,
    LAG(loss) OVER (PARTITION BY team ORDER BY match_date) AS prev_loss,
    -- Approximate round number (parse numeric part if possible)
    SAFE_CAST(REGEXP_EXTRACT(round, r'(\d+)') AS INT64) AS round_num
  FROM team_matches
),

per_team AS (
  SELECT
    team,
    year,
    COUNT(*) AS games_played,
    SUM(win) AS wins,
    ROUND(SAFE_DIVIDE(SUM(win), COUNT(*)), 3) AS win_rate,
    ROUND(AVG(pf), 2) AS avg_points_for,
    ROUND(AVG(pa), 2) AS avg_points_against,
    ROUND(AVG(margin), 2) AS avg_margin,
    ROUND(STDDEV(margin), 2) AS consistency_score,

    -- Home fortress: home win rate minus away win rate
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN is_home = 1 THEN win ELSE 0 END),
      NULLIF(SUM(CASE WHEN is_home = 1 THEN 1 ELSE 0 END), 0)), 3) AS home_win_rate,
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN is_home = 0 THEN win ELSE 0 END),
      NULLIF(SUM(CASE WHEN is_home = 0 THEN 1 ELSE 0 END), 0)), 3) AS away_win_rate,
    ROUND(
      COALESCE(SAFE_DIVIDE(SUM(CASE WHEN is_home = 1 THEN win ELSE 0 END),
        NULLIF(SUM(CASE WHEN is_home = 1 THEN 1 ELSE 0 END), 0)), 0) -
      COALESCE(SAFE_DIVIDE(SUM(CASE WHEN is_home = 0 THEN win ELSE 0 END),
        NULLIF(SUM(CASE WHEN is_home = 0 THEN 1 ELSE 0 END), 0)), 0),
    3) AS home_dependency,

    -- Early season (rounds 1-8) vs late season (rounds 18+)
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN round_num BETWEEN 1 AND 8 THEN win ELSE 0 END),
      NULLIF(SUM(CASE WHEN round_num BETWEEN 1 AND 8 THEN 1 ELSE 0 END), 0)), 3) AS early_season_win_rate,
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN round_num >= 18 THEN win ELSE 0 END),
      NULLIF(SUM(CASE WHEN round_num >= 18 THEN 1 ELSE 0 END), 0)), 3) AS late_season_win_rate,

    -- Bounce-back: win rate after a loss
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN prev_loss = 1 THEN win ELSE 0 END),
      NULLIF(SUM(CASE WHEN prev_loss = 1 THEN 1 ELSE 0 END), 0)), 3) AS bounce_back_rate,
    -- Streak maintenance: win rate after a win
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN prev_win = 1 THEN win ELSE 0 END),
      NULLIF(SUM(CASE WHEN prev_win = 1 THEN 1 ELSE 0 END), 0)), 3) AS streak_maintenance_rate,

    -- Close games (margin <= 6)
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN ABS(margin) <= 6 AND win = 1 THEN 1 ELSE 0 END),
      NULLIF(SUM(CASE WHEN ABS(margin) <= 6 THEN 1 ELSE 0 END), 0)), 3) AS close_game_win_rate,

    -- Blowout vulnerability (lost by 13+)
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN margin <= -13 THEN 1 ELSE 0 END), COUNT(*)), 3) AS blowout_loss_rate,

    -- Attack vs defense ratio
    ROUND(SAFE_DIVIDE(AVG(pf), NULLIF(AVG(pa), 0)), 3) AS attack_defense_ratio

  FROM with_prev
  GROUP BY team, year
  HAVING COUNT(*) >= 5
)

SELECT * FROM per_team;
