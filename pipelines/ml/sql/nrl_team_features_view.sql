-- Feature view: per-team per-season aggregate stats from NRL fixtures (1990-2025).
-- Unpivots home/away into a single row per team per match, then aggregates by season.
CREATE OR REPLACE VIEW `data-feeder-lcd.curated.nrl_team_features_v` AS
WITH matches AS (
  -- Unpivot: one row per team per match
  SELECT
    Season AS year,
    Round AS round,
    HomeTeam AS team,
    HomeScore AS points_for,
    AwayScore AS points_against,
    HomeScore - AwayScore AS margin,
    CASE WHEN HomeScore > AwayScore THEN 1 ELSE 0 END AS win,
    CASE WHEN HomeScore = AwayScore THEN 1 ELSE 0 END AS draw,
    CASE WHEN HomeScore < AwayScore THEN 1 ELSE 0 END AS loss,
    1 AS is_home,
    Venue AS venue
  FROM `data-feeder-lcd.curated.nrl_fixtures_1990_2025`
  WHERE HomeScore IS NOT NULL AND AwayScore IS NOT NULL

  UNION ALL

  SELECT
    Season AS year,
    Round AS round,
    AwayTeam AS team,
    AwayScore AS points_for,
    HomeScore AS points_against,
    AwayScore - HomeScore AS margin,
    CASE WHEN AwayScore > HomeScore THEN 1 ELSE 0 END AS win,
    CASE WHEN AwayScore = HomeScore THEN 1 ELSE 0 END AS draw,
    CASE WHEN AwayScore < HomeScore THEN 1 ELSE 0 END AS loss,
    0 AS is_home,
    Venue AS venue
  FROM `data-feeder-lcd.curated.nrl_fixtures_1990_2025`
  WHERE HomeScore IS NOT NULL AND AwayScore IS NOT NULL
)
SELECT
  team,
  year,
  COUNT(*) AS games_played,
  SUM(win) AS wins,
  SUM(draw) AS draws,
  SUM(loss) AS losses,
  ROUND(SAFE_DIVIDE(SUM(win), COUNT(*)), 3) AS win_rate,
  SUM(points_for) AS total_points_for,
  SUM(points_against) AS total_points_against,
  ROUND(AVG(points_for), 2) AS avg_points_for,
  ROUND(AVG(points_against), 2) AS avg_points_against,
  ROUND(AVG(margin), 2) AS avg_margin,
  ROUND(STDDEV(margin), 2) AS margin_stddev,
  -- Home/away splits
  SUM(CASE WHEN is_home = 1 THEN win ELSE 0 END) AS home_wins,
  SUM(CASE WHEN is_home = 1 THEN 1 ELSE 0 END) AS home_games,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN is_home = 1 THEN win ELSE 0 END),
    NULLIF(SUM(CASE WHEN is_home = 1 THEN 1 ELSE 0 END), 0)), 3) AS home_win_rate,
  SUM(CASE WHEN is_home = 0 THEN win ELSE 0 END) AS away_wins,
  SUM(CASE WHEN is_home = 0 THEN 1 ELSE 0 END) AS away_games,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN is_home = 0 THEN win ELSE 0 END),
    NULLIF(SUM(CASE WHEN is_home = 0 THEN 1 ELSE 0 END), 0)), 3) AS away_win_rate,
  -- Dominant wins (13+ points, ~2 converted tries)
  SUM(CASE WHEN margin >= 13 THEN 1 ELSE 0 END) AS blowout_wins,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN margin >= 13 THEN 1 ELSE 0 END),
    NULLIF(SUM(win), 0)), 3) AS blowout_rate,
  -- Close games (decided by 6 or fewer points)
  SUM(CASE WHEN ABS(margin) <= 6 THEN 1 ELSE 0 END) AS close_games,
  SUM(CASE WHEN ABS(margin) <= 6 AND win = 1 THEN 1 ELSE 0 END) AS close_wins,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN ABS(margin) <= 6 AND win = 1 THEN 1 ELSE 0 END),
    NULLIF(SUM(CASE WHEN ABS(margin) <= 6 THEN 1 ELSE 0 END), 0)), 3) AS close_game_win_rate,
  -- Points differential
  SUM(points_for) - SUM(points_against) AS points_differential
FROM matches
GROUP BY team, year
HAVING COUNT(*) >= 5;
