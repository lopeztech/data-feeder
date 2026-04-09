-- Feature view: per-match rolling features for NRL match outcome prediction.
-- Computes rolling form, head-to-head, and contextual features for each fixture.
CREATE OR REPLACE VIEW `data-feeder-lcd.curated.nrl_match_features_v` AS
WITH matches AS (
  SELECT
    PARSE_DATE('%Y-%m-%d', date) AS match_date,
    EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', date)) AS year,
    round,
    home_team,
    away_team,
    home_score,
    away_score,
    home_score - away_score AS margin,
    venue
  FROM `data-feeder-lcd.curated.nrl_fixtures_1990_2025`
  WHERE home_score IS NOT NULL AND away_score IS NOT NULL
),

-- Unpivot to per-team rows for rolling calcs
team_matches AS (
  SELECT match_date, year, round, home_team AS team, home_score AS pf, away_score AS pa,
    home_score - away_score AS margin,
    CASE WHEN home_score > away_score THEN 1 ELSE 0 END AS win, 1 AS is_home
  FROM matches
  UNION ALL
  SELECT match_date, year, round, away_team, away_score, home_score,
    away_score - home_score,
    CASE WHEN away_score > home_score THEN 1 ELSE 0 END, 0
  FROM matches
),

-- Rolling stats per team (last 5 matches before each game)
rolling AS (
  SELECT
    team, match_date, year, round,
    AVG(win) OVER (PARTITION BY team ORDER BY match_date
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS last5_win_rate,
    AVG(margin) OVER (PARTITION BY team ORDER BY match_date
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS last5_avg_margin,
    AVG(pf) OVER (PARTITION BY team ORDER BY match_date
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS last5_avg_pf,
    AVG(pa) OVER (PARTITION BY team ORDER BY match_date
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS last5_avg_pa,
    -- Season-to-date win rate
    AVG(win) OVER (PARTITION BY team, year ORDER BY match_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS season_win_rate,
    -- Home-only win rate this season
    AVG(CASE WHEN is_home = 1 THEN win END) OVER (PARTITION BY team, year ORDER BY match_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS season_home_win_rate
  FROM team_matches
),

-- Head-to-head historical record (all prior meetings)
h2h AS (
  SELECT
    m.match_date,
    m.home_team,
    m.away_team,
    COUNTIF(prior.home_score > prior.away_score) AS h2h_home_wins,
    COUNTIF(prior.away_score > prior.home_score) AS h2h_away_wins,
    COUNT(*) AS h2h_total
  FROM matches m
  LEFT JOIN matches prior
    ON prior.match_date < m.match_date
    AND ((prior.home_team = m.home_team AND prior.away_team = m.away_team)
      OR (prior.home_team = m.away_team AND prior.away_team = m.home_team))
  GROUP BY m.match_date, m.home_team, m.away_team
)

SELECT
  m.match_date,
  m.year,
  m.round,
  m.home_team,
  m.away_team,
  m.home_score,
  m.away_score,
  m.margin,
  m.venue,
  -- Home team rolling features
  ROUND(hr.last5_win_rate, 3) AS home_last5_win_rate,
  ROUND(hr.last5_avg_margin, 2) AS home_last5_avg_margin,
  ROUND(hr.last5_avg_pf, 2) AS home_last5_avg_pf,
  ROUND(hr.last5_avg_pa, 2) AS home_last5_avg_pa,
  ROUND(hr.season_win_rate, 3) AS home_season_win_rate,
  ROUND(hr.season_home_win_rate, 3) AS home_season_home_win_rate,
  -- Away team rolling features
  ROUND(ar.last5_win_rate, 3) AS away_last5_win_rate,
  ROUND(ar.last5_avg_margin, 2) AS away_last5_avg_margin,
  ROUND(ar.last5_avg_pf, 2) AS away_last5_avg_pf,
  ROUND(ar.last5_avg_pa, 2) AS away_last5_avg_pa,
  ROUND(ar.season_win_rate, 3) AS away_season_win_rate,
  -- Head-to-head
  h.h2h_total,
  ROUND(SAFE_DIVIDE(h.h2h_home_wins, NULLIF(h.h2h_total, 0)), 3) AS h2h_home_win_rate,
  -- Derived
  ROUND(COALESCE(hr.last5_win_rate, 0.5) - COALESCE(ar.last5_win_rate, 0.5), 3) AS form_differential,
  ROUND(COALESCE(hr.season_win_rate, 0.5) - COALESCE(ar.season_win_rate, 0.5), 3) AS season_form_differential
FROM matches m
LEFT JOIN rolling hr ON hr.team = m.home_team AND hr.match_date = m.match_date
LEFT JOIN rolling ar ON ar.team = m.away_team AND ar.match_date = m.match_date
LEFT JOIN h2h h ON h.match_date = m.match_date AND h.home_team = m.home_team AND h.away_team = m.away_team;
