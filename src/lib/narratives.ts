export interface Narrative {
  title: string;
  overview: string;
  whatItShows: string;
  actions: string[];
  fineTuning: string | null;
}

export function getNarrative(modelName: string, modelType: string, outputTable?: string): Narrative {
  const key = `${modelName}-${modelType}-${outputTable ?? ''}`;

  // NFL Team models
  if (key.includes('team') && modelType === 'clusters') return {
    title: 'NFL Team Playing Styles',
    overview: 'This model groups NFL teams into archetypes based on how they play \u2014 passing volume, rushing tendencies, and turnover rates \u2014 independent of win/loss record. It reveals which playing styles are most strongly associated with winning.',
    whatItShows: 'Each cluster represents a distinct team archetype. The avg_win_pct shows which styles correlate with success. Teams in high-turnover clusters tend to lose more, while balanced passing/rushing teams win more consistently.',
    actions: [
      'Compare your team\'s archetype to the highest-winning cluster to identify style gaps',
      'Teams in the high-turnover cluster should prioritize ball security and defensive takeaways',
      'Use archetype labels to inform draft strategy \u2014 if pass-heavy teams win more, invest in QB and WR',
    ],
    fineTuning: 'The model currently finds 3 archetypes. If clusters are too broad, increase max_k to 10. If archetype labels seem off, the auto-labeling uses the top 2 features above global mean \u2014 consider weighting by feature importance instead.',
  };

  if (key.includes('team') && modelType === 'predictions') return {
    title: 'NFL Win Prediction Model',
    overview: 'A GradientBoosting model predicts each team\'s win percentage from their season statistics. Two models run: "all_features" uses every stat (including point differential which nearly determines wins), and "controllable" excludes outcome-derived stats to show which actionable inputs drive winning.',
    whatItShows: 'Teams with large positive residuals (actual > predicted) are over-performing their stats \u2014 they may be winning close games through coaching, luck, or intangibles. Negative residuals indicate under-performers whose stats suggest they should win more.',
    actions: [
      'Focus on the "controllable" model\'s feature importances \u2014 these are the stats a team can actually improve',
      'Over-performing teams (high residual) may regress \u2014 be cautious betting on sustained success without stat improvement',
      'Under-performing teams with strong controllable stats are buy-low candidates \u2014 their record should improve',
    ],
    fineTuning: 'R\u00B2 of ~0.80 is strong for NFL prediction. If MAE exceeds 0.10, try adding schedule strength as a feature. The "all_features" model is dominated by points_diff (0.82 importance) which is expected \u2014 the "controllable" model is more useful for decision-making.',
  };

  if (outputTable?.includes('feature_importances')) return {
    title: 'What Stats Drive Winning?',
    overview: 'Feature importance rankings from the win prediction model show which team statistics have the strongest relationship with winning. The "controllable" model excludes point differential and related stats to focus on actionable metrics.',
    whatItShows: 'Higher importance = stronger predictive power. The "all_features" model confirms points_diff dominates (0.82), but the "controllable" model reveals that scoring (points_per_game), defense (points_opp_per_game), and rushing volume (rush_att) are the most actionable drivers.',
    actions: [
      'Invest in offensive scoring capability and defensive points prevention \u2014 these are the top controllable factors',
      'Rushing attempts matter more than rushing efficiency \u2014 maintaining a run game keeps defenses honest',
      'Use these importances to weight player evaluation: positions that drive top features are most valuable',
    ],
    fineTuning: null,
  };

  if (outputTable?.includes('optimal_profile')) return {
    title: 'Championship Team Blueprint',
    overview: 'This analysis defines the statistical profile of elite NFL teams (top 25% win percentage per season) compared to the league average. The "gap" column shows how far above average a team needs to be in each stat to compete for a championship.',
    whatItShows: 'Elite teams produce ~380 more total yards, ~293 more passing yards, and ~60 more points per season than average. They also allow ~44 fewer points. The Ridge regression coefficients validate which gaps matter most for win percentage.',
    actions: [
      'Use the elite median as a target benchmark when evaluating team roster construction',
      'The biggest gaps (total_yards, pass_yds) suggest passing offense is the primary differentiator in the modern NFL',
      'Defensive point prevention (points_opp gap of -44.5) is the most actionable defensive target',
    ],
    fineTuning: 'The "top 25%" threshold means ~168 elite team-seasons out of 672. Adjust to top 10% for a stricter championship-only profile, or top 33% for a playoff-contender profile.',
  };

  if (outputTable?.includes('positional_value')) return {
    title: 'Positional Value for Roster Building',
    overview: 'This bridges team-level insights to player selection by mapping which positions contribute most to the stats that drive winning. Each position gets a value score based on how much it influences the top win-predictive features.',
    whatItShows: 'QB, OL, WR, and RB are nearly equal in total value, suggesting balanced roster investment is optimal. QB edges out slightly due to outsized influence on passing efficiency metrics. Defensive positions (DEF, DB, LB) contribute through turnover and penalty prevention.',
    actions: [
      'Don\'t over-invest in one position \u2014 the near-equal values suggest balanced roster construction wins',
      'QB has the highest single-position value \u2014 prioritize franchise QB acquisition',
      'OL is the second most valuable \u2014 offensive line investment has outsized impact on both passing and rushing',
    ],
    fineTuning: 'The position-contribution matrix is hardcoded with domain knowledge estimates. Consider calibrating weights against actual player-level regression data once more player datasets are uploaded.',
  };

  // European Football player models
  if (modelType === 'clusters' && modelName === 'player') return {
    title: 'Player Role Identification',
    overview: 'K-Means clustering groups players into natural roles based on their statistical profiles \u2014 goals, assists, tackles, saves, and other performance metrics. Each cluster represents a distinct player archetype.',
    whatItShows: 'Clusters typically separate into attackers (high goals/shots), midfielders (balanced assists/tackles), defenders (high tackles/interceptions), and goalkeepers (high saves). The impact_score measures how representative each player is of their cluster.',
    actions: [
      'Identify players near cluster boundaries \u2014 they may be versatile or misclassified by their listed position',
      'Compare player stats to their cluster\'s average to find under/over-performers within a role',
      'Use clusters to find replacement players: search the same cluster for similar statistical profiles',
    ],
    fineTuning: 'If goalkeepers aren\'t cleanly separated, their unique stat profile (saves vs goals) may need feature weighting. If clusters are unbalanced (one cluster has 50%+ of players), consider increasing k or removing low-variance features.',
  };

  if (modelType === 'anomalies') return {
    title: 'Unusual Player Detection',
    overview: 'Isolation Forest identifies players with statistical profiles that deviate significantly from the norm. These aren\'t necessarily "bad" players \u2014 anomalies can be hidden gems with unique skill combinations or players with unusual usage patterns.',
    whatItShows: 'Players flagged as anomalies have stat distributions that don\'t fit typical patterns. A high anomaly score means the player\'s profile is more unusual. This could indicate a versatile player, a specialist, or a player with inflated/deflated stats.',
    actions: [
      'Review flagged anomalies manually \u2014 high-scoring anomalies with good traditional stats may be undervalued',
      'Anomalous goalkeepers or defenders with attacking stats may indicate tactical versatility worth exploiting',
      'Players who are anomalies with poor traditional stats may be misused and could benefit from a role change',
    ],
    fineTuning: 'The contamination rate (default 5%) controls how many anomalies are flagged. Increase to 10% for a broader scan, decrease to 2% for only the most extreme outliers. If too many goalkeepers are flagged, consider separate models by position.',
  };

  // F1 models
  if (outputTable?.includes('f1_driver_predictions') || (key.includes('f1_driver') && modelType === 'predictions')) return {
    title: 'F1 Driver Race Performance',
    overview: 'A GradientBoosting model predicts each driver\'s race finish position from their grid slot, qualifying performance, and historical data. The residual reveals which drivers consistently outperform or underperform their starting position.',
    whatItShows: 'Drivers with positive residuals (actual better than predicted) are extracting more from their car than expected \u2014 indicating superior racecraft, tyre management, or wet-weather ability. Negative residuals suggest a driver is losing places from their grid slot.',
    actions: [
      'Drivers with consistently positive residuals across seasons are elite race performers \u2014 prioritize for team signings',
      'Grid position is the strongest predictor, but drivers who overcome poor qualifying are strategically valuable',
      'Compare residuals between teammates to isolate driver skill from car performance',
    ],
    fineTuning: 'The model uses only finishers (DNFs excluded). To include reliability, add a separate DNF prediction model. Consider era-specific models since F1 regulations change significantly across decades.',
  };

  if (outputTable?.includes('f1_constructor_rankings') || (key.includes('f1_constructor') && modelType === 'clusters')) return {
    title: 'F1 Constructor Dominance Rankings',
    overview: 'A composite dominance score ranks every F1 constructor across their full history, weighted by win rate (30%), podium rate (25%), points per race (20%), reliability (15%), and average finish position (10%). K-Means clustering identifies constructor archetypes.',
    whatItShows: 'The rankings separate truly dominant constructors from merely competitive ones. Archetypes reveal different paths to success \u2014 some teams win through raw pace, others through reliability and consistency.',
    actions: [
      'Compare your constructor\'s archetype to the dominant cluster to identify what metrics to prioritize',
      'Reliability is weighted 15% but separates good teams from great ones \u2014 a fast but fragile car leaves points on the table',
      'Use per-season data to identify when constructors entered or exited their dominant era \u2014 regulation changes are key inflection points',
    ],
    fineTuning: 'The composite weights (30/25/20/15/10) reflect modern F1 value. For historical analysis pre-2003, consider adjusting points_per_race weight since point systems changed. Increase k to 6+ if archetypes are too broad.',
  };

  if (outputTable?.includes('f1_pitstop') || (key.includes('f1_pitstop') && modelType === 'predictions')) return {
    title: 'F1 Pit Stop Strategy Impact',
    overview: 'A GradientBoosting model predicts positions gained or lost based on pit stop strategy \u2014 number of stops, stop speed, timing, and grid position. Feature importances quantify whether faster pit crews or better strategy timing matters more.',
    whatItShows: 'The model separates the impact of pit execution (how fast the stops are) from pit strategy (when and how many times to stop). Per-constructor pit stats reveal which teams have the fastest crews and which gain the most through strategy.',
    actions: [
      'If pit duration importance is high, invest in pit crew training and equipment \u2014 each second costs approximately 0.5 positions',
      'First stop timing (lap number) importance shows whether early or late stopping strategies gain more positions',
      'Compare constructor pit stats to identify which teams have a strategic advantage vs a mechanical advantage',
    ],
    fineTuning: 'The model excludes stops over 120 seconds (penalties, red flags). Safety car pit stops are included but distort timing importance \u2014 consider adding a safety_car flag if available. Grid position dominates predictions; try a model without grid to isolate pure strategy impact.',
  };

  if (outputTable?.includes('f1_driver_feature_importances')) return {
    title: 'What Predicts F1 Race Results?',
    overview: 'Feature importance rankings from the driver performance model show which factors most strongly predict where a driver finishes. Grid position typically dominates, but the remaining features reveal what separates good race days from bad ones.',
    whatItShows: 'Higher importance = stronger predictive power. After grid position, qualifying performance and race distance (laps) contribute most. Year captures era effects \u2014 modern F1 has less overtaking, making grid position even more dominant.',
    actions: [
      'If grid importance exceeds 0.70, qualifying performance is paramount \u2014 one-lap pace is the biggest differentiator',
      'Laps/race distance importance suggests endurance and tyre management matter at specific circuits',
      'Use these importances to focus driver development programs on the highest-impact skills',
    ],
    fineTuning: null,
  };

  if (outputTable?.includes('f1_pitstop_feature_importances')) return {
    title: 'Pit Strategy: Speed vs Timing',
    overview: 'Feature importances from the pit stop model show whether fast pit execution or strategic timing contributes more to gaining positions during a race.',
    whatItShows: 'Grid position captures the overall competitive position; after that, stop count and timing reveal strategic value. If avg_stop_duration ranks high, pit crew performance is a genuine competitive advantage.',
    actions: [
      'If stop timing (first_stop_lap) outranks stop duration, strategy calls matter more than pit crew speed',
      'If total_stops is important, the number of stops (1-stop vs 2-stop) is a key strategic decision',
      'Use per-constructor breakdown to benchmark your pit crew against the field',
    ],
    fineTuning: null,
  };

  if (outputTable?.includes('f1_constructor_pit_stats')) return {
    title: 'Constructor Pit Crew Performance',
    overview: 'Per-constructor, per-season pit stop statistics showing average and fastest stop durations, total stops, and the average positions gained through pit windows.',
    whatItShows: 'Constructors with lower average pit durations and higher positions gained have a combined strategic and mechanical advantage. Comparing across years shows which teams are improving their pit operations.',
    actions: [
      'Benchmark pit duration against the top 3 teams to quantify the gap',
      'Track year-over-year improvement \u2014 teams investing in pit crew training show measurable duration decreases',
      'Cross-reference with positions gained to see if faster stops translate to actual race positions',
    ],
    fineTuning: null,
  };

  // NRL models
  if (outputTable?.includes('nrl_team_rankings') || (key.includes('nrl') && key.includes('dominance'))) return {
    title: 'NRL Team Dominance Rankings',
    overview: 'A composite dominance score ranks every NRL team across 35 years of fixtures (1990\u20132025), weighted by win rate (25%), average margin (20%), away win rate (15%), close-game clutch (15%), blowout rate (10%), defensive strength (10%), and longevity (5%). K-Means clustering identifies team archetypes.',
    whatItShows: 'The rankings separate dynasties from contenders and competitive teams. An era-adjusted modern dominance score weights recent decades higher, so teams dominating now rank differently from all-time greats. Archetypes reveal whether a team wins through grinding consistency or explosive attacking.',
    actions: [
      'Compare your team\'s archetype to the dynasty cluster to identify which metrics to prioritize',
      'Use modern dominance score alongside all-time ranking to distinguish current form from historical reputation',
      'Check the per-season breakdown to identify when your team entered or exited its peak era',
    ],
    fineTuning: 'The composite weights (25/20/15/15/10/10/5) are tuned for NRL fixtures data. If close-game win rate seems over-weighted, reduce to 10% and increase margin weight. The era-adjustment uses a 10-year half-life \u2014 increase to 15 years for a more historically balanced view.',
  };

  if (outputTable?.includes('nrl_match_predictions') || (key.includes('nrl') && key.includes('match') && modelType === 'predictions')) return {
    title: 'NRL Match Outcome Predictor',
    overview: 'A GradientBoosting model predicts match margins from rolling 5-game form, season-to-date performance, head-to-head history, and home advantage. Residual analysis reveals which teams consistently beat or underperform model expectations.',
    whatItShows: 'Feature importances show what actually drives NRL match outcomes \u2014 is it recent form, home ground advantage, or head-to-head psychological edges? Teams with positive overperformance scores have an X factor the model can\'t explain: culture, coaching quality, or mental toughness.',
    actions: [
      'Check feature importances to prioritise preparation: if h2h_home_win_rate ranks high, game plans should be opponent-specific',
      'Teams with consistent overperformance are genuinely elite \u2014 their wins aren\'t just from easy draws',
      'Underperforming teams may be coasting on favourable schedules \u2014 expect regression in tougher fixtures',
    ],
    fineTuning: 'The model uses a 5-game rolling window. Increase to 8 for more stable form estimates, or decrease to 3 for more reactive predictions. Head-to-head features may overfit for rare matchups \u2014 consider filtering to pairs with 10+ meetings.',
  };

  if (outputTable?.includes('nrl_match_feature_importances')) return {
    title: 'What Predicts NRL Match Results?',
    overview: 'Feature importance rankings from the match prediction model show which factors most strongly predict the margin between home and away teams.',
    whatItShows: 'Higher importance = stronger predictive power. Recent form (last 5 games) typically dominates, but the balance between home advantage, season form, and head-to-head history reveals what coaches should focus on.',
    actions: [
      'If home_season_home_win_rate ranks high, home ground advantage is a genuine factor \u2014 schedule accordingly',
      'If form_differential dominates, recent momentum matters most \u2014 focus on building winning streaks',
      'Use these importances to weight pre-match analysis: spend time on the factors that actually predict outcomes',
    ],
    fineTuning: null,
  };

  if (outputTable?.includes('nrl_team_overperformance')) return {
    title: 'NRL Team Overperformance',
    overview: 'Teams ranked by how much they consistently beat or fall short of model predictions. Positive overperformance means the team wins by more (or loses by less) than their stats suggest they should.',
    whatItShows: 'Overperforming teams have intangibles the model can\'t capture \u2014 coaching, culture, mental toughness, or clutch play. Underperformers may have flattering stats but lack the ability to convert advantages into results.',
    actions: [
      'Top overperformers are the teams to study for coaching methodology and team culture',
      'Underperforming teams should audit whether their training translates to match-day execution',
      'Use this alongside the dominance rankings to separate genuine quality from statistical illusion',
    ],
    fineTuning: null,
  };

  if (outputTable?.includes('nrl_team_profiles')) return {
    title: 'NRL Team Playing Styles',
    overview: 'K-Means clustering on tactical features identifies distinct playing styles across all NRL teams. Each team gets a SWOT assessment: strengths (metrics above 75th percentile) and weaknesses (below 25th percentile).',
    whatItShows: 'Playing style clusters reveal whether a team is a consistent grinder, high-scoring attacker, home specialist, clutch performer, or late-season surger. The SWOT breakdown shows exactly where a team excels and where it\'s vulnerable.',
    actions: [
      'Compare your team\'s style to the most successful cluster \u2014 should you adapt or lean into your identity?',
      'Target weaknesses for off-season improvement: low bounce-back rate suggests mental resilience training',
      'High home dependency means away game preparation needs specific attention',
    ],
    fineTuning: 'Style labels are auto-assigned based on the most distinguishing feature per cluster. If labels seem off, check the underlying tactical metrics for each cluster to understand the true separation.',
  };

  if (outputTable?.includes('nrl_rivalry_matrix')) return {
    title: 'NRL Head-to-Head Rivalry Matrix',
    overview: 'Complete head-to-head records between all NRL team pairs across 35 years, including home/away splits and dominance flags for statistically significant edges (>60% win rate with 10+ meetings).',
    whatItShows: 'Rivalry records reveal psychological edges that persist across eras. A team with a dominant head-to-head record against a specific opponent has a structural advantage worth exploiting in preparation.',
    actions: [
      'Flag opponents where your win rate is below 40% \u2014 these rivalries need specific tactical game plans',
      'Compare home vs away win rates per rival to identify venue-specific advantages',
      'Use long-term trends to assess whether a rivalry dynamic is shifting in your favour',
    ],
    fineTuning: null,
  };

  if (outputTable?.includes('nrl_team_trends')) return {
    title: 'NRL Team Trajectory Analysis',
    overview: 'Rolling 5-year windows track each team\'s performance trajectory over time. Teams are classified as improving, declining, or stable based on the slope of their win rate across windows.',
    whatItShows: 'Trajectory analysis separates teams on the rise from those declining \u2014 independent of their current ranking. A low-ranked team with an improving trajectory is a different proposition to a high-ranked team in decline.',
    actions: [
      'Improving teams are building something \u2014 study what changed (coaching, roster, culture) during the inflection',
      'Declining teams should compare recent windows to their peak to identify what regressed',
      'Stable teams have found their level \u2014 breakthrough requires a structural change, not incremental improvement',
    ],
    fineTuning: 'The 5-year window smooths single-season noise but may lag rapid changes. Try 3-year windows for more responsive trend detection.',
  };

  if (modelType === 'predictions' && modelName === 'player') return {
    title: 'Player Rating Prediction',
    overview: 'A GradientBoosting model predicts each player\'s match rating from their performance statistics. The residual (predicted - actual) reveals players whose stats suggest they should be rated higher or lower than they are.',
    whatItShows: 'Positive residuals mean the model expects a higher rating than the player received \u2014 these players\' stats are better than their rating reflects. Negative residuals indicate players whose rating exceeds what their stats justify.',
    actions: [
      'Players with large positive residuals are statistically undervalued \u2014 potential transfer targets',
      'Players with negative residuals may be benefiting from reputation or team quality rather than individual performance',
      'Feature importances show which stats most influence ratings \u2014 focus player development on high-importance skills',
    ],
    fineTuning: 'R\u00B2 of ~0.60-0.70 is typical for rating prediction. If specific positions have poor predictions, consider position-specific models. Adding expected goals/assists as features may improve prediction for attackers.',
  };

  return {
    title: 'Model Analysis',
    overview: 'This model provides data-driven insights from your uploaded datasets.',
    whatItShows: 'The table below shows the model\'s output. Review the values to identify patterns and actionable insights.',
    actions: ['Review the output data for patterns', 'Compare values across records to identify outliers'],
    fineTuning: null,
  };
}
