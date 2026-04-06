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
