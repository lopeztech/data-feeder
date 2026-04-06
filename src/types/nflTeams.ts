export interface NFLTeamRanking {
  rank: number;
  team: string;
  composite_score: number;
  mean_dominance: number;
  peak_dominance: number;
  seasons_top_8: number;
  total_wins: number;
  total_losses: number;
  avg_win_pct: number;
  avg_points_per_game: number;
  avg_points_opp_per_game: number;
  best_season_year: number;
}

export interface NFLTeamSeason {
  year: number;
  team: string;
  dominance_score: number;
  pillar_winning: number;
  pillar_offence: number;
  pillar_defence: number;
  pillar_efficiency: number;
  win_loss_perc: number;
  is_elite: number;
}

export interface NFLDominanceDriver {
  feature_name: string;
  importance: number;
  rank: number;
  elite_mean: number;
  league_mean: number;
  elite_advantage: number;
}

export interface NFLTeamAnalysisData {
  rankings: NFLTeamRanking[];
  seasons: NFLTeamSeason[];
  drivers: NFLDominanceDriver[];
}
