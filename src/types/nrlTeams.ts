export interface NRLTeamRanking {
  rank: number;
  team: string;
  composite_score: number;
  modern_dominance: number;
  mean_win_rate: number;
  mean_avg_margin: number;
  mean_home_win_rate: number;
  mean_away_win_rate: number;
  mean_close_game_win_rate: number;
  mean_blowout_rate: number;
  mean_avg_points_for: number;
  mean_avg_points_against: number;
  total_wins: number;
  total_games: number;
  seasons_active: number;
  peak_season: number;
  archetype_id: number;
  archetype_label: string;
}

export interface NRLTeamSeason {
  year: number;
  team: string;
  games_played: number;
  wins: number;
  draws: number;
  losses: number;
  win_rate: number;
  avg_points_for: number;
  avg_points_against: number;
  avg_margin: number;
  home_win_rate: number;
  away_win_rate: number;
  blowout_rate: number;
  close_game_win_rate: number;
  points_differential: number;
}

export interface NRLTeamProfile {
  team: string;
  playing_style: string;
  style_id: number;
  strengths: string;
  weaknesses: string;
  win_rate: number;
  avg_margin: number;
  consistency_score: number;
  home_dependency: number;
  bounce_back_rate: number;
  streak_maintenance_rate: number;
  close_game_win_rate: number;
  blowout_loss_rate: number;
  attack_defense_ratio: number;
  early_season_win_rate: number;
  late_season_win_rate: number;
}

export interface NRLRivalry {
  team: string;
  opponent: string;
  total_matches: number;
  wins: number;
  losses: number;
  win_rate: number;
  avg_margin: number;
  home_win_rate: number;
  is_dominant: number;
}

export interface NRLTeamTrend {
  team: string;
  window_start: number;
  window_end: number;
  seasons_in_window: number;
  avg_win_rate: number;
  avg_margin: number;
  avg_close_game_wr: number;
  avg_bounce_back: number;
  trajectory: string;
}

export interface NRLTeamAnalysisData {
  rankings: NRLTeamRanking[];
  seasons: NRLTeamSeason[];
  profiles: NRLTeamProfile[];
  rivalries: NRLRivalry[];
  trends: NRLTeamTrend[];
}
