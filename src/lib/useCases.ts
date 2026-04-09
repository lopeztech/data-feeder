export type UseCase = 'all' | 'f1' | 'nfl' | 'nrl' | 'european-football' | 'other';

export const USE_CASE_META: Record<Exclude<UseCase, 'all'>, { label: string; icon: string; patterns: string[] }> = {
  f1: {
    label: 'Formula 1',
    icon: '🏎️',
    patterns: ['f1_', 'formula1', 'formula_1', 'circuits', 'constructors', 'drivers', 'lap_times', 'pit_stops', 'qualifying', 'races', 'results', 'seasons', 'sprint_results', 'status', 'constructor_results', 'constructor_standings', 'driver_standings', 'f1_driver', 'f1_constructor', 'f1_pitstop'],
  },
  nfl: {
    label: 'NFL',
    icon: '🏈',
    patterns: ['nfl_', 'team_win', 'team_archetype', 'team_optimal', 'team_feature', 'positional_value', 'team_dominance'],
  },
  nrl: {
    label: 'NRL',
    icon: '🏉',
    patterns: ['nrl_'],
  },
  'european-football': {
    label: 'European Football',
    icon: '⚽',
    patterns: ['all_player_', 'player_'],
  },
  other: {
    label: 'Other',
    icon: '📊',
    patterns: [],
  },
};

const USE_CASE_ENTRIES = Object.entries(USE_CASE_META) as [Exclude<UseCase, 'all'>, typeof USE_CASE_META[keyof typeof USE_CASE_META]][];

/** Detect use case from a string (dataset name, model name, table name, etc.) */
export function detectUseCaseFromString(name: string): Exclude<UseCase, 'all'> {
  const lower = name.toLowerCase();
  for (const [uc, meta] of USE_CASE_ENTRIES) {
    if (meta.patterns.some(p => lower.includes(p))) return uc;
  }
  return 'other';
}

/** Detect use case from multiple strings (model name + source tables, etc.) */
export function detectUseCase(names: string[]): Exclude<UseCase, 'all'> {
  return detectUseCaseFromString(names.join(' '));
}
