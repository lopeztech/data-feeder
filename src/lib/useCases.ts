export type UseCase = 'all' | 'nfl' | 'european-football';

export const USE_CASE_META: Record<Exclude<UseCase, 'all'>, { label: string; icon: string; patterns: string[] }> = {
  nfl: {
    label: 'NFL',
    icon: '🏈',
    patterns: ['nfl_', 'team_win', 'team_archetype', 'team_optimal', 'team_feature', 'positional_value', 'team_dominance'],
  },
  'european-football': {
    label: 'European Football',
    icon: '⚽',
    patterns: ['all_player_', 'player_'],
  },
};

const USE_CASE_ENTRIES = Object.entries(USE_CASE_META) as [Exclude<UseCase, 'all'>, typeof USE_CASE_META[keyof typeof USE_CASE_META]][];

/** Detect use case from a string (dataset name, model name, table name, etc.) */
export function detectUseCaseFromString(name: string): Exclude<UseCase, 'all'> {
  const lower = name.toLowerCase();
  for (const [uc, meta] of USE_CASE_ENTRIES) {
    if (meta.patterns.some(p => lower.includes(p))) return uc;
  }
  return 'european-football';
}

/** Detect use case from multiple strings (model name + source tables, etc.) */
export function detectUseCase(names: string[]): Exclude<UseCase, 'all'> {
  return detectUseCaseFromString(names.join(' '));
}
