import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { fetchModels } from '../lib/uploadService';
import type { ModelInfo, ModelType } from '../lib/uploadService';
import { MOCK_MODELS } from '../data/mockClusters';
import { detectUseCase, USE_CASE_META } from '../lib/useCases';
import type { UseCase } from '../lib/useCases';
import { getNarrative } from '../lib/narratives';

const TYPE_META: Record<ModelType, { label: string; badge: string }> = {
  clusters: { label: 'Clustering', badge: 'bg-blue-100 text-blue-700' },
  anomalies: { label: 'Anomaly Detection', badge: 'bg-amber-100 text-amber-700' },
  predictions: { label: 'Regression', badge: 'bg-green-100 text-green-700' },
  profile: { label: 'Profile Analysis', badge: 'bg-purple-100 text-purple-700' },
};

export default function InsightsListPage() {
  const { user } = useAuth();
  const navigate = useNavigate();
  const isGuest = user?.role === 'guest';
  const [cards, setCards] = useState<ModelInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [useCaseFilter, setUseCaseFilter] = useState<UseCase>('all');

  useEffect(() => {
    if (isGuest) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- sync mock data for guest mode
      setCards(MOCK_MODELS);
      return;
    }
    setLoading(true);
    fetchModels()
      .then(setCards)
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [isGuest]);

  return (
    <div className="p-4 sm:p-8 max-w-6xl mx-auto">
      <div className="mb-8">
        <h1 className="text-xl sm:text-2xl font-bold text-gray-900">Insights</h1>
        <p className="text-gray-500 mt-1 text-sm">
          Select a model to view its data lineage and ML insights.
        </p>
      </div>

      {isGuest && (
        <div className="p-4 mb-6 bg-amber-50 border border-amber-200 rounded-xl">
          <p className="text-sm font-medium text-amber-800">You're viewing demo data. Sign in with Google to see live models from your pipeline.</p>
        </div>
      )}

      {loading && (
        <div className="flex items-center justify-center py-20">
          <div className="w-8 h-8 border-4 border-brand-600 border-t-transparent rounded-full animate-spin" />
        </div>
      )}

      {error && (
        <div className="p-4 bg-red-50 border border-red-200 rounded-xl">
          <p className="text-sm text-red-700">{error}</p>
        </div>
      )}

      {!loading && !error && cards.length === 0 && (
        <div className="text-center py-20">
          <svg className="w-12 h-12 text-gray-300 mx-auto mb-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
          </svg>
          <p className="text-sm text-gray-500">No ML models found.</p>
          <p className="text-xs text-gray-400 mt-1">Upload data and run the ML pipeline to generate models.</p>
        </div>
      )}

      {!loading && !error && cards.length > 0 && (() => {
        const tagged = cards.map(c => ({ ...c, useCase: detectUseCase([c.model, c.outputTable, ...c.sourceTables]) }));
        const useCases = [...new Set(tagged.map(c => c.useCase))];
        const filtered = useCaseFilter === 'all' ? tagged : tagged.filter(c => c.useCase === useCaseFilter);
        const groups = useCases
          .filter(uc => useCaseFilter === 'all' || uc === useCaseFilter)
          .map(uc => ({ uc, cards: filtered.filter(c => c.useCase === uc) }))
          .filter(g => g.cards.length > 0);

        return (
          <>
            {/* Use case filter tabs */}
            {useCases.length > 1 && (
              <div className="flex gap-2 mb-6 flex-wrap">
                <button
                  onClick={() => setUseCaseFilter('all')}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
                    useCaseFilter === 'all' ? 'bg-brand-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  All
                </button>
                {useCases.map(uc => {
                  const meta = USE_CASE_META[uc];
                  return (
                    <button
                      key={uc}
                      onClick={() => setUseCaseFilter(uc)}
                      className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
                        useCaseFilter === uc ? 'bg-brand-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                      }`}
                    >
                      {meta.icon} {meta.label}
                    </button>
                  );
                })}
              </div>
            )}

            {groups.map(({ uc, cards: groupCards }) => {
              const ucMeta = USE_CASE_META[uc];
              return (
                <div key={uc} className="mb-8">
                  <div className="flex items-center gap-2 mb-4">
                    <span className="text-lg">{ucMeta.icon}</span>
                    <h2 className="text-lg font-semibold text-gray-900">{ucMeta.label}</h2>
                    <span className="text-xs text-gray-400">{groupCards.length} {groupCards.length === 1 ? 'model' : 'models'}</span>
                  </div>
                  <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                    {uc === 'nfl' && (
                      <button
                        key="nfl-teams"
                        onClick={() => navigate('/nfl-teams')}
                        className="text-left bg-white border border-gray-200 rounded-xl p-5 hover:border-brand-300 hover:shadow-md transition-all group"
                      >
                        <div className="flex items-center gap-3 mb-3">
                          <div className="w-9 h-9 rounded-lg bg-brand-50 flex items-center justify-center group-hover:bg-brand-100 transition-colors">
                            <svg className="w-4.5 h-4.5 text-brand-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                            </svg>
                          </div>
                          <div className="flex-1 min-w-0">
                            <h3 className="font-semibold text-gray-900 text-sm truncate">NFL Teams</h3>
                            <span className="inline-block mt-0.5 px-2 py-0.5 rounded-full text-xs font-medium bg-indigo-100 text-indigo-700">
                              Composite Analysis
                            </span>
                          </div>
                          <svg className="w-4 h-4 text-gray-300 group-hover:text-brand-500 transition-colors" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                          </svg>
                        </div>
                        <p className="text-sm font-medium text-gray-700 mb-2">Team Dominance Rankings</p>
                        <p className="text-xs text-gray-500 leading-relaxed mb-3 line-clamp-3">
                          Composite dominance scoring ranks all NFL teams across winning record, offensive output, defensive strength, and efficiency. Identifies the best teams historically and the stats that separate elite from average.
                        </p>
                        <div className="border-t border-gray-100 pt-2">
                          <p className="text-xs font-medium text-gray-500 mb-1">Key actions:</p>
                          <p className="text-xs text-gray-400 line-clamp-2">Compare team pillars (winning, offence, defence, efficiency) to identify strengths and weaknesses for roster decisions</p>
                        </div>
                      </button>
                    )}
                    {groupCards.map(c => {
                      const meta = TYPE_META[c.type];
                      return (
                        <button
                          key={`${c.model}-${c.type}`}
                          onClick={() => navigate(`/insights/${encodeURIComponent(c.type)}/${encodeURIComponent(c.model)}`)}
                          className="text-left bg-white border border-gray-200 rounded-xl p-5 hover:border-brand-300 hover:shadow-md transition-all group"
                        >
                          <div className="flex items-center gap-3 mb-3">
                            <div className="w-9 h-9 rounded-lg bg-brand-50 flex items-center justify-center group-hover:bg-brand-100 transition-colors">
                              <svg className="w-4.5 h-4.5 text-brand-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                              </svg>
                            </div>
                            <div className="flex-1 min-w-0">
                              <h3 className="font-semibold text-gray-900 text-sm truncate">{c.model}</h3>
                              <span className={`inline-block mt-0.5 px-2 py-0.5 rounded-full text-xs font-medium ${meta.badge}`}>
                                {meta.label}
                              </span>
                            </div>
                            <svg className="w-4 h-4 text-gray-300 group-hover:text-brand-500 transition-colors" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                            </svg>
                          </div>

                          {(() => {
                            const narrative = getNarrative(c.model, c.type, c.outputTable);
                            return (
                              <>
                                <p className="text-sm font-medium text-gray-700 mb-2">{narrative.title}</p>
                                <p className="text-xs text-gray-500 leading-relaxed mb-3 line-clamp-3">{narrative.overview}</p>
                                <div className="border-t border-gray-100 pt-2">
                                  <p className="text-xs font-medium text-gray-500 mb-1">Key actions:</p>
                                  <p className="text-xs text-gray-400 line-clamp-2">{narrative.actions[0]}</p>
                                </div>
                              </>
                            );
                          })()}
                        </button>
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </>
        );
      })()}
    </div>
  );
}
