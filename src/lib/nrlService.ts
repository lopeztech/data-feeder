import { getGoogleCredential } from '../context/AuthContext';
import type { NRLTeamAnalysisData } from '../types/nrlTeams';

const API_BASE = import.meta.env.VITE_UPLOAD_API_URL || '/api/uploads';

export async function fetchNRLTeamAnalysis(): Promise<NRLTeamAnalysisData> {
  const token = getGoogleCredential();
  const headers: Record<string, string> = {};
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(`${API_BASE}/nrl-team-analysis`, { headers });
  if (!res.ok) throw new Error(`NRL team analysis fetch failed (${res.status})`);
  return res.json() as Promise<NRLTeamAnalysisData>;
}
