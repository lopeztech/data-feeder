import { getGoogleCredential } from '../context/AuthContext';
import type { NFLTeamAnalysisData } from '../types/nflTeams';

const API_BASE = import.meta.env.VITE_UPLOAD_API_URL || '/api/uploads';

export async function fetchNFLTeamAnalysis(): Promise<NFLTeamAnalysisData> {
  const token = getGoogleCredential();
  const headers: Record<string, string> = {};
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(`${API_BASE}/nfl-team-analysis`, { headers });
  if (!res.ok) throw new Error(`NFL team analysis fetch failed (${res.status})`);
  return res.json() as Promise<NFLTeamAnalysisData>;
}
