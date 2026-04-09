import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { AuthProvider } from './context/AuthContext';
import ProtectedRoute from './components/ProtectedRoute';
import Layout from './components/Layout';
import LoginPage from './pages/LoginPage';
import UploadPage from './pages/UploadPage';
import JobsPage from './pages/JobsPage';
import InsightsListPage from './pages/InsightsListPage';
import InsightsPage from './pages/InsightsPage';
import ReportPage from './pages/ReportPage';
import NFLTeamsPage from './pages/NFLTeamsPage';
import NRLTeamsPage from './pages/NRLTeamsPage';

const queryClient = new QueryClient();

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <BrowserRouter>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
            <Route element={<ProtectedRoute />}>
              <Route element={<Layout />}>
                <Route path="/upload" element={<UploadPage />} />
                <Route path="/jobs" element={<JobsPage />} />
                <Route path="/insights" element={<InsightsListPage />} />
                <Route path="/insights/:type/:model" element={<InsightsPage />} />
                <Route path="/insights/:type/:model/report" element={<ReportPage />} />
                <Route path="/nfl-teams" element={<NFLTeamsPage />} />
                <Route path="/nrl-teams" element={<NRLTeamsPage />} />
              </Route>
            </Route>
            <Route path="*" element={<Navigate to="/insights" replace />} />
          </Routes>
        </BrowserRouter>
      </AuthProvider>
    </QueryClientProvider>
  );
}
