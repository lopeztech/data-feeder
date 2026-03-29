import { useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';

export default function LoginPage() {
  const { user, signInAsGuest, gsiReady, renderGoogleButton } = useAuth();
  const navigate = useNavigate();
  const googleBtnRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (gsiReady && googleBtnRef.current) {
      renderGoogleButton(googleBtnRef.current);
    }
  }, [gsiReady, renderGoogleButton]);

  // Navigate once user is set (after Google callback fires)
  useEffect(() => {
    if (user && user.role === 'google') navigate('/upload');
  }, [user, navigate]);

  const handleGuest = () => {
    signInAsGuest();
    navigate('/jobs');
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-brand-700 to-blue-900 flex items-center justify-center p-4">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md p-10">
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 bg-brand-600 rounded-2xl mb-4">
            <svg className="w-8 h-8 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
            </svg>
          </div>
          <h1 className="text-2xl font-bold text-gray-900">Data Feeder</h1>
          <p className="text-gray-500 mt-1 text-sm">GCP Data Ingestion Platform</p>
        </div>

        <div className="space-y-3">
          {/* Google Identity Services rendered button */}
          <div ref={googleBtnRef} className="flex justify-center" />

          <div className="relative">
            <div className="absolute inset-0 flex items-center">
              <div className="w-full border-t border-gray-200" />
            </div>
            <div className="relative flex justify-center text-xs text-gray-400">
              <span className="bg-white px-2">or</span>
            </div>
          </div>

          <button
            onClick={handleGuest}
            className="w-full flex items-center justify-center gap-2 bg-gray-100 hover:bg-gray-200 rounded-lg px-4 py-3 text-sm font-medium text-gray-600 transition"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
            </svg>
            Continue as Guest (demo mode)
          </button>
        </div>

        <p className="mt-6 text-xs text-gray-400 text-center">
          Guest mode uses mock data. Uploading new files requires a Google account.
        </p>
      </div>
    </div>
  );
}
