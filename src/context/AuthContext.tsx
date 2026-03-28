import { createContext, useContext, useEffect, useState, useCallback, ReactNode } from 'react';
import { AuthUser } from '../types';

interface AuthContextValue {
  user: AuthUser | null;
  loading: boolean;
  signInWithGoogle: () => void;
  signInAsGuest: () => void;
  signOutUser: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

const GUEST_USER: AuthUser = {
  uid: 'guest',
  email: null,
  displayName: 'Guest',
  photoURL: null,
  role: 'guest',
};

const CLIENT_ID = import.meta.env.VITE_GOOGLE_CLIENT_ID;

interface GoogleCredentialResponse {
  credential: string;
}

function parseJwt(token: string): Record<string, string> {
  const base64Url = token.split('.')[1];
  const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
  const json = decodeURIComponent(
    atob(base64)
      .split('')
      .map((c) => '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2))
      .join(''),
  );
  return JSON.parse(json);
}

// Store the credential so uploadService can retrieve it
let _credential: string | null = null;
export function getGoogleCredential(): string | null {
  return _credential;
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(null);
  const [loading, setLoading] = useState(() => !!CLIENT_ID);

  const handleCredentialResponse = useCallback((response: GoogleCredentialResponse) => {
    const payload = parseJwt(response.credential);
    _credential = response.credential;
    setUser({
      uid: payload.sub,
      email: payload.email ?? null,
      displayName: payload.name ?? null,
      photoURL: payload.picture ?? null,
      role: 'google',
    });
    setLoading(false);
  }, []);

  useEffect(() => {
    if (!CLIENT_ID) return;

    const script = document.createElement('script');
    script.src = 'https://accounts.google.com/gsi/client';
    script.async = true;
    script.onload = () => {
      window.google.accounts.id.initialize({
        client_id: CLIENT_ID,
        callback: handleCredentialResponse,
        auto_select: true,
      });
      setLoading(false);
    };
    script.onerror = () => setLoading(false);
    document.head.appendChild(script);

    return () => {
      document.head.removeChild(script);
    };
  }, [handleCredentialResponse]);

  const signInWithGoogle = useCallback(() => {
    if (!CLIENT_ID) throw new Error('Google Client ID is not configured.');
    window.google.accounts.id.prompt();
  }, []);

  const signInAsGuest = useCallback(() => {
    _credential = null;
    setUser(GUEST_USER);
    setLoading(false);
  }, []);

  const signOutUser = useCallback(() => {
    if (CLIENT_ID && window.google?.accounts?.id) {
      window.google.accounts.id.disableAutoSelect();
    }
    _credential = null;
    setUser(null);
  }, []);

  return (
    <AuthContext.Provider value={{ user, loading, signInWithGoogle, signInAsGuest, signOutUser }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used inside AuthProvider');
  return ctx;
}
