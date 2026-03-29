import { createContext, useContext, useEffect, useState, useCallback, ReactNode } from 'react';
import { AuthUser } from '../types';

interface AuthContextValue {
  user: AuthUser | null;
  loading: boolean;
  gsiReady: boolean;
  signInAsGuest: () => void;
  signOutUser: () => void;
  renderGoogleButton: (element: HTMLElement) => void;
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
let _credential: string | null = sessionStorage.getItem('gis_credential');
export function getGoogleCredential(): string | null {
  return _credential;
}

function loadPersistedUser(): AuthUser | null {
  try {
    const stored = sessionStorage.getItem('auth_user');
    return stored ? JSON.parse(stored) : null;
  } catch {
    return null;
  }
}

function persistAuth(user: AuthUser | null, credential: string | null) {
  if (user) {
    sessionStorage.setItem('auth_user', JSON.stringify(user));
  } else {
    sessionStorage.removeItem('auth_user');
  }
  if (credential) {
    sessionStorage.setItem('gis_credential', credential);
  } else {
    sessionStorage.removeItem('gis_credential');
  }
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(loadPersistedUser);
  const [loading, setLoading] = useState(() => !loadPersistedUser() && !!CLIENT_ID);
  const [gsiReady, setGsiReady] = useState(false);

  const handleCredentialResponse = useCallback((response: GoogleCredentialResponse) => {
    const payload = parseJwt(response.credential);
    _credential = response.credential;
    const newUser: AuthUser = {
      uid: payload.sub,
      email: payload.email ?? null,
      displayName: payload.name ?? null,
      photoURL: payload.picture ?? null,
      role: 'google',
    };
    persistAuth(newUser, response.credential);
    setUser(newUser);
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
      setGsiReady(true);
      setLoading(false);
    };
    script.onerror = () => setLoading(false);
    document.head.appendChild(script);

    return () => {
      document.head.removeChild(script);
    };
  }, [handleCredentialResponse]);

  const renderGoogleButton = useCallback((element: HTMLElement) => {
    if (!gsiReady) return;
    window.google.accounts.id.renderButton(element, {
      type: 'standard',
      theme: 'outline',
      size: 'large',
      width: element.offsetWidth,
      text: 'signin_with',
    });
  }, [gsiReady]);

  const signInAsGuest = useCallback(() => {
    _credential = null;
    persistAuth(GUEST_USER, null);
    setUser(GUEST_USER);
    setLoading(false);
  }, []);

  const signOutUser = useCallback(() => {
    if (CLIENT_ID && window.google?.accounts?.id) {
      window.google.accounts.id.disableAutoSelect();
    }
    _credential = null;
    persistAuth(null, null);
    setUser(null);
  }, []);

  return (
    <AuthContext.Provider value={{ user, loading, gsiReady, signInAsGuest, signOutUser, renderGoogleButton }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used inside AuthProvider');
  return ctx;
}
