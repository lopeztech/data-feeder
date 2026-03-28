# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm run dev        # Start dev server (http://localhost:5173)
npm run build      # TypeScript check + production build
npm run lint       # ESLint
npm run preview    # Preview production build locally
```

## Environment

Copy `.env.example` to `.env.local` and fill in Firebase values. Without them the app still runs — Google sign-in will fail but Guest mode works fully.

## Architecture

React 18 + Vite + TypeScript SPA. Tailwind CSS v3 for styling. React Router v7 for routing. Firebase for auth. React Query for server state.

### Auth model
Two entry points on the login page:
- **Google OAuth** — Firebase `signInWithPopup`. On success, user gets `role: 'google'` and full access (upload + jobs).
- **Guest** — no Firebase call; sets a synthetic `GUEST_USER` in `AuthContext` with `role: 'guest'`. All upload controls are disabled; jobs page shows `MOCK_JOBS` from `src/data/mockJobs.ts`.

`AuthContext` (`src/context/AuthContext.tsx`) exposes `user`, `signInWithGoogle`, `signInAsGuest`, `signOutUser`. Check `user.role === 'guest'` to gate features.

### Route structure
```
/login          → LoginPage (public)
/upload         → UploadPage (protected, Google only can submit)
/jobs           → JobsPage  (protected, guests see mock data)
```

`ProtectedRoute` redirects unauthenticated users to `/login`. `Layout` wraps all protected routes with the sidebar.

### Data flow (design intent)
Upload page → calls `POST /api/uploads/init` → receives GCS signed URL → browser uploads directly to GCS Bronze bucket → GCS notifies Pub/Sub → Cloud Function validates (Bronze→Silver) → Dataflow transforms (Silver→Gold→BigQuery). Job status tracked in Firestore and reflected in the jobs page via `onSnapshot`.

The current implementation simulates upload progress client-side. The `MOCK_JOBS` dataset in `src/data/mockJobs.ts` represents what real Firestore documents will look like.

## Terraform (Infrastructure as Code)

> **All GCP infrastructure (Terraform) is centrally managed in the `platform-infra` repo, not here.** Do not look for or create `terraform/` in this repo.

The `platform-infra` repo provisions all GCP resources for the lopezcloud.dev org, including the WIF pool/provider and service accounts used by this repo's GitHub Actions workflow.

### Key files
| File | Purpose |
|---|---|
| `src/context/AuthContext.tsx` | Auth state, Google + Guest sign-in/out |
| `src/lib/firebase.ts` | Firebase app + auth + Google provider init |
| `src/data/mockJobs.ts` | Demo pipeline jobs (used by guests + dev) |
| `src/types/index.ts` | Shared types: `AuthUser`, `PipelineJob`, `JobStatus` |
| `src/pages/LoginPage.tsx` | Login UI with Google and Guest options |
| `src/pages/UploadPage.tsx` | File drop zone + pipeline preview |
| `src/pages/JobsPage.tsx` | Job table with status filter + detail modal |
| `src/components/Layout.tsx` | Sidebar nav + user panel |
