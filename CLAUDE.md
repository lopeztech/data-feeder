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

Copy `.env.example` to `.env.local` and fill in `VITE_GOOGLE_CLIENT_ID`. Without it the app still runs — Google sign-in will fail but Guest mode works fully. `VITE_UPLOAD_API_URL` is optional locally (defaults to `/api/uploads`); in production it's set to the Cloud Function URL at Docker build time.

## Architecture

React 19 + Vite 8 + TypeScript SPA. Tailwind CSS v4 for styling. React Router v7 for routing. Google Identity Services (GIS) for auth. React Query for server state.

### Auth model
Two entry points on the login page:
- **Google OAuth** — Google Identity Services `renderButton`. On success, user gets `role: 'google'` and full access (upload + jobs). The GIS JWT (ID token) is used as the Bearer token for API calls. Auth state persisted to `sessionStorage`.
- **Guest** — no auth call; sets a synthetic `GUEST_USER` in `AuthContext` with `role: 'guest'`. All upload controls are disabled; jobs page shows `MOCK_JOBS` from `src/data/mockJobs.ts`.

`AuthContext` (`src/context/AuthContext.tsx`) exposes `user`, `gsiReady`, `signInAsGuest`, `signOutUser`, `renderGoogleButton`. Check `user.role === 'guest'` to gate features. `getGoogleCredential()` is exported for retrieving the current ID token.

### Route structure
```
/login          → LoginPage (public)
/upload         → UploadPage (protected, Google only can submit)
/jobs           → JobsPage  (protected, Google users see live Firestore data, guests see mock data)
```

`ProtectedRoute` redirects unauthenticated users to `/login`. `Layout` wraps all protected routes with a responsive sidebar (collapsible drawer on mobile).

### Upload API (Cloud Function)

The upload API is a Cloud Function (`functions/upload-api/`) — not bundled in the SPA container. It handles:
- `POST /init` — generates GCS signed URL (simple or resumable), creates Firestore job document
- `GET /jobs` — returns 100 most recent jobs from Firestore
- `GET /:uploadId/status` — returns a single job document

Runs as `sa-upload-api` service account with `serviceAccountTokenCreator` and `datastore.user`. Firestore database: `data-feeder`.

### Validator (Cloud Function)

The validator (`functions/validator/`) is a Pub/Sub-triggered Cloud Function that processes uploaded files:
- Triggered by `file-uploaded` topic (GCS OBJECT_FINALIZE on Bronze bucket)
- Validates CSV (csv-parse), JSON, NDJSON (schema/record check), Parquet/Avro (magic bytes)
- **Pass**: copies to Silver bucket, updates Firestore → `TRANSFORMING`, publishes `validation-complete`
- **Fail**: copies to Rejected bucket, updates Firestore → `REJECTED`/`FAILED`, publishes `pipeline-failed`
- Idempotency guard: only processes jobs with status `UPLOADING`

Runs as `sa-validator` service account with `datastore.user`, `pubsub.subscriber`, `pubsub.publisher`. Env vars: `GCS_RAW_BUCKET`, `GCS_SILVER_BUCKET`, `GCS_REJECTED_BUCKET`, `FIRESTORE_DATABASE`, `VALIDATION_COMPLETE_TOPIC`, `PIPELINE_FAILED_TOPIC`.

### Loader (Cloud Function)

The loader (`functions/loader/`) is a Pub/Sub-triggered Cloud Function that loads validated data into BigQuery:
- Triggered by `validation-complete` topic (published by validator)
- Parses CSV/JSON/NDJSON from Silver bucket
- Creates BigQuery table in `curated` dataset if it doesn't exist (schema inferred from data, day-partitioned)
- Streams rows into BigQuery in batches of 500
- Updates Firestore → `LOADED` with row counts
- On failure: updates Firestore → `FAILED`, publishes `pipeline-failed`
- Idempotency guard: only processes jobs with status `TRANSFORMING`

Runs as `sa-dataflow` service account with `bigquery.dataEditor`, `bigquery.jobUser`, `datastore.user`, `pubsub.subscriber`, `pubsub.publisher`.

### Data flow
Upload page → calls `POST /init` on Cloud Function → receives GCS signed URL → browser uploads directly to GCS Bronze bucket → GCS notifies Pub/Sub → Validator Cloud Function validates (Bronze→Silver/Rejected) → publishes `validation-complete` → Loader Cloud Function loads to BigQuery (Silver→Gold) → updates Firestore to LOADED. Job status tracked in Firestore and fetched by the jobs page via the `/jobs` endpoint.

## Terraform (Infrastructure as Code)

> **All GCP infrastructure (Terraform) is centrally managed in the `platform-infra` repo, not here.** Do not look for or create `terraform/` in this repo.

The `platform-infra` repo provisions all GCP resources for the lopezcloud.dev org, including the WIF pool/provider and service accounts used by this repo's GitHub Actions workflows.

## CI/CD

Four deploy workflows:
- `deploy.yml` — builds Docker image (nginx + SPA), pushes to Artifact Registry, deploys to Cloud Run. Resolves the Cloud Function URL and bakes it into the build. Includes post-deploy smoke test.
- `deploy-function.yml` — builds and deploys the upload-api Cloud Function (HTTP trigger).
- `deploy-validator.yml` — builds and deploys the validator Cloud Function (Pub/Sub trigger on `file-uploaded` topic).
- `deploy-loader.yml` — builds and deploys the loader Cloud Function (Pub/Sub trigger on `validation-complete` topic).

Both use Workload Identity Federation (no static keys) and deploy to `australia-southeast1` in project `data-feeder-lcd`.

### Key files
| File | Purpose |
|---|---|
| `src/context/AuthContext.tsx` | Auth state (GIS), Google + Guest sign-in/out, sessionStorage persistence |
| `src/lib/uploadService.ts` | initUpload, listJobs, simpleUploadToGCS, resumableUploadToGCS |
| `src/types/google-accounts.d.ts` | Type declarations for the Google Identity Services client |
| `src/data/mockJobs.ts` | Demo pipeline jobs (used by guests + dev) |
| `src/types/index.ts` | Shared types: `AuthUser`, `PipelineJob`, `JobStatus` |
| `src/pages/LoginPage.tsx` | Login UI with Google renderButton and Guest options |
| `src/pages/UploadPage.tsx` | File drop zone, schema preview, BQ schema export, metadata form, upload progress |
| `src/pages/JobsPage.tsx` | Job list (live or mock) with status filter + detail modal |
| `src/components/Layout.tsx` | Responsive sidebar nav + user panel |
| `functions/upload-api/src/index.ts` | Cloud Function: /init, /jobs, /:id/status |
| `functions/validator/src/index.ts` | Cloud Function: Pub/Sub handler for file validation |
| `functions/validator/src/validators.ts` | Pure validation logic per file format |
| `functions/loader/src/index.ts` | Cloud Function: Silver→Gold BigQuery loader |
| `functions/loader/src/parsers.ts` | File parsing for BQ row conversion |
