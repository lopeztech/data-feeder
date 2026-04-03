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
- Text formats: PII masking → type inference → null standardization → type casting → deduplication → Parquet/Snappy conversion
- Per-record validation: valid rows → Silver (Parquet), rejected rows → Rejected bucket (NDJSON manifest with errors)
- Binary formats (Parquet/Avro): pass through unchanged after magic-byte validation
- **Pass**: writes Parquet to Silver bucket, updates Firestore → `TRANSFORMING`, publishes `validation-complete`
- **Fail**: copies to Rejected bucket, updates Firestore → `REJECTED`/`FAILED`, publishes `pipeline-failed`
- Idempotency guard: only processes jobs with status `UPLOADING`

Runs as `sa-validator` service account with `datastore.user`, `pubsub.subscriber`, `pubsub.publisher`. Env vars: `GCS_RAW_BUCKET`, `GCS_SILVER_BUCKET`, `GCS_REJECTED_BUCKET`, `FIRESTORE_DATABASE`, `VALIDATION_COMPLETE_TOPIC`, `PIPELINE_FAILED_TOPIC`.

### Loader (Dataflow Flex Template + Launcher)

The loader stage uses an Apache Beam Dataflow Flex Template (`pipelines/dataflow/`) launched by a thin Cloud Function (`functions/loader/`):

**Launcher** (`functions/loader/`): Pub/Sub-triggered Cloud Function that receives `validation-complete` messages and launches the Dataflow Flex Template with parameters (`silver_gcs_path`, `target_bq_table`, `job_id`). Idempotency guard: only launches for jobs with status `TRANSFORMING`.

**Dataflow Pipeline** (`pipelines/dataflow/`): Apache Beam Python pipeline containerized as a Docker image in Artifact Registry:
- Reads validated Parquet from GCS Silver path
- Two-stage load: Silver Parquet → `staging` dataset (WRITE_TRUNCATE) → `curated` dataset (WRITE_APPEND)
- Auto-detects high-cardinality columns from Parquet schema for BigQuery clustering (up to 4 fields)
- BigQuery tables partitioned by `_PARTITIONDATE` with dataset-specific clustering
- Cloud Monitoring metrics: `rows_processed`, `bytes_written`, `error_count`
- Worker autoscaling: minimum 1, maximum 20 workers
- Updates Firestore → `LOADED` with row counts
- On failure: updates Firestore → `FAILED`, publishes `pipeline-failed`

Template parameters: `silver_gcs_path`, `target_bq_table`, `job_id`, `project_id`, plus optional `firestore_database`, `bq_staging_dataset`, `bq_curated_dataset`, `dataset_location`, `pipeline_failed_topic`.

Runs as `sa-dataflow` service account with `bigquery.dataEditor`, `bigquery.jobUser`, `datastore.user`, `pubsub.subscriber`, `pubsub.publisher`, `dataflow.worker`.

### ML Pipeline (Vertex AI)

K-Means clustering for player role identification (`pipelines/ml/`):
- **Feature view**: `curated.player_features_v` — JOIN profiles + stats, derived features (goals/appearance, shot accuracy, goals vs expected)
- **Pipeline** (KFP): preprocess → train (sweep k=3..10, best silhouette) → evaluate (cluster analysis, write to `curated.player_clusters`) → deploy to Vertex AI endpoint
- **Model**: scikit-learn KMeans, StandardScaler normalized features
- **Output**: `player_clusters` table with cluster_id + impact_score per player
- Runs as `sa-ml` service account via Vertex AI Pipelines

### Data flow
Upload page → calls `POST /init` on Cloud Function → receives GCS signed URL → browser uploads directly to GCS Bronze bucket (date-partitioned paths: `dataset/year=YYYY/month=MM/day=DD/uuid/file`) → GCS notifies Pub/Sub → Validator Cloud Function validates, masks PII, type-casts, deduplicates, converts to Parquet/Snappy (Bronze→Silver/Rejected) → publishes `validation-complete` → Loader launcher Cloud Function triggers Dataflow Flex Template → Beam pipeline loads to staging (WRITE_TRUNCATE) then appends to curated with clustering (Silver→Gold, autoscaling 1–20 workers) → updates Firestore to LOADED. ML Pipeline reads from Gold, trains K-Means, writes cluster assignments back to BigQuery.

## Terraform (Infrastructure as Code)

> **All GCP infrastructure (Terraform) is centrally managed in the `platform-infra` repo, not here.** Do not look for or create `terraform/` in this repo.

The `platform-infra` repo provisions all GCP resources for the lopezcloud.dev org, including the WIF pool/provider and service accounts used by this repo's GitHub Actions workflows.

## CI/CD

Five deploy workflows:
- `deploy.yml` — builds Docker image (nginx + SPA), pushes to Artifact Registry, deploys to Cloud Run. Resolves the Cloud Function URL and bakes it into the build. Includes post-deploy smoke test.
- `deploy-function.yml` — builds and deploys the upload-api Cloud Function (HTTP trigger).
- `deploy-validator.yml` — builds and deploys the validator Cloud Function (Pub/Sub trigger on `file-uploaded` topic).
- `deploy-loader.yml` — builds the Dataflow Flex Template Docker image, creates the template spec, and deploys the launcher Cloud Function (Pub/Sub trigger on `validation-complete` topic).
- `deploy-ml-pipeline.yml` — creates BQ feature view, compiles KFP pipeline, submits to Vertex AI.

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
| `functions/validator/src/index.ts` | Cloud Function: Pub/Sub handler for file validation + transformation |
| `functions/validator/src/validators.ts` | Pure validation logic per file format |
| `functions/validator/src/schema.ts` | Type inference, casting, null standardization, deduplication |
| `functions/validator/src/parquet.ts` | Parquet/Snappy writer for Silver output |
| `functions/loader/src/index.ts` | Cloud Function: thin launcher that triggers Dataflow Flex Template |
| `pipelines/dataflow/pipeline.py` | Apache Beam pipeline: Silver Parquet → staging → curated BigQuery |
| `pipelines/dataflow/Dockerfile` | Flex Template container image |
| `pipelines/dataflow/metadata.json` | Flex Template parameter metadata |
| `pipelines/ml/pipeline.py` | KFP pipeline definition for K-Means clustering |
| `pipelines/ml/components/` | Pipeline components: preprocess, train, evaluate, deploy |
| `pipelines/ml/sql/player_features_view.sql` | BigQuery view joining profiles + stats |
