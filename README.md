# data-feeder

A React SPA that lets authenticated users upload structured data files (CSV, JSON, NDJSON, Parquet, Avro) and track them through a multi-stage GCP ingestion pipeline. Files bypass the application server entirely — the browser uploads directly to Google Cloud Storage via signed URLs, which triggers a downstream Bronze → Silver → Gold transformation pipeline.

GCP infrastructure is managed with Terraform in the [`platform-infra`](../platform-infra) repo.

---

## Tech Stack

| Layer | Choice |
|---|---|
| UI framework | React 18 + TypeScript |
| Build tool | Vite 5 |
| Styling | Tailwind CSS v3 |
| Routing | React Router v7 |
| Server state | TanStack React Query v5 |
| Auth | Google Identity Services (OAuth) |
| Container | Docker (nginx:1.27-alpine) |
| Hosting | Google Cloud Run (`australia-southeast1`) |
| Image registry | Artifact Registry |
| CI/CD | GitHub Actions + Workload Identity Federation |
| IaC | Terraform — managed in `platform-infra` repo |
| Environments | Single (prod) |

---

## Architecture

### Frontend

The app is a single-page application served as a static bundle. In production it runs inside an nginx container on Cloud Run, which handles SPA routing (all paths fall back to `index.html`) and aggressive asset caching. The container listens on port 8080, matching Cloud Run's default.

The Google OAuth Client ID is injected at Docker build time as a `VITE_*` build arg, so the compiled JS bundle contains it directly — there is no runtime config server.

```
Browser
  └── Cloud Run (nginx, port 8080)
        └── Static bundle (React SPA)
              ├── Google Identity Services (OAuth popup/One Tap)
              └── /api/* → Cloud Run API (backend, separate Cloud Run service)
```

### Upload flow

Files never transit the application server. The browser talks directly to GCS:

```
1. Browser  →  POST /api/uploads/init       (Cloud Run API, Google ID token)
                  payload: { filename, contentType, fileSize, dataset, bqTable }
                  returns: { uploadId, signedUrl, objectPath, uploadType }

2. Browser  →  PUT <signedUrl>              (direct to GCS Bronze bucket)
               ├── files ≤ 5 MB : single PUT        (v4 signed URL)
               └── files > 5 MB : chunked PUT        (GCS resumable session URI,
                                  Content-Range headers, HTTP 308 on each chunk)

3. GCS (Bronze)  →  OBJECT_FINALIZE event  →  Pub/Sub: file-uploaded
4. Cloud Function (validator)  →  schema validation, type casting  →  Silver
5. Dataflow                    →  transformation, aggregation       →  Gold → BigQuery
```

Object path convention: `raw/<dataset>/<YYYY>/<MM>/<DD>/<uuid>/<filename>`

Signed URLs expire after 15 minutes and are scoped to a single object path. The `upload-api` service account can only sign URLs for the target bucket — it has no broad GCS write access.

---

## Data Pipeline (Medallion Architecture)

| Zone | GCS Bucket | Purpose | Retention |
|---|---|---|---|
| Bronze | `<project>-raw-<env>` | Immutable raw uploads, versioning enabled | 90 days |
| Silver | `<project>-staging-<env>` | Schema-validated, type-cast Parquet files | 30 days |
| Gold | `<project>-curated-<env>` | Business-ready aggregated data | Indefinite |
| Rejected | `<project>-rejected-<env>` | Records that failed validation, annotated with error | 14 days |

All buckets use:
- **CMEK encryption** via Cloud KMS (`data-pipeline-<env>` key ring, per-layer keys, 90-day rotation)
- **Uniform bucket-level access** (no per-object ACLs)
- **Public access prevention: enforced**

Job state transitions tracked in Firestore and streamed to the frontend via `onSnapshot`:

```
UPLOADING → VALIDATING → TRANSFORMING → LOADED
                      ↘ REJECTED (bad records quarantined)
               ↘ FAILED (unrecoverable error)
```

---

## GCP Infrastructure

All resources are provisioned by Terraform in `platform-infra/projects/data-feeder/`. Region: `australia-southeast1`.

### Pub/Sub

Three pipeline topics with 7-day message retention:

| Topic | Producer | Consumer |
|---|---|---|
| `file-uploaded-<env>` | GCS Bronze `OBJECT_FINALIZE` notification | validator Cloud Function |
| `validation-complete-<env>` | validator Cloud Function | Dataflow Silver→Gold pipeline |
| `pipeline-failed-<env>` | any pipeline stage on error | alerting subscription |
| `pipeline-dlq-<env>` (dead-letter) | Pub/Sub (after 5 failed delivery attempts) | ops monitoring |

The `validator` subscription has a 300-second ack deadline (Cloud Function max timeout) with exponential backoff (10s–600s). The `dataflow` subscription has a 600-second ack deadline and **exactly-once delivery**.

### Cloud Run API

Service: `data-feeder-api-<env>`

- Runs as `sa-upload-api-<env>` service account
- CPU only billed when handling requests (`cpu_idle = true`)
- Prod: min 1 instance (no cold starts); dev/staging: min 0
- All secrets pulled from Secret Manager at runtime — no plaintext env vars in config
- Health check: `GET /health` (liveness probe, 30s interval)
- Custom domain: `datafeeder.lopezcloud.dev` via Cloud Run domain mapping

### Firestore

Database: `data-feeder-<env>` (Native mode)

- Prod: point-in-time recovery enabled (7-day window), delete protection enabled
- Two composite indexes on the `jobs` collection:
  - `dataset ASC + created_at DESC` — powers per-dataset job queries
  - `status ASC + created_at DESC` — powers the status filter on the jobs page

### BigQuery

Four datasets (`<name>_<env>`):

| Dataset | Purpose |
|---|---|
| `raw` | External tables over Bronze GCS — schema-on-read, no load required |
| `staging` | Silver layer — validated, type-cast native tables (60-day expiry) |
| `curated` | Gold layer — aggregated, business-ready analytics tables |
| `audit` | Pipeline job history + dataset version snapshots |

The `audit` dataset contains two managed tables:
- **`pipeline_jobs`** — one row per ingestion job, updated at each stage transition; partitioned by `created_at`, clustered by `dataset` + `status`
- **`dataset_versions`** — immutable snapshot metadata per Dataflow run for ML reproducibility; partitioned by `snapshot_ts`, clustered by `dataset` + `job_id`

All datasets use CMEK via Cloud KMS.

### Service Accounts & IAM

| Service Account | Role in pipeline | Key permissions |
|---|---|---|
| `sa-upload-api-<env>` | Cloud Run API | `iam.serviceAccountTokenCreator` (signs GCS URLs), `datastore.user`, `secretmanager.secretAccessor` |
| `sa-validator-<env>` | Cloud Function (Bronze→Silver) | `datastore.user`, `pubsub.subscriber`, `pubsub.publisher`, `logging.logWriter` |
| `sa-dataflow-<env>` | Dataflow workers (Silver→Gold) | `dataflow.worker`, `bigquery.dataEditor`, `bigquery.jobUser`, `datastore.user`, `pubsub.subscriber` |
| `sa-cicd-<env>` | GitHub Actions deploys | `run.admin`, `cloudfunctions.admin`, `artifactregistry.writer`, `iam.serviceAccountUser` |

### Workload Identity Federation

GitHub Actions authenticates to GCP without long-lived service account keys:

- WIF pool: `github-pool-<env>`
- OIDC provider: `https://token.actions.githubusercontent.com`
- Attribute condition scoped to `lopeztech/platform-infra` and `lopeztech/data-feeder` repositories
- `sa-cicd-<env>` granted `iam.workloadIdentityUser` via `principalSet` on the pool

### Secret Manager

Runtime config (GCP project ID, signing keys, etc.) is stored in Secret Manager and mounted into the Cloud Run API container as env vars via `secretKeyRef`. The `sa-upload-api-<env>` service account has `secretmanager.secretAccessor` on these secrets only.

---

## CI/CD

### Pull requests (`ci.yml`)

Runs on every PR targeting `main`:
1. ESLint
2. Vitest unit tests

### Deploy to Cloud Run (`deploy.yml`)

Triggers on push to `main` (when `src/`, `public/`, or root config files change) or manual `workflow_dispatch`. Always targets the `prod` GitHub Actions environment:

1. Authenticate to GCP via WIF (no static keys)
2. Build Docker image — Google OAuth Client ID injected from GitHub Actions secrets
3. Push image to Artifact Registry
4. Deploy to Cloud Run

---

## Route Structure

```
/login    LoginPage     public
/upload   UploadPage    protected — Google users can submit; guests see disabled form
/jobs     JobsPage      protected — Google users see live Firestore data; guests see mock data
*                       → /login
```

`ProtectedRoute` redirects unauthenticated users to `/login`. `Layout` wraps all protected routes with the sidebar nav.

### Auth model

Two entry points:

- **Google OAuth** — Google Identity Services `google.accounts.id.prompt()`. Grants `role: 'google'`; full access to upload and jobs. The GIS JWT (ID token) is used as the Bearer token for API calls.
- **Guest** — no auth call. Sets a synthetic `GUEST_USER` in `AuthContext` with `role: 'guest'`. Upload controls are disabled; the jobs page renders `MOCK_JOBS` from `src/data/mockJobs.ts`.

---

## Source Layout

```
src/
├── App.tsx                    Router + provider tree
├── context/
│   └── AuthContext.tsx        Auth state (GIS), Google + Guest sign-in/out, credential store
├── lib/
│   └── uploadService.ts       initUpload, simpleUploadToGCS, resumableUploadToGCS, getUploadStatus
├── pages/
│   ├── LoginPage.tsx          Login UI (Google + Guest)
│   ├── UploadPage.tsx         File drop zone, schema preview, metadata form, upload progress
│   └── JobsPage.tsx           Job table with status filter + detail modal
├── components/
│   ├── Layout.tsx             Sidebar nav + user panel
│   └── ProtectedRoute.tsx     Auth guard
├── data/
│   └── mockJobs.ts            Demo pipeline jobs (used by guests + dev)
└── types/
    ├── index.ts               AuthUser, PipelineJob, JobStatus, JobStats
    └── google-accounts.d.ts   Type declarations for Google Identity Services client
```

---

## Local Development

```bash
cp .env.example .env.local   # fill in VITE_GOOGLE_CLIENT_ID (optional — Guest mode works without it)
npm install
npm run dev                  # http://localhost:5173
```

```bash
npm run build                # TypeScript check + production build
npm run lint                 # ESLint
npm run preview              # Preview production build locally
npm test                     # Vitest (pass-with-no-tests)
```

### Environment variables

`VITE_GOOGLE_CLIENT_ID` is optional locally. Without it, Google sign-in fails but Guest mode works fully.

| Variable | Description |
|---|---|
| `VITE_GOOGLE_CLIENT_ID` | Google OAuth 2.0 Client ID (from GCP Console > APIs & Services > Credentials) |
