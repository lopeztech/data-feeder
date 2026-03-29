# data-feeder

A React SPA that lets authenticated users upload structured data files (CSV, JSON, NDJSON, Parquet, Avro) and track them through a multi-stage GCP ingestion pipeline. Files bypass the application server entirely — the browser uploads directly to Google Cloud Storage via signed URLs, which triggers a downstream Bronze → Silver → Gold transformation pipeline.

GCP infrastructure is managed with Terraform in the [`platform-infra`](../platform-infra) repo.

---

## Tech Stack

| Layer | Choice |
|---|---|
| UI framework | React 19 + TypeScript |
| Build tool | Vite 8 |
| Styling | Tailwind CSS v4 |
| Routing | React Router v7 |
| Server state | TanStack React Query v5 |
| Auth | Google Identity Services (OAuth) |
| SPA container | Docker (nginx:1.27-alpine) on Cloud Run |
| Upload API | Cloud Functions (2nd gen, Node.js 20) |
| Hosting | Google Cloud Run (`australia-southeast1`) |
| Image registry | Artifact Registry |
| CI/CD | GitHub Actions + Workload Identity Federation |
| IaC | Terraform — managed in `platform-infra` repo |

---

## Architecture

### Frontend

The app is a single-page application served as a static bundle. In production it runs inside an nginx container on Cloud Run, which handles SPA routing (all paths fall back to `index.html`) and aggressive asset caching. The container listens on port 8080, matching Cloud Run's default.

The Google OAuth Client ID and Upload API function URL are injected at Docker build time as `VITE_*` build args, so the compiled JS bundle contains them directly — there is no runtime config server.

```
Browser
  └── Cloud Run (nginx, port 8080)
        └── Static bundle (React SPA)
              ├── Google Identity Services (renderButton sign-in)
              └── Upload API → Cloud Function (upload-api)
```

### Upload API (Cloud Function)

A serverless Cloud Function (`upload-api`) handles upload orchestration:

- `POST /init` — validates request, generates GCS signed URL (simple or resumable), creates Firestore job document
- `GET /jobs` — returns the 100 most recent jobs from Firestore (ordered by `created_at` desc)
- `GET /:uploadId/status` — returns a single job document from Firestore

The function runs as `sa-upload-api` with `serviceAccountTokenCreator` (signs GCS URLs) and `datastore.user` (Firestore read/write). CORS is configured for `datafeeder.lopezcloud.dev` and `localhost:5173`.

Source: `functions/upload-api/`

### Upload flow

Files never transit the application server. The browser talks directly to GCS:

```
1. Browser  →  POST /init                  (Cloud Function, Google ID token)
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

Object path convention: `<dataset>/<uuid>/<filename>`

Signed URLs expire after 15 minutes and are scoped to a single object path.

---

## Data Pipeline (Medallion Architecture)

| Zone | GCS Bucket | Purpose | Retention |
|---|---|---|---|
| Bronze | `<project>-raw` | Immutable raw uploads, versioning enabled | 90 days |
| Silver | `<project>-staging` | Schema-validated, type-cast Parquet files | 30 days |
| Gold | `<project>-curated` | Business-ready aggregated data | Indefinite |
| Rejected | `<project>-rejected` | Records that failed validation, annotated with error | 14 days |

All buckets use:
- **CMEK encryption** via Cloud KMS (`data-pipeline` key ring, per-layer keys, 90-day rotation)
- **Uniform bucket-level access** (no per-object ACLs)
- **Public access prevention: enforced**

Job state transitions tracked in Firestore:

```
UPLOADING → VALIDATING → TRANSFORMING → LOADED
                      ↘ REJECTED (bad records quarantined)
               ↘ FAILED (unrecoverable error)
```

---

## GCP Infrastructure

All resources are provisioned by Terraform in `platform-infra/projects/data-feeder/`. Region: `australia-southeast1`. Project: `data-feeder-lcd`.

### Pub/Sub

Three pipeline topics with 7-day message retention:

| Topic | Producer | Consumer |
|---|---|---|
| `file-uploaded` | GCS Bronze `OBJECT_FINALIZE` notification | validator Cloud Function |
| `validation-complete` | validator Cloud Function | Dataflow Silver→Gold pipeline |
| `pipeline-failed` | any pipeline stage on error | alerting subscription |
| `pipeline-dlq` (dead-letter) | Pub/Sub (after 5 failed delivery attempts) | ops monitoring |

### Cloud Run (SPA)

Service: `data-feeder-api`

- Serves static nginx container with the React SPA
- Custom domain: `datafeeder.lopezcloud.dev` via Global HTTPS Load Balancer
- Health check: `GET /health` (nginx returns 200)
- Min 1 instance (no cold starts), max 10

### Cloud Function (Upload API)

Function: `upload-api` (2nd gen, HTTP trigger)

- Runs as `sa-upload-api` service account
- Env vars: `GCS_RAW_BUCKET`, `FIRESTORE_DATABASE`
- 256Mi memory, max 10 instances, 120s timeout
- Unauthenticated access allowed (auth handled at application level via Bearer token)

### Firestore

Database: `data-feeder` (Native mode)

- `jobs` collection: one document per upload, created by the upload API function
- Prod: point-in-time recovery enabled (7-day window), delete protection enabled

### BigQuery

Four datasets:

| Dataset | Purpose |
|---|---|
| `raw` | External tables over Bronze GCS — schema-on-read, no load required |
| `staging` | Silver layer — validated, type-cast native tables (60-day expiry) |
| `curated` | Gold layer — aggregated, business-ready analytics tables |
| `audit` | Pipeline job history + dataset version snapshots |

### Service Accounts & IAM

| Service Account | Role in pipeline | Key permissions |
|---|---|---|
| `sa-upload-api` | Cloud Function (upload API) | `iam.serviceAccountTokenCreator` (signs GCS URLs), `datastore.user`, `secretmanager.secretAccessor` |
| `sa-validator` | Cloud Function (Bronze→Silver) | `datastore.user`, `pubsub.subscriber`, `pubsub.publisher`, `logging.logWriter` |
| `sa-dataflow` | Dataflow workers (Silver→Gold) | `dataflow.worker`, `bigquery.dataEditor`, `bigquery.jobUser`, `datastore.user`, `pubsub.subscriber` |
| `sa-cicd` | GitHub Actions deploys | `run.admin`, `cloudfunctions.admin`, `artifactregistry.writer`, `iam.serviceAccountUser` |

### Workload Identity Federation

GitHub Actions authenticates to GCP without long-lived service account keys:

- WIF pool: `github-pool`
- OIDC provider: `https://token.actions.githubusercontent.com`
- Attribute condition scoped to `lopeztech/platform-infra` and `lopeztech/data-feeder` repositories
- `sa-cicd` granted `iam.workloadIdentityUser` via `principalSet` on the pool

### Load Balancer & DNS

- Global HTTPS Load Balancer with Google-managed SSL certificate for `datafeeder.lopezcloud.dev`
- Serverless NEG routing to Cloud Run
- Cloudflare DNS A record (DNS-only, not proxied)
- HTTP → HTTPS redirect (301)

---

## CI/CD

### Pull requests (`ci.yml`)

Runs on every PR targeting `main`:
1. ESLint
2. Vitest unit tests

### Deploy SPA to Cloud Run (`deploy.yml`)

Triggers on push to `main` (when `src/`, `public/`, or root config files change) or manual `workflow_dispatch`:

1. Authenticate to GCP via WIF
2. Resolve Upload API function URL
3. Build Docker image — `VITE_GOOGLE_CLIENT_ID` and `VITE_UPLOAD_API_URL` injected as build args
4. Push image to Artifact Registry
5. Deploy to Cloud Run

### Deploy Upload API function (`deploy-function.yml`)

Triggers on push to `main` (when `functions/upload-api/**` changes) or manual `workflow_dispatch`:

1. Authenticate to GCP via WIF
2. Install dependencies and build TypeScript
3. Deploy to Cloud Functions (2nd gen) with `sa-upload-api` service account

---

## Route Structure

```
/login    LoginPage     public
/upload   UploadPage    protected — Google users can submit; guests see disabled form
/jobs     JobsPage      protected — Google users see live Firestore data; guests see mock data
*                       → /login
```

`ProtectedRoute` redirects unauthenticated users to `/login`. `Layout` wraps all protected routes with a responsive sidebar (collapsible drawer on mobile).

### Auth model

Two entry points:

- **Google OAuth** — Google Identity Services `renderButton`. Grants `role: 'google'`; full access to upload and jobs. The GIS JWT (ID token) is used as the Bearer token for API calls. Auth state persisted to `sessionStorage`.
- **Guest** — no auth call. Sets a synthetic `GUEST_USER` in `AuthContext` with `role: 'guest'`. Upload controls are disabled; the jobs page renders `MOCK_JOBS` from `src/data/mockJobs.ts`.

### Features

- **Drag-and-drop upload** with format validation (CSV, JSON, NDJSON, Parquet, Avro)
- **Client-side schema inference** on CSV/JSON/NDJSON — detects column names and types, displays preview table
- **Auto-generated BigQuery JSON schema** — downloadable from the data preview
- **Auto-populated metadata** — dataset name and BQ table derived from filename
- **Upload progress bar** with resumable chunked upload for files > 5MB
- **Pipeline preview** showing Bronze → Silver → Gold → BigQuery destination
- **Mobile-responsive UI** — collapsible sidebar drawer, card layout for jobs on small screens

---

## Source Layout

```
src/
├── App.tsx                    Router + provider tree
├── context/
│   └── AuthContext.tsx        Auth state (GIS), Google + Guest sign-in/out, sessionStorage persistence
├── lib/
│   └── uploadService.ts       initUpload, listJobs, simpleUploadToGCS, resumableUploadToGCS
├── pages/
│   ├── LoginPage.tsx          Login UI (Google renderButton + Guest)
│   ├── UploadPage.tsx         File drop zone, schema preview, BQ schema export, metadata form, upload progress
│   └── JobsPage.tsx           Job list with status filter + detail modal (live Firestore data or mock)
├── components/
│   ├── Layout.tsx             Responsive sidebar nav + user panel
│   └── ProtectedRoute.tsx     Auth guard
├── data/
│   └── mockJobs.ts            Demo pipeline jobs (used by guests + dev)
└── types/
    ├── index.ts               AuthUser, PipelineJob, JobStatus, JobStats
    └── google-accounts.d.ts   Type declarations for Google Identity Services client

functions/
└── upload-api/                Cloud Function source (Node.js 20, TypeScript)
    ├── src/index.ts           HTTP handler: /init, /jobs, /:id/status
    ├── package.json
    └── tsconfig.json
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

| Variable | Description |
|---|---|
| `VITE_GOOGLE_CLIENT_ID` | Google OAuth 2.0 Client ID (optional locally — Guest mode works without it) |
| `VITE_UPLOAD_API_URL` | Upload API Cloud Function URL (set automatically by CI; optional locally) |
