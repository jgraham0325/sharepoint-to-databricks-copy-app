# SharePoint to Databricks Transfer

A Databricks App that lets users browse a SharePoint site, select files, and copy them into a Databricks Unity Catalog Volume. FastAPI backend + React frontend.

## Prerequisites

- Python 3.9+
- Node.js 18+
- A Databricks workspace (Azure) with Unity Catalog enabled
- A Microsoft Entra ID (Azure AD) app registration with the following API permissions:
  - `User.Read` - Read signed-in user profile (used to show “Logged in as …” and to persist session)
  - `Sites.Read.All` - Read SharePoint sites
  - `Files.Read.All` - Read files from SharePoint
  - `Group.Read.All` - Read group information (needed for Team sites)
  - `Team.ReadBasic.All` - Read basic team information (needed to discover Team sites)

## Microsoft Entra ID App Setup

1. Go to [Azure Portal > App registrations](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade) and create a new registration.
2. Under **Authentication > Web**, add a redirect URI:
   - For local dev: `http://localhost:8000/api/v1/auth/callback`
   - For Databricks: `https://<your-app-url>/api/v1/auth/callback`
3. Under **Certificates & secrets**, create a new client secret. Copy the value.
4. Under **API permissions**, add `Microsoft Graph > Delegated permissions`:
   - `User.Read`
   - `Sites.Read.All`
   - `Files.Read.All`
   - `Group.Read.All`
   - `Team.ReadBasic.All`
   
   **Important:** After adding these permissions, click **"Grant admin consent"** to approve them for your organization. Without admin consent, users won't be able to see Team sites.
5. Note your **Application (client) ID** and **Directory (tenant) ID** from the Overview page.

## Running Locally

### 1. Install backend dependencies

```bash
cd back-end
python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Edit `back-end/.env` with your values:

```
MS_CLIENT_ID=<your Entra app client ID>
MS_CLIENT_SECRET=<your Entra app client secret>
MS_TENANT_ID=<your Azure tenant ID>

DATABRICKS_HOST=https://<your-workspace>.azuredatabricks.net
DATABRICKS_TOKEN=<your personal access token>

APP_URL=http://localhost:8000
```

Generate a Databricks personal access token from your workspace under **Settings > Developer > Access tokens**.

### 3. Install frontend dependencies and build

```bash
cd front-end
npm install
npm run build
```

This outputs the compiled React app to `back-end/static/`.

### 4. Start the server

```bash
cd back-end
uvicorn app:app --reload
```

**Seeing server logs:** Logs from the FastAPI app (and `common.logger`) go to stdout. If you run the backend locally, they appear in the same terminal as uvicorn. If logs don’t show up, run with unbuffered output: `PYTHONUNBUFFERED=1 uvicorn app:app --reload`. If you run the app as a **Databricks App** (deployed via the bundle), the server runs in the cloud—view logs in the Databricks UI under the app’s run (e.g. **Apps** → your app → **Logs** or **Runs**).

Open [http://localhost:8000](http://localhost:8000) in your browser.

### Frontend development mode (optional)

For hot-reloading while working on the UI, run the Vite dev server alongside the backend:

```bash
# Terminal 1 — backend
cd back-end
uvicorn app:app --reload

# Terminal 2 — frontend dev server
cd front-end
npm run dev
```

Open [http://localhost:5173](http://localhost:5173). **Use this URL (not 8000) so frontend changes reload automatically.** The Vite dev server proxies `/api` requests to the backend on port 8000.

**Auth redirect:** Keep your Microsoft redirect URI as `http://localhost:8000/api/v1/auth/callback`. The app sends that URL to Microsoft, so the OAuth popup lands on the backend. After sign-in, the callback page posts the token to the opener (your tab on 5173), so you stay on the Vite app. No second redirect URI is required.

## Deploying to Databricks

### 1. Store Microsoft credentials as Databricks secrets

Create a secret scope and add your three app credentials. The **same scope** is used by the transfer job to read these and to read per-user tokens (written by the app when you start a transfer):

```bash
databricks secrets create-scope sharepoint-app-scope

databricks secrets put-secret sharepoint-app-scope ms-client-id --string-value "<your client ID>"
databricks secrets put-secret sharepoint-app-scope ms-client-secret --string-value "<your client secret>"
databricks secrets put-secret sharepoint-app-scope ms-tenant-id --string-value "<your tenant ID>"
```

- **App credentials** (`ms-client-id`, `ms-client-secret`, `ms-tenant-id`): you create these once. They match what the app and the transfer job expect.
- **Per-user tokens** (keys `tokens_<user_oid>`): written by the app when you start a transfer; the job reads them. You do not create these manually.
- **Permissions**: The app identity needs **write** (e.g. `put_secret`) on the scope so it can store tokens. The **sharepoint-transfer** job identity needs **read** (e.g. `get_secret`) on the scope so it can read tokens and app credentials.
- To use a different scope name, set the `SHAREPOINT_SECRET_SCOPE` environment variable (default is `sharepoint-app-scope`).

### 2. Build the frontend

```bash
cd front-end
npm install
npm run build
```

The build output in `back-end/static/` is deployed alongside the backend.

### 3. Update the redirect URI

Once you know your Databricks App URL (e.g. `https://sharepoint-to-databricks-transfer-1234567890.azuredatabricks.net`), add it as a redirect URI in your Entra app registration:

```
https://<your-app-url>/api/v1/auth/callback
```

Also update `APP_URL` — in `app.yaml` add an env entry, or the app can auto-detect from the request in production. For simplicity you can add:

```yaml
env:
  - name: APP_URL
    value: "https://<your-app-url>"
```

### 4. Deploy with Databricks Asset Bundles

Set your workspace URL (required by the bundle), then deploy:

```bash
export DATABRICKS_HOST=https://<your-workspace>.azuredatabricks.net
databricks bundle deploy -t dev
```

Replace `<your-workspace>` with your workspace hostname (e.g. `adb-1234567890123456.7`). You must be authenticated (e.g. `databricks auth login` or a token in `~/.databrickscfg`).

After deployment, open your Databricks workspace, navigate to **Compute > Apps**, and find **SharePoint to Databricks Transfer** to see its URL and status.

The bundle also deploys a job named **sharepoint-transfer** (see `databricks.yml`). It runs on **serverless compute** and is used to offload large file transfers so the app server does not hold 10GB+ in memory. See [Large file handling](#large-file-handling) below.

## Large file handling

To avoid running out of memory on the app server when copying very large files (e.g. 10GB):

- **Files under the threshold** (default 100 MiB): The server streams each file from SharePoint to a temp file in 8 MiB chunks, then uploads to the volume. Memory use is bounded; temp disk is used on the server.
- **Files at or above the threshold** (or size unknown): The server does not download the file. Instead it starts a **Databricks job run** per file. The job runs the script in `notebooks/sharepoint_transfer.py` on a cluster: it streams the file from SharePoint to the cluster’s local disk, then uploads to the volume. The app server only fetches download URLs and submits runs; it polls run status until all are done.

Configure:

- **`LARGE_FILE_THRESHOLD_BYTES`** (default `104857600` = 100 MiB): Files with size ≥ this are offloaded to the job.
- **`SHAREPOINT_TRANSFER_JOB_ID`** (optional): Job ID of the `sharepoint-transfer` job. If unset, the app looks up the job by name after deployment.

Ensure the `sharepoint-transfer` job is deployed (it is defined in `databricks.yml`) and that the job’s cluster has network access to Microsoft Graph and to your workspace (for volume uploads). **If selecting large files does not trigger the job** and you see an error like "Databricks job 'sharepoint-transfer' not found", run `databricks bundle deploy -t dev` so the job is created, or set `SHAREPOINT_TRANSFER_JOB_ID` in the app environment to the job ID from **Workflows > Jobs** in the workspace.

### Scaling to many files (e.g. 10K+)

- **Small batches** (fewer than `MAX_FILES_ON_SERVER` files, default 20, and all under the size threshold): Transfers run on the app server to avoid job startup delay.
- **Larger batches**: The app writes manifest file(s) to the destination volume and submits one Databricks job run per manifest chunk (`FILES_PER_MANIFEST_CHUNK`, default 50). The job reads the manifest and transfers each file.
- **SharePoint listing** follows `@odata.nextLink` so folders with more than 200 direct children are fully enumerated.
- **Download URL resolution** uses bounded concurrency to avoid throttling.
- **Transfer results** are capped in memory (`MAX_TRANSFER_RESULTS_IN_MEMORY`, default 500); the UI shows a summary and the capped list.
- The job has `max_concurrent_runs: 20` in `databricks.yml`.

## Project Structure

```
sharepoint-upload-app/
├── databricks.yml              # DAB deployment config (app + sharepoint-transfer job)
├── notebooks/
│   └── sharepoint_transfer.py  # Job script: stream SharePoint → volume (large files)
├── back-end/
│   ├── app.py                  # FastAPI entry point
│   ├── app.yaml                # Databricks App runtime config
│   ├── requirements.txt
│   ├── .env.example            # Local dev env template
│   ├── common/
│   │   ├── config.py           # Env var loading
│   │   ├── logger.py           # Logging utility
│   │   ├── authentication/
│   │   │   └── workspace.py    # Databricks WorkspaceClient
│   │   └── connectors/
│   │       ├── workspace.py    # Volume upload operations
│   │       └── microsoft_graph.py  # Graph API (auth + browse + download)
│   ├── models/                 # Pydantic request/response models
│   ├── services/               # Business logic
│   ├── routes/v1/              # API endpoints
│   └── static/                 # React build output (generated)
└── front-end/
    ├── package.json
    ├── vite.config.ts
    └── src/                    # React + TypeScript source
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/healthcheck` | Health check |
| `GET` | `/api/v1/auth/login` | Get Microsoft login URL |
| `GET` | `/api/v1/auth/callback` | OAuth callback (browser redirect) |
| `POST` | `/api/v1/auth/refresh` | Refresh an MS access token |
| `GET` | `/api/v1/sharepoint/sites?query=` | Search SharePoint sites |
| `GET` | `/api/v1/sharepoint/sites/{id}/drives` | List document libraries |
| `GET` | `/api/v1/sharepoint/drives/{id}/children` | List folder contents |
| `POST` | `/api/v1/transfer/start` | Start file transfer to Volume |
| `POST` | `/api/v1/transfer/copy-folder` | **Agent-friendly:** Copy entire SharePoint folder (recursively) to a volume |
| `GET` | `/api/v1/transfer/status/{id}` | Poll transfer progress |
| `GET` | `/api/v1/volumes/catalogs` | List Unity Catalog catalogs |
| `GET` | `/api/v1/volumes/catalogs/{c}/schemas` | List schemas |
| `GET` | `/api/v1/volumes/catalogs/{c}/schemas/{s}/volumes` | List volumes |
| `POST` | `/api/v1/agent/chat` | Chat with the agent (SharePoint → volume copy) |

## In-app Agent (chat UI)

The app includes an **Agent** page that uses a Databricks Foundation Model API (LLM) so you can copy SharePoint content to a volume by talking in plain language.

1. **Open the Agent:** In the UI, click **Agent** (or go to `#/agent`).
2. **Sign in** with Microsoft so the agent can access SharePoint on your behalf.
3. **Chat:** Ask in natural language, for example:
   - *"List my SharePoint sites"*
   - *"Copy the Reports folder from the Marketing site to catalog main, schema my_schema, volume sharepoint_imports"*
   - *"What volumes do I have in catalog main?"*

The agent uses **tool calling** against your connected Databricks workspace (list sites, list drives, list folder contents, copy folder to volume, list catalogs/schemas/volumes) and returns a short summary.

**Backend configuration:** The agent calls the Databricks Foundation Model API using `DATABRICKS_HOST` and `DATABRICKS_TOKEN`. Optionally set `DATABRICKS_CHAT_MODEL` (default: `databricks-meta-llama-3-3-70b-instruct`) to use a different model. The model must support function/tool calling (see [Databricks docs](https://docs.databricks.com/en/machine-learning/model-serving/function-calling.html)).

## AI agent–driven automation

You can drive the copy entirely from an AI agent or script by choosing a SharePoint site and folder, then calling one endpoint to copy that folder (and all nested files) into a Unity Catalog volume.

### 1. Get a Microsoft token

The agent must have a valid Microsoft access token (e.g. from your app’s OAuth flow or a service account). Send it on every request as the `X-MS-Token` header.

### 2. Resolve site → drive → folder (optional)

- **Sites:** `GET /api/v1/sharepoint/sites?query=...`  
  Returns sites; use `id` and pick the site you want.
- **Drives (document libraries):** `GET /api/v1/sharepoint/sites/{site_id}/drives`  
  Returns drives; use `id` as `drive_id`.
- **Folder (optional):** `GET /api/v1/sharepoint/drives/{drive_id}/children` for root, or `?item_id={folder_id}` for a subfolder.  
  Use the folder’s `id` as `folder_item_id`. Omit `folder_item_id` to copy from the drive root.

### 3. Resolve destination volume

- **Catalogs:** `GET /api/v1/volumes/catalogs`
- **Schemas:** `GET /api/v1/volumes/catalogs/{catalog}/schemas`
- **Volumes:** `GET /api/v1/volumes/catalogs/{catalog}/schemas/{schema}/volumes`

Use the chosen `catalog`, `schema_name`, and `volume` in the copy request.

### 4. Copy folder to volume

**Request:** `POST /api/v1/transfer/copy-folder`

**Headers:** `X-MS-Token: <microsoft-access-token>`

**Body (JSON):**

```json
{
  "drive_id": "<drive-id-from-step-2>",
  "folder_item_id": "<folder-id-or-null-for-root>",
  "catalog": "main",
  "schema_name": "my_schema",
  "volume": "sharepoint_volume",
  "subfolder": "imports/2025"
}
```

- `folder_item_id`: omit or `null` to copy the entire document library from the root.
- `subfolder`: optional path inside the volume (e.g. `imports/2025`). Folder structure under the source folder is preserved.

**Response:** Same as `POST /api/v1/transfer/start` — returns `transfer_id`, `status`, `total`, `completed`, `failed`, `results`. Poll with `GET /api/v1/transfer/status/{transfer_id}` until `status` is `completed` or `failed`.

**Example (curl):**

```bash
curl -X POST http://localhost:8000/api/v1/transfer/copy-folder \
  -H "Content-Type: application/json" \
  -H "X-MS-Token: YOUR_MS_ACCESS_TOKEN" \
  -d '{"drive_id":"b!xxx","folder_item_id":"01ABC...","catalog":"main","schema_name":"my_schema","volume":"sharepoint_volume","subfolder":"migration"}'
```

An AI agent can chain: discover site → pick drive (and optionally folder) → pick volume → call `copy-folder` → poll `transfer/status` until done.

## Scripts

### Find your site and drive IDs

Run the helper script to list all SharePoint sites you can access and their document libraries (drives) with IDs:

```bash
python scripts/list_sharepoint_sites_and_drives.py
```

You’ll sign in once via device code (browser). The script prints each **Site ID** and **Drive ID**; use the Drive ID with the sample-files script or the app API.

### Create sample files in SharePoint

The `scripts/create_sharepoint_sample_files.py` script creates many sample files in a SharePoint document library (e.g. 5,000 files of ~10 MB each) using Microsoft Graph upload sessions. Useful for load testing or populating a site before running transfers.

**Requirements:** Same Microsoft app credentials as the app (`back-end/.env`). The app registration must have **Sites.FullControl.All** or **Files.ReadWrite.All** (delegated) and admin consent.

**Usage:**

1. Install backend dependencies (so `msal`, `httpx`, `python-dotenv` are available).
2. From the repo root, use either a **document library URL** (easiest) or a **drive ID**:

   **Option A — URL (recommended):** Paste the SharePoint document library URL from your browser (you can include a subfolder path):

   ```bash
   python scripts/create_sharepoint_sample_files.py --url "https://<tenant>.sharepoint.com/sites/<SiteName>/Shared%20Documents"
   # or with a subfolder:
   python scripts/create_sharepoint_sample_files.py --url "https://<tenant>.sharepoint.com/sites/<SiteName>/Shared%20Documents/MyFolder"
   ```

   **Option B — Drive ID:** Run `python scripts/list_sharepoint_sites_and_drives.py` to find the drive ID, then:

   ```bash
   python scripts/create_sharepoint_sample_files.py --drive-id "<drive-id>" [--folder-id "<folder-item-id>"] [--count 5000] [--size-mb 10]
   ```

3. On first run you’ll be prompted to sign in via device code (browser). With `--url`, the script resolves the site and document library from the URL; no need to look up IDs.

**URL format:** Must start with `https://` and the path must be `/sites/<SiteName>/<LibraryName>` or `/teams/<TeamName>/<LibraryName>`. Add `/Folder/SubFolder` to upload into a subfolder. The library name is often "Shared Documents" or "Documents".

**Options:** `--count` (default 5000), `--size-mb` (default 10), `--prefix` (default `sample`; files are named `sample_00001.bin`, …), `--dry-run` to print the plan without uploading.

## Smoke Test

```bash
curl http://localhost:8000/api/v1/healthcheck
# {"status":"ok"}
```
