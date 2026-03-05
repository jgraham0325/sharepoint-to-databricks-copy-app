# SharePoint Upload App

A Databricks App that lets users browse a SharePoint site, select files, and copy them into a Databricks Unity Catalog Volume. FastAPI backend + React frontend.

## Prerequisites

- Python 3.9+
- Node.js 18+
- A Databricks workspace (Azure) with Unity Catalog enabled
- A Microsoft Entra ID (Azure AD) app registration with the following API permissions:
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

Open [http://localhost:5173](http://localhost:5173). The Vite dev server proxies `/api` requests to the backend on port 8000.

## Deploying to Databricks

### 1. Store Microsoft credentials as Databricks secrets

Create a secret scope and add your three secrets:

```bash
databricks secrets create-scope sharepoint-app-scope

databricks secrets put-secret sharepoint-app-scope ms-client-id --string-value "<your client ID>"
databricks secrets put-secret sharepoint-app-scope ms-client-secret --string-value "<your client secret>"
databricks secrets put-secret sharepoint-app-scope ms-tenant-id --string-value "<your tenant ID>"
```

These names match what `back-end/app.yaml` references under `value_from`.

### 2. Build the frontend

```bash
cd front-end
npm install
npm run build
```

The build output in `back-end/static/` is deployed alongside the backend.

### 3. Update the redirect URI

Once you know your Databricks App URL (e.g. `https://sharepoint-upload-app-1234567890.azuredatabricks.net`), add it as a redirect URI in your Entra app registration:

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

```bash
databricks bundle deploy -t dev
```

After deployment, open your Databricks workspace, navigate to **Compute > Apps**, and find `sharepoint-upload-app` to see its URL and status.

## Project Structure

```
sharepoint-upload-app/
├── databricks.yml              # DAB deployment config
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

## Smoke Test

```bash
curl http://localhost:8000/api/v1/healthcheck
# {"status":"ok"}
```
