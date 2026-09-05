# Onyx FOSS Kotlin port

This branch keeps the approved local FOSS admin UI and Kotlin management backend,
and uses a Python 3.13 model-server for both embedding and reranking.

## Supported scope

- File, Jira, Confluence, and GitHub connector administration and collection
- Connector credentials encrypted at rest with AES-GCM
- PostgreSQL-backed jobs, attempts, checkpoints, and document sets
- Tika extraction, Python model-server calls, and OpenSearch indexing
- Hybrid keyword and vector search through a Streamable HTTP MCP endpoint
- Authentication-free admin UI for local or private networks

All other connector cards and unrelated admin routes remain visible but are
disabled. Document Sets are always public; user and group controls are visible
but disabled.

## Model artifacts

The selected model-server implementation on this branch is Python 3.13. The
following pinned, Apache-2.0 artifacts are downloaded under the ignored `models/`
directory:

- Granite 311M Multilingual R2 INT8 OpenVINO embedding
- GTE multilingual reranker-base
- BGE reranker v2 m3 candidate

See `MODELS.md` for revisions and SHA-256 values.

The Python service runs Granite INT8 through the OpenVINO Python API and GTE
through its pinned PyTorch custom model code. It exposes the existing Onyx
embedding and cross-encoder endpoints. The Kotlin backend can submit candidate
arrays to `/manage/search/rerank`; it sorts by returned scores and preserves the
retrieval order if the reranker is unavailable. BGE remains selectable through
`RERANKER_MODEL` and `RERANKER_MODEL_PATH`.

## Run

Download the pinned Granite artifact, then create the local environment file and set a unique credential key:

```bash
cd _kotlin
python3 scripts/download_models.py --with-rerankers
cp .env.example .env
openssl rand -base64 32
# Set ONYX_CREDENTIAL_ENCRYPTION_KEY and a strong OPENSEARCH_ADMIN_PASSWORD in .env.
docker compose config -q
docker compose build
docker compose up -d
```

For an isolated stack, use a separate project, port, subnet, and read-only model path:

```bash
COMPOSE_PROJECT_NAME=onyx-kotlin-isolated \
WEB_PORT=13300 \
COMPOSE_SUBNET=192.168.241.0/24 \
MODEL_DIR=/absolute/path/to/models \
docker compose up -d --build

COMPOSE_PROJECT_NAME=onyx-kotlin-isolated \
WEB_PORT=13300 \
./scripts/test-kotlin-file-ingestion.sh

COMPOSE_PROJECT_NAME=onyx-kotlin-isolated \
WEB_PORT=13300 \
COMPOSE_SUBNET=192.168.241.0/24 \
MODEL_DIR=/absolute/path/to/models \
docker compose down -v --remove-orphans
```

The script derives `http://localhost:13300` from `WEB_PORT`. Set
`WEB_BASE_URL` only when the Web service uses another address.

Start the UI, API, worker, Python model-server, PostgreSQL, and OpenSearch:

```bash
docker compose up -d
```

Open `http://localhost:3000`. Only the Web service is published to the host.

## MCP search

Connect remote MCP clients to `https://onyx-admin.com/mcp`. The Web service
proxies this path to the Kotlin backend. The backend endpoint is not published
directly.

```json
{
  "mcpServers": {
    "onyx": {
      "url": "https://onyx-admin.com/mcp"
    }
  }
}
```

The `search` tool accepts an optional `document_sets` array. It searches the
union of those sets. It retrieves 50 keyword and vector candidates by default,
then reranks at most 30. Set `ONYX_SEARCH_CANDIDATES` and
`ONYX_SEARCH_RERANK_CANDIDATES` to change these values.

Delete the existing OpenSearch index before this version is deployed. The
application does not delete it. It rejects an incompatible embedding mapping
and waits for a clean reindex.

The MCP endpoint has no authentication in this development version. Do not
expose it beyond the intended private environment until authentication exists.

## Verification

```bash
# Backend
cd backend
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew test

# OpenSearch integration profile (starts one shared container)
JAVA_HOME="$HOME/.sdkman/candidates/java/21-zulu" ./gradlew opensearchIntegrationTest
cd ..

# Web-to-PostgreSQL-to-OpenSearch File ingestion
./scripts/test-kotlin-file-ingestion.sh

# Python model-server
cd model-server
python3.13 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest -q
cd ..

# Frontend (Node.js 24)
cd web
source "$HOME/.nvm/nvm.sh"
nvm use 24
npm ci --legacy-peer-deps
npm run types:check
npm run build
```

The File, Jira, Confluence, and GitHub loaders support bounded batches,
checkpoints, poll windows, pruning, and document failures.
Document Set changes update existing OpenSearch chunks.

Image-specific vector embedding is not complete. It remains WATCHLIST work.

See `SOURCE_PROVENANCE.md` for source and license boundaries and
`docs/model-server-spike.md` for the model compatibility gate.
