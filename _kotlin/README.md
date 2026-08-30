# Onyx FOSS Kotlin port

This project keeps the approved local FOSS admin UI and replaces the supported
management and ingestion backend with Kotlin.

## Supported scope

- File, Jira, Confluence, and GitHub connector administration and collection
- Connector credentials encrypted at rest with AES-GCM
- PostgreSQL-backed jobs, attempts, checkpoints, and document sets
- Tika extraction, Kotlin model-server calls, and OpenSearch indexing
- Authentication-free admin UI for local or private networks

All other connector cards and unrelated admin routes remain visible but are
disabled. Document Sets are always public; user and group controls are visible
but disabled.

## Model artifacts

The selected model-server implementation is Kotlin. The following pinned,
Apache-2.0 artifacts are downloaded under the ignored `models/` directory:

- Granite 311M Multilingual R2 INT8 OpenVINO embedding
- GTE multilingual reranker-base
- BGE reranker v2 m3 candidate

See `MODELS.md` for revisions and SHA-256 values.

The Kotlin service runs the real Granite INT8 OpenVINO model through JNA and the
OpenVINO C API. DJL loads the exact Hugging Face tokenizer. The service returns
768-dimensional CLS-pooled vectors with optional L2 normalization and never
calls Python or fabricates output. GTE and BGE reranking remain separate
candidates and return `RERANKER_EXPORT_NOT_CONFIGURED` until a selected export
passes the golden score suite. Reranker readiness does not block ingestion.

## Run

Create the local environment file and set a unique credential key:

```bash
cd _kotlin
cp .env.example .env
openssl rand -base64 32
# Paste the result into ONYX_CREDENTIAL_ENCRYPTION_KEY in .env.
docker compose config -q
docker compose build
docker compose up -d
```

The default profile starts the UI, API, PostgreSQL, and OpenSearch. The worker
and Kotlin model-server are in the `ingestion` profile. The downloaded Granite
artifact passes readiness, so enable ingestion with:

```bash
docker compose --profile ingestion up -d
```

Open `http://localhost:3000`. Only the Web service is published to the host.

## Verification

```bash
# Backend
docker run --rm -v "$PWD/backend:/workspace" -w /workspace \
  gradle:8.14.3-jdk21 gradle test --no-daemon

# Kotlin model-server
docker run --rm -v "$PWD:/workspace" -w /workspace/model-server \
  gradle:8.14.4-jdk21 gradle test --no-daemon

# Frontend (Node.js 24)
cd web
source "$HOME/.nvm/nvm.sh"
nvm use 24
npm ci --legacy-peer-deps
npm run types:check
npm run build
```

## Current connector limits

- Jira uses the REST v2 search endpoint.
- Confluence collects pages but not attachments or comments.
- GitHub private organization repositories must be listed explicitly.
- Remote Connector rate-limit retry and backoff are not implemented yet.

See `SOURCE_PROVENANCE.md` for source and license boundaries and
`docs/model-server-spike.md` for the model compatibility gate.
