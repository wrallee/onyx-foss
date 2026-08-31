# Onyx FOSS Kotlin port

This branch keeps the approved local FOSS admin UI and Kotlin management backend,
and uses a Python 3.13 model-server for both embedding and reranking.

## Supported scope

- File, Jira, Confluence, and GitHub connector administration and collection
- Connector credentials encrypted at rest with AES-GCM
- PostgreSQL-backed jobs, attempts, checkpoints, and document sets
- Tika extraction, Python model-server calls, and OpenSearch indexing
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
# Paste the result into ONYX_CREDENTIAL_ENCRYPTION_KEY in .env.
docker compose config -q
docker compose build
docker compose up -d
```

Start the UI, API, worker, Python model-server, PostgreSQL, and OpenSearch:

```bash
docker compose up -d
```

Open `http://localhost:3000`. Only the Web service is published to the host.

## Verification

```bash
# Backend
docker run --rm -v "$PWD/backend:/workspace" -w /workspace \
  gradle:8.14.3-jdk21 gradle test --no-daemon

# Python model-server
cd python-model-server
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

## Current connector limits

- Jira uses the REST v2 search endpoint.
- Confluence collects pages but not attachments or comments.
- GitHub private organization repositories must be listed explicitly.
- Remote Connector rate-limit retry and backoff are not implemented yet.

See `SOURCE_PROVENANCE.md` for source and license boundaries and
`docs/model-server-spike.md` for the model compatibility gate.
