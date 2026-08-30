# Model-server porting spike

## Implemented outcome

The user selected Kotlin. The production PoC now runs the pinned Granite 311M
Multilingual R2 INT8 OpenVINO artifact through JNA bindings to the bundled
OpenVINO C API and the exact DJL Hugging Face tokenizer. Docker validation as
UID 10001 returned English and Korean 768-dimensional normalized embeddings;
the full Compose File ingestion path also completed and indexed two chunks.

GTE multilingual reranker-base and BGE reranker v2 m3 are downloaded, pinned
candidates. Reranking remains disabled until one candidate has a validated
OpenVINO or ONNX export and passes the score golden suite.

## Scope and source boundary

This note is based only on the local FOSS checkout at
`/home/wrallee/Workspace/onyx-foss`, at commit
`60b2c0c3616bba8bd56c1c8ce02d320c79b0b06f` (2026-08-29). It inspects
`backend/model_server` and its local call sites. `_kotlinmania` and external
repositories were not read or used.

This is a feasibility spike, not an implementation or a language decision.
The final model-server implementation remains a user choice after the
acceptance checks below: **Kotlin/JVM, Go, retain the Python implementation,
or use an external compatible inference service**. A failed Kotlin spike must
not silently select Python.

## Active FOSS model-server contract

`model_server.main` only mounts `management_endpoints.router` and
`encoders.router`. The files under `backend/model_server/legacy` are not
mounted; their reranker and intent routes are commented out. Cloud embedding
providers also bypass this server in the local caller.

| Method and path | Required behaviour | Consumer |
| --- | --- | --- |
| `POST /encoder/bi-encoder-embed` | Embed a non-empty batch with a local model; return `{"embeddings": [[float, ...], ...]}` in input order. | `EmbeddingModel` in `onyx/natural_language_processing/search_nlp_models.py` |
| `GET /api/health` | Return HTTP 200 with no required body. | Compose and platform health checks |
| `GET /api/gpu-status` | Return `{"gpu_available": boolean, "type": "CUDA" | "MAC_MPS" | "NONE"}`. | `onyx/utils/gpu_utils.py` |
| `GET /metrics` | Prometheus metrics endpoint installed by the FastAPI instrumentator. | Operations/monitoring |

The local caller builds the embedding URL as
`http://{MODEL_SERVER_HOST}:{MODEL_SERVER_PORT}/encoder/bi-encoder-embed`.
It forwards optional `X-Onyx-Tenant-ID` and `X-Onyx-Request-ID` headers. A new
service should accept and propagate those headers to logs/traces when present,
but no authentication or tenant authorization is required by this model
endpoint.

The request shape currently sent to a local model is:

```json
{
  "texts": ["..."],
  "model_name": "nomic-ai/nomic-embed-text-v1",
  "max_context_length": 512,
  "normalize_embeddings": true,
  "provider_type": null,
  "text_type": "query",
  "manual_query_prefix": "search_query: ",
  "manual_passage_prefix": "search_document: "
}
```

`deployment_name`, `api_key`, `api_url`, `api_version`, and
`reduced_dimension` are part of the shared request schema but are not used by
the active local model-server route. `provider_type` must be absent or `null`;
non-null provider requests are rejected because cloud providers are called
directly by the application instead.

Compatibility cases to preserve during a port:

- An absent/empty `texts` array returns HTTP 400 with `No texts to be embedded`.
- Empty strings or a missing local `model_name` fail the embedding request. The
  current FastAPI wrapper translates these failures to HTTP 500; a replacement
  should either preserve that behaviour for compatibility or deliberately
  change it together with the Kotlin client contract.
- The caller retries passage embedding network/JSON failures three times and
  rate-limit responses up to ten times. It treats HTTP 429 specially.
- The server retries `RuntimeError` from SentenceTransformers up to three times
  because concurrent encodes can fail with `Already borrowed`.

## Preprocessing and model coupling

The active encoder is smaller than the full historical model-server surface,
but it is not just a vector HTTP wrapper.

1. It caches one `SentenceTransformer` per `model_name`, loads it with
   `trust_remote_code=False`, and sets `model.max_seq_length` from every
   request. The default bundled model is `nomic-ai/nomic-embed-text-v1`.
2. It prepends `manual_query_prefix` only for `text_type=query` and
   `manual_passage_prefix` only for `text_type=passage`, before tokenization.
   The defaults are `search_query: ` and `search_document: `.
3. It calls `SentenceTransformer.encode(texts,
   normalize_embeddings=normalize_embeddings)`. Pooling, the tokenizer,
   special tokens, truncation and normalization are therefore coupled to the
   actual SentenceTransformers model artifact, not expressed in the HTTP API.
4. The application independently loads a Hugging Face tokenizer to split
   source content before requesting embeddings. Chunk-boundary equivalence
   therefore also depends on using the same tokenizer and the requested
   context length.
5. The local defaults are a 512-token context, 768 dimensions and normalized
   embeddings. `DOCUMENT_ENCODER_MODEL`, `DOC_EMBEDDING_DIM`,
   `NORMALIZE_EMBEDDINGS`, both prefixes, and batch size can be overridden by
   environment/configuration; the port must not hard-code only the defaults.

The source tree downloads the default model during the Python Docker build and
does not contain its resolved `modules.json`, tokenizer files, model weights,
or any ONNX export. Before choosing a JVM or Go runtime, inspect the **actual
approved model artifact** used in the target deployment and record:

- tokenizer files and tokenizer version;
- SentenceTransformers `modules.json`, pooling mode, normalization module, and
  model config;
- native weight format and whether a faithful ONNX export exists or can be
  created from the approved artifact;
- model and dependency licenses, hashes, and intended CPU/GPU execution mode.

Without that artifact audit, a source-only review cannot prove exact Kotlin or
Go numerical compatibility.

## Runtime and deployment behaviour to carry forward

- The entry point exits cleanly without importing ML libraries when
  `DISABLE_MODEL_SERVER` is enabled, which is how an external compatible
  endpoint is supported.
- Startup pre-warms the model's RoPE/cache, detects CUDA and Apple MPS, and
  caps Torch threads to the container cgroup CPU quota. A port needs an
  equivalent warm-up, accelerator capability response, and quota-aware worker
  sizing rather than using all host cores.
- The image preloads the default model into the Hugging Face cache. At runtime
  it merges that cache into a mounted cache volume. A replacement needs an
  explicit immutable model-cache/image strategy; it must not download a model
  on each application request.
- Custom CA roots are additive to public roots. Preserve that behaviour if a
  replacement downloads models or calls any HTTPS endpoint.
- The original deployment has separate inference and indexing model-server
  services to prevent ingestion from delaying inference. The planned
  Connector/Document Set system needs only the indexing path initially, but
  its service contract should remain deployable as a separate instance.

## Kotlin/JVM feasibility

Kotlin is feasible **if** the approved model can be represented by a supported
runtime (typically an ONNX graph) and its tokenizer/pooling pipeline can be
made equivalent. The HTTP surface can be implemented simply with Spring Boot
or Ktor, and a JVM ONNX Runtime binding can execute an approved exported graph.

The high-risk portion is model parity, not the web API:

- ONNX Runtime does not by itself reproduce SentenceTransformers module order,
  Hugging Face fast-tokenizer behaviour, prefixing, special tokens, pooling or
  normalization.
- The default source build uses PyTorch + Transformers +
  SentenceTransformers. It proves an ordinary SentenceTransformers load with
  remote code disabled, but does not prove an ONNX artifact is present or that
  its output includes the same pooling stage.
- A JVM tokenizer solution must be tested against the approved model's exact
  tokenizer files, including Korean/Unicode and truncation. Substituting a
  merely similar tokenizer is not acceptable for existing vectors.
- GPU support requires matching native runtime/provider packaging and device
  selection. Start with the target CPU profile unless the deployment requires
  GPU parity.

Recommended Kotlin spike deliverable: a small standalone service exposing only
the three required HTTP endpoints, loading one immutable approved model, with
model, tokenizer and pooling adapters kept explicit and covered by the golden
suite below. Do not integrate it into the main backend until it passes.

## Go feasibility

Go is also feasible **if** the same model is available in an executable native
format and an exact tokenizer/pooling implementation is selected. It has the
same HTTP-contract simplicity as Kotlin, but generally requires a native
inference runtime (for example an approved ONNX Runtime binding) and native
tokenizer integration rather than a pure-Go port of SentenceTransformers.

Its main risks are:

- CGO/native runtime, CPU instruction set and GPU-provider image compatibility;
- exact Hugging Face tokenizer parity and SentenceTransformers pooling outside
  the Python library;
- reproducible cross-platform builds and model-cache ownership;
- operational ownership of the native inference library upgrade cadence.

Recommended Go spike deliverable: the same minimal endpoint contract and the
same artifact manifest/golden suite as Kotlin. Do not treat lower HTTP-service
complexity as evidence of numerical compatibility.

## Concrete golden, benchmark, and acceptance design

Generate the baseline from the existing local Python model server with the
**same approved model artifact and environment values**. Store no proprietary
source documents or credentials in fixtures.

### Golden fixture

- Request cases: one item and the indexing batch size; English, Korean,
  mixed Unicode/emoji, punctuation, an input near the requested token limit,
  an input above it, and text that demonstrates both query and passage
  prefixes.
- Configuration cases: default 512 context/768 dimensions with normalization
  on; normalization off; a smaller supported context; custom query/passage
  prefixes; every approved local model configuration.
- Protocol/error cases: health, GPU status, no `texts`, `texts=[""]`, missing
  model name, and non-null provider type.
- Persist each request, response vector, vector length, finite-value check,
  L2 norm, baseline model artifact hash, config values, server image digest,
  and source commit. Never serialize API keys.

For each candidate server, require the same response cardinality/order and
dimension, finite values, prefix behaviour, and the following per-vector
checks against the Python baseline:

| Check | Initial pass criterion |
| --- | --- |
| Normalized-vector norm | `abs(norm - 1.0) <= 1e-5` |
| Cosine similarity | `>= 0.99999` for the same float model/artifact |
| Retrieval equivalence | identical top-10 on a fixed multilingual corpus; document any tie ordering |
| HTTP behaviour | required paths/statuses and JSON field names match |

If quantization, a changed exported graph, or a different accelerator makes
the cosine threshold unattainable, report the measured distribution and ask
the user to approve a new threshold before reindexing. Do not silently weaken
the criterion.

### Benchmark

Run each candidate and the Python baseline on the same locked image/model hash,
hardware, cgroup CPU quota, batch sizes, and warm-up sequence. Measure:

- cold-start time to health and time to first successful embedding;
- warm p50/p95 latency and throughput for query batch 1 and indexing batches;
- RSS, CPU, and GPU memory where applicable;
- 1, 2, and configured-concurrency request load, including failure/retry rate;
- image size and model-cache size.

Use at least one warm-up phase followed by three independent measured runs.
Report median and worst run, not only the best result. The proposed planning
gate is no more than 20% worse than the Python baseline for warm throughput and
memory; if that is not met, report the data for a user decision rather than
choosing a fallback automatically.

## Decision checkpoint

Stop after the artifact audit and Kotlin/Go proof(s) produce the golden and
benchmark report. Present the following to the user before changing Compose or
selecting a production model-server implementation:

| Option | Evidence to present |
| --- | --- |
| Kotlin/JVM | artifact adapter status, parity results, benchmark, native packaging/image details |
| Go | artifact adapter status, parity results, benchmark, native packaging/image details |
| Existing Python | baseline performance/image/operations data; only if the user explicitly selects it |
| External compatible service | accepted endpoint contract, trust/TLS/model-cache ownership, parity and latency data |

The user must explicitly choose one option. Until then, no Compose service is
added, replaced, or defaulted to Python.

## Licensing and provenance

The checked local checkout's root `LICENSE` is MIT (copyright
2023-present DanswerAI, Inc.), and the local `Dockerfile.model_server` labels
the model-server code/image as MIT-licensed. Preserve the root license,
copyright notice, source attribution and any retained file headers in every
copy or substantial portion.

That source-level finding does **not** clear the downloaded model weights,
tokenizer, JVM/Go libraries, or native runtimes. Before implementation, add a
machine-readable provenance/SBOM record with each artifact's exact source,
version/hash and license, and reject an artifact whose license is not approved.
The local configuration itself states that configured models must be MIT or
Apache licensed; verify that assertion from the acquired artifact metadata
instead of bypassing any licensing control.

## Local evidence

- `backend/model_server/encoders.py`: active encoder route, cache, prefixing,
  normalization, request validation, and concurrency retry.
- `backend/model_server/main.py` and `management_endpoints.py`: mounted routes,
  metrics, lifecycle, health, and GPU status.
- `backend/shared_configs/model_server_models.py` and `enums.py`: request and
  response contract.
- `backend/onyx/natural_language_processing/search_nlp_models.py`: caller URL,
  request serialization, headers, and retries.
- `backend/shared_configs/configs.py` and `backend/onyx/configs/model_configs.py`:
  default model, context, dimension, normalization, and prefixes.
- `backend/Dockerfile.model_server` and
  `deployment/docker_compose/docker-compose.yml`: Python dependencies, model
  preload/cache, two-service deployment, and health check.
- `backend/tests/unit/model_server/test_embedding.py` and
  `backend/tests/airgap/test_default_model_server_embeddings_are_finite.py`:
  existing basic concurrency and finite/dimension coverage.
