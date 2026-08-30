# Kotlin model-server PoC

This Java 21 / Spring Boot service implements the local Onyx FOSS model-server
HTTP contract needed by the Kotlin Connector and Document Set path:

- `GET /api/health` is liveness and returns 200 while the process is running.
- `GET /api/gpu-status` currently reports `NONE` until an accelerator-aware
  runtime is implemented.
- The embedding endpoint runs the verified local Granite INT8 OpenVINO model and returns actual vectors. Readiness is UP only after model hashes, tokenizer loading and CPU compilation succeed.

It does **not** fabricate vectors, download a model, invoke the Python model
server, or silently select a Python fallback.

## Selected local artifacts

The local model inventory pins Granite embedding to
`ibm-granite/granite-embedding-311m-multilingual-r2` revision
`44399559930365213510b1ee2eb15ded83374f0e`, Apache-2.0. Its checked-in local
development manifest is `granite-openvino-int8.manifest.json`, which verifies:

- OpenVINO INT8 XML and BIN files;
- `tokenizer.json`;
- 768 output dimensions, CLS pooling, and 32,768-token context.

The runtime uses the exact local tokenizer JSON, passes input IDs and attention masks to OpenVINO, pools token zero as CLS, and L2-normalizes only when requested.

`Alibaba-NLP/gte-multilingual-reranker-base` revision
`8215cf04918ba6f7b6a62bb44238ce2953d8831c` and
`BAAI/bge-reranker-v2-m3` revision
`953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e` are Apache-2.0 selectable reranker
candidates. Both local downloads are safetensors artifacts, not approved OpenVINO
or ONNX exports. `POST /encoder/cross-encoder-scores` therefore returns
`RERANKER_EXPORT_NOT_CONFIGURED` until one pinned export passes the golden score
suite. It never returns fabricated scores.

## Artifact contract

Set `MODEL_ARTIFACT_MANIFEST` to an absolute path to a JSON manifest conforming
to `src/main/resources/model-artifact-manifest.schema.json`. Model and tokenizer
paths can be relative to the manifest directory. The PoC verifies each declared
SHA-256 before it considers an artifact present.

An approved artifact package must contain:

1. An OpenVINO IR XML and BIN pair for the approved model. 2. The exact tokenizer JSON and special-token configuration. 3. Pooling, normalization, license and provenance records.

`model-artifact-manifest.example.json` is intentionally invalid: its paths and
hashes are placeholders.

A missing or mismatched manifest remains MODEL_ARTIFACT_NOT_CONFIGURED and keeps readiness DOWN. A verified Granite artifact that cannot initialize reports MODEL_RUNTIME_INITIALIZATION_FAILED. The endpoint never fabricates vectors.

## Build and run

The module uses Gradle Kotlin DSL and a Java 21 toolchain. Docker builds with a
Java 21 Gradle builder, so it does not require a host JDK.

```bash
cd _kotlin/model-server
gradle test
docker build -t onyx-foss-kotlin-model-server .
docker run --rm -p 9000:9000 \
  -e MODEL_ARTIFACT_MANIFEST=/models/manifest.json \
  -v /absolute/path/to/approved-model-package:/models:ro \
  onyx-foss-kotlin-model-server
```

With the Granite manifest and local model mounted, readiness becomes UP and embedding requests return 768-dimensional vectors.

```bash
curl -i http://localhost:9000/api/health
curl -s http://localhost:9000/actuator/health/readiness
curl -s -X POST http://localhost:9000/encoder/bi-encoder-embed \
  -H 'Content-Type: application/json' \
  -d '{"texts":["hello"],"model_name":"ibm-granite/granite-embedding-311m-multilingual-r2","max_context_length":512,"normalize_embeddings":true,"text_type":"query"}'
```

## Adapter boundary

The production adapter uses the embedded Apache-2.0 DJL HuggingFace tokenizer and the OpenVINO C ABI from the JavaCPP-packaged runtime. It is a real JVM inference implementation, not a Python fallback. Reranking remains separately gated until an approved ONNX or OpenVINO export is supplied.

## Verification

The test suite uses the downloaded Granite model for English and Korean contract checks: output dimensions, finite values, normalization, repeat cosine and prefix handling. Docker smoke verification confirms readiness and a real HTTP embedding response. Reranker readiness is exposed separately and does not block ingestion readiness.

## Licensing

This module is new code and contains no model weights or copied third-party
runtime. Preserve the local Onyx FOSS MIT license and attribution for any copied
FOSS code. Independently approve and record the license for every model,
tokenizer, native runtime and library before distribution.
