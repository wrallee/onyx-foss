#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

sha256sum --check <<'CHECKSUMS'
9dc67b11a2f629359329ccef6c8a572477ee316375dcbf664fad1b8c90d812f3  models/granite-embedding-311m-multilingual-r2-int8-openvino/openvino/openvino_model_qint8_quantized.xml
19cd99b8657e8f86529ce05384194b1bf3dbde94fd48a571a832344c119c27bc  models/granite-embedding-311m-multilingual-r2-int8-openvino/openvino/openvino_model_qint8_quantized.bin
0087c868b33bad550a78a08d19798cfd7f713cde4f020803b8f51f405503e15f  models/granite-embedding-311m-multilingual-r2-int8-openvino/tokenizer.json
781299da695e58439d70d491840da22ea0935d1d57d9646eb9725f1f19754e89  models/granite-embedding-311m-multilingual-r2-int8-openvino/1_Pooling/config.json
10ebaa49322dd7e01a13a91c49810939e3f91f231aceaa47fdf0cab3083954f6  models/gte-multilingual-reranker-base/model.safetensors
d6f76fe13d42f80dcee0cb86a1aeb5f14f8909bb8a8782f7a4a4ad76697ef164  models/gte-multilingual-reranker-base/tokenizer.json
d9e3e081faff1eefb84019509b2f5558fd74c1a05a2c7db22f74174fcedb5286  models/bge-reranker-v2-m3/model.safetensors
69564b696052886ed0ac63fa393e928384e0f8caada38c1f4864a9bfbf379c15  models/bge-reranker-v2-m3/tokenizer.json
cfc8146abe2a0488e9e2a0c56de7952f7c11ab059eca145a0a727afce0db2865  models/bge-reranker-v2-m3/sentencepiece.bpe.model
CHECKSUMS

echo "Model artifact check passed."
