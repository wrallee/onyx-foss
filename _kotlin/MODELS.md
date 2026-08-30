# Model artifacts

Model binaries are stored under the ignored `models/` directory. They are not
committed to Git. Each download is pinned to a Hugging Face revision.

## Granite embedding

- Repository: `ibm-granite/granite-embedding-311m-multilingual-r2`
- Revision: `44399559930365213510b1ee2eb15ded83374f0e`
- License: Apache-2.0
- Runtime format: OpenVINO INT8, 768 output dimensions, CLS pooling
- Local directory: `models/granite-embedding-311m-multilingual-r2-int8-openvino`

Verified files:

| File | SHA-256 |
| --- | --- |
| `openvino/openvino_model_qint8_quantized.xml` | `9dc67b11a2f629359329ccef6c8a572477ee316375dcbf664fad1b8c90d812f3` |
| `openvino/openvino_model_qint8_quantized.bin` | `19cd99b8657e8f86529ce05384194b1bf3dbde94fd48a571a832344c119c27bc` |
| `tokenizer.json` | `0087c868b33bad550a78a08d19798cfd7f713cde4f020803b8f51f405503e15f` |
| `1_Pooling/config.json` | `781299da695e58439d70d491840da22ea0935d1d57d9646eb9725f1f19754e89` |

## GTE multilingual reranker

- Repository: `Alibaba-NLP/gte-multilingual-reranker-base`
- Revision: `8215cf04918ba6f7b6a62bb44238ce2953d8831c`
- License: Apache-2.0
- Source format: safetensors, 306M parameters
- Local directory: `models/gte-multilingual-reranker-base`

Verified files:

| File | SHA-256 |
| --- | --- |
| `model.safetensors` | `10ebaa49322dd7e01a13a91c49810939e3f91f231aceaa47fdf0cab3083954f6` |
| `tokenizer.json` | `d6f76fe13d42f80dcee0cb86a1aeb5f14f8909bb8a8782f7a4a4ad76697ef164` |

## BGE reranker candidate

- Repository: `BAAI/bge-reranker-v2-m3`
- Revision: `953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e`
- License: Apache-2.0
- Source format: safetensors
- Local directory: `models/bge-reranker-v2-m3`

Verified files:

| File | SHA-256 |
| --- | --- |
| `model.safetensors` | `d9e3e081faff1eefb84019509b2f5558fd74c1a05a2c7db22f74174fcedb5286` |
| `tokenizer.json` | `69564b696052886ed0ac63fa393e928384e0f8caada38c1f4864a9bfbf379c15` |
| `sentencepiece.bpe.model` | `cfc8146abe2a0488e9e2a0c56de7952f7c11ab059eca145a0a727afce0db2865` |

The official reranker artifacts are not OpenVINO graphs. The Kotlin runtime
must not claim readiness for either reranker until a pinned OpenVINO or ONNX
export passes the golden score tests. It must never fabricate scores or call
Python implicitly. GTE and BGE remain separate comparison candidates until that
evaluation selects one.
