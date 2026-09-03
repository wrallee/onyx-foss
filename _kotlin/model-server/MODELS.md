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
