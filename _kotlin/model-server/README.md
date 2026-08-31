# Python 3.13 model server

This service provides both local model contracts used by Onyx:

- `POST /encoder/bi-encoder-embed`: Granite 311M Multilingual R2 INT8 OpenVINO
- `POST /encoder/cross-encoder-scores`: GTE multilingual reranker-base by default

It runs inside `python:3.13-slim`; the host Python version is irrelevant. Models
are mounted read-only from `../models` and are never downloaded at request time.
Use `BAAI/bge-reranker-v2-m3` plus its local path to select the BGE candidate.

The GTE runtime loads the pinned Apache-2.0 `Alibaba-NLP/new-impl` source copied
by `scripts/download_models.py --with-rerankers`. Inference is serialized by
default to keep CPU and memory usage bounded.
