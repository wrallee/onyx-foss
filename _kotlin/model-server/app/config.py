from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


def _boolean(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    embedding_model_path: Path
    embedding_model_name: str
    embedding_openvino_file: str
    reranker_model_path: Path
    reranker_model_name: str
    reranker_required: bool
    reranker_max_documents: int
    reranker_max_length: int
    inference_concurrency: int
    torch_threads: int

    @classmethod
    def from_environment(cls) -> "Settings":
        return cls(
            embedding_model_path=Path(
                os.getenv(
                    "EMBEDDING_MODEL_PATH",
                    "/models/granite-embedding-311m-multilingual-r2-int8-openvino",
                )
            ),
            embedding_model_name=os.getenv(
                "EMBEDDING_MODEL_NAME",
                "ibm-granite/granite-embedding-311m-multilingual-r2",
            ),
            embedding_openvino_file=os.getenv(
                "EMBEDDING_OPENVINO_FILE",
                "openvino/openvino_model_qint8_quantized.xml",
            ),
            reranker_model_path=Path(
                os.getenv(
                    "RERANKER_MODEL_PATH",
                    "/models/gte-multilingual-reranker-base",
                )
            ),
            reranker_model_name=os.getenv(
                "RERANKER_MODEL",
                "Alibaba-NLP/gte-multilingual-reranker-base",
            ),
            reranker_required=_boolean("RERANKER_REQUIRED", True),
            reranker_max_documents=int(os.getenv("RERANKER_MAX_DOCUMENTS", "100")),
            reranker_max_length=int(os.getenv("RERANKER_MAX_LENGTH", "512")),
            inference_concurrency=max(1, int(os.getenv("MODEL_INFERENCE_CONCURRENCY", "1"))),
            torch_threads=max(1, int(os.getenv("TORCH_NUM_THREADS", "4"))),
        )
