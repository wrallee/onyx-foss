from __future__ import annotations

from dataclasses import dataclass
import importlib
import math
from pathlib import Path
import sys
from threading import BoundedSemaphore
from typing import Any

import numpy as np

from app.config import Settings
from app.contracts import EmbedRequest, EmbedTextType, RerankRequest


@dataclass(frozen=True)
class RuntimeStatus:
    ready: bool
    code: str
    message: str
    model_name: str
    device: str = "CPU"


class EmbeddingRuntime:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._tokenizer: Any = None
        self._compiled_model: Any = None
        self._semaphore = BoundedSemaphore(settings.inference_concurrency)
        self.status = RuntimeStatus(
            False,
            "NOT_LOADED",
            "Granite embedding runtime is not loaded.",
            settings.embedding_model_name,
        )

    def load(self) -> None:
        from openvino import Core
        from transformers import AutoTokenizer

        model_path = self.settings.embedding_model_path
        tokenizer_path = model_path / "tokenizer.json"
        openvino_path = model_path / self.settings.embedding_openvino_file
        for required in (tokenizer_path, openvino_path):
            if not required.is_file():
                raise FileNotFoundError(f"Required embedding artifact is missing: {required}")

        self._tokenizer = AutoTokenizer.from_pretrained(
            model_path,
            local_files_only=True,
            trust_remote_code=False,
        )
        self._compiled_model = Core().compile_model(str(openvino_path), "CPU")
        self.status = RuntimeStatus(
            True,
            "READY",
            "Granite INT8 OpenVINO embedding runtime is ready.",
            self.settings.embedding_model_name,
        )

    def embed(self, request: EmbedRequest) -> list[list[float]]:
        if not self.status.ready:
            raise RuntimeError(self.status.message)
        if request.provider_type is not None:
            raise ValueError("provider_type must be null for a local embedding model.")
        if not request.texts or any(not text for text in request.texts):
            raise ValueError("texts must contain only non-empty strings.")
        if request.model_name not in (None, self.settings.embedding_model_name):
            raise ValueError(f"Unsupported embedding model: {request.model_name}")

        prefix = None
        if request.text_type == EmbedTextType.QUERY:
            prefix = request.manual_query_prefix
        elif request.text_type == EmbedTextType.PASSAGE:
            prefix = request.manual_passage_prefix
        texts = [f"{prefix}{text}" if prefix else text for text in request.texts]

        encoded = self._tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=request.max_context_length,
            return_tensors="np",
        )
        inputs = {
            "input_ids": encoded["input_ids"].astype(np.int64, copy=False),
            "attention_mask": encoded["attention_mask"].astype(np.int64, copy=False),
        }
        with self._semaphore:
            outputs = self._compiled_model(inputs)
        hidden = self._output(outputs, "last_hidden_state")
        vectors = np.asarray(hidden[:, 0, :], dtype=np.float32)
        if request.normalize_embeddings:
            norms = np.linalg.norm(vectors, axis=1, keepdims=True)
            if np.any(norms == 0):
                raise RuntimeError("Embedding model returned a zero vector.")
            vectors = vectors / norms
        if request.reduced_dimension is not None:
            vectors = vectors[:, : request.reduced_dimension]
        return vectors.tolist()

    @staticmethod
    def _output(outputs: Any, name: str) -> np.ndarray:
        for key, value in outputs.items():
            if getattr(key, "any_name", None) == name:
                return np.asarray(value)
        if len(outputs) != 1:
            raise RuntimeError(f"OpenVINO output {name} was not found.")
        return np.asarray(next(iter(outputs.values())))


class RerankerRuntime:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._tokenizer: Any = None
        self._model: Any = None
        self._torch: Any = None
        self._semaphore = BoundedSemaphore(settings.inference_concurrency)
        self.status = RuntimeStatus(
            False,
            "NOT_LOADED",
            "Reranker runtime is not loaded.",
            settings.reranker_model_name,
        )

    def load(self) -> None:
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        torch.set_num_threads(self.settings.torch_threads)
        model_path = self.settings.reranker_model_path
        if not (model_path / "model.safetensors").is_file():
            raise FileNotFoundError(f"Reranker weights are missing: {model_path}")

        self._tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
        remote_code = model_path / "remote_code"
        if remote_code.is_dir() and (remote_code / "modeling.py").is_file():
            parent = str(remote_code.parent)
            if parent not in sys.path:
                sys.path.insert(0, parent)
            configuration = importlib.import_module("remote_code.configuration")
            modeling = importlib.import_module("remote_code.modeling")
            config = configuration.NewConfig.from_pretrained(model_path, local_files_only=True)
            self._model = modeling.NewForSequenceClassification.from_pretrained(
                model_path,
                config=config,
                local_files_only=True,
            )
        else:
            self._model = AutoModelForSequenceClassification.from_pretrained(
                model_path,
                local_files_only=True,
                trust_remote_code=False,
            )
        self._model.eval()
        self._torch = torch
        self.status = RuntimeStatus(
            True,
            "READY",
            "Local cross-encoder reranker is ready.",
            self.settings.reranker_model_name,
        )

    def score(self, request: RerankRequest) -> list[float]:
        if not self.status.ready:
            raise RuntimeError(self.status.message)
        if request.provider_type is not None:
            raise ValueError("provider_type must be null for a local reranker.")
        if not request.query or not request.documents or any(not doc for doc in request.documents):
            raise ValueError("query and documents must contain non-empty strings.")
        if len(request.documents) > self.settings.reranker_max_documents:
            raise ValueError(
                f"documents exceeds maximum {self.settings.reranker_max_documents}."
            )
        if request.model_name != self.settings.reranker_model_name:
            raise ValueError(f"Unsupported reranker model: {request.model_name}")

        pairs = [(request.query, document) for document in request.documents]
        encoded = self._tokenizer(
            pairs,
            padding=True,
            truncation=True,
            max_length=self.settings.reranker_max_length,
            return_tensors="pt",
        )
        sequence_length = encoded["input_ids"].shape[1]
        position_ids = self._torch.arange(sequence_length).unsqueeze(0).expand(
            encoded["input_ids"].shape[0], -1
        )
        with self._semaphore, self._torch.inference_mode():
            logits = self._model(
                **encoded, position_ids=position_ids, return_dict=True
            ).logits.reshape(-1).float()
            scores = self._torch.sigmoid(logits)
        return scores.cpu().tolist()
