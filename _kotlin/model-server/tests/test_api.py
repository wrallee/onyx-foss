from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app, embedding_runtime, reranker_runtime
from app.runtime import RuntimeStatus


@pytest.fixture
def client(monkeypatch) -> TestClient:
    def load_embedding() -> None:
        embedding_runtime.status = RuntimeStatus(
            True,
            "READY",
            "test embedding ready",
            embedding_runtime.settings.embedding_model_name,
        )

    def load_reranker() -> None:
        reranker_runtime.status = RuntimeStatus(
            True,
            "READY",
            "test reranker ready",
            reranker_runtime.settings.reranker_model_name,
        )

    monkeypatch.setattr(embedding_runtime, "load", load_embedding)
    monkeypatch.setattr(reranker_runtime, "load", load_reranker)
    with TestClient(app) as test_client:
        yield test_client


def test_health_contract(client: TestClient) -> None:
    response = client.get("/api/health")
    assert response.status_code == 200


def test_embed_contract_with_fake_runtime(monkeypatch, client: TestClient) -> None:
    monkeypatch.setattr(
        embedding_runtime,
        "embed",
        lambda request: [[1.0, 0.0] for _ in request.texts],
    )
    response = client.post(
        "/encoder/bi-encoder-embed",
        json={
            "texts": ["hello", "안녕하세요"],
            "model_name": embedding_runtime.settings.embedding_model_name,
            "max_context_length": 512,
            "normalize_embeddings": True,
            "text_type": "passage",
        },
    )
    assert response.status_code == 200
    assert response.json() == {"embeddings": [[1.0, 0.0], [1.0, 0.0]]}


def test_rerank_contract_with_fake_runtime(monkeypatch, client: TestClient) -> None:
    monkeypatch.setattr(reranker_runtime, "score", lambda request: [0.1, 0.9])
    response = client.post(
        "/encoder/cross-encoder-scores",
        json={
            "query": "query",
            "documents": ["first", "second"],
            "model_name": reranker_runtime.settings.reranker_model_name,
        },
    )
    assert response.status_code == 200
    assert response.json() == {"scores": [0.1, 0.9]}
