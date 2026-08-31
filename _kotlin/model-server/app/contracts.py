from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class EmbedTextType(str, Enum):
    QUERY = "query"
    PASSAGE = "passage"


class EmbedRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    texts: list[str]
    model_name: str | None = None
    deployment_name: str | None = None
    max_context_length: int = Field(default=512, ge=1, le=32768)
    normalize_embeddings: bool = True
    api_key: str | None = None
    provider_type: str | None = None
    text_type: EmbedTextType
    manual_query_prefix: str | None = None
    manual_passage_prefix: str | None = None
    api_url: str | None = None
    api_version: str | None = None
    reduced_dimension: int | None = None


class EmbedResponse(BaseModel):
    embeddings: list[list[float]]


class RerankRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    query: str
    documents: list[str]
    model_name: str
    provider_type: str | None = None
    api_key: str | None = None
    api_url: str | None = None


class RerankResponse(BaseModel):
    scores: list[float]


class ApiError(BaseModel):
    code: str
    message: str
