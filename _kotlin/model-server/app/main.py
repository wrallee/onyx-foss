from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import logging
import time
from typing import Any

from fastapi import FastAPI, HTTPException, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from app.config import Settings
from app.contracts import ApiError, EmbedRequest, EmbedResponse
from app.runtime import EmbeddingRuntime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("onyx-python-model-server")

REQUESTS = Counter(
    "onyx_model_server_requests_total",
    "Model server requests",
    ("operation", "status"),
)
LATENCY = Histogram(
    "onyx_model_server_request_seconds",
    "Model server request latency",
    ("operation",),
)
READY = Gauge(
    "onyx_model_server_ready",
    "Model readiness",
    ("model",),
)

settings = Settings.from_environment()
embedding_runtime = EmbeddingRuntime(settings)
startup_errors: dict[str, str] = {}


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        await run_in_threadpool(embedding_runtime.load)
        READY.labels("embedding").set(1)
        logger.info("embedding runtime ready: %s", embedding_runtime.status.model_name)
    except Exception as error:
        startup_errors["embedding"] = str(error)
        READY.labels("embedding").set(0)
        logger.exception("Failed to load embedding runtime")
    yield


app = FastAPI(title="Onyx Python Model Server", version="1.0.0", lifespan=lifespan)


@app.exception_handler(ValueError)
async def invalid_request(_: Any, error: ValueError):
    return JSONResponse(
        status_code=400,
        content=ApiError(code="INVALID_REQUEST", message=str(error)).model_dump(),
    )


@app.exception_handler(RuntimeError)
async def runtime_unavailable(_: Any, error: RuntimeError):
    return JSONResponse(
        status_code=503,
        content=ApiError(code="MODEL_RUNTIME_UNAVAILABLE", message=str(error)).model_dump(),
    )


@app.get("/api/health", status_code=200)
def health() -> None:
    return None


@app.get("/api/gpu-status")
def gpu_status() -> dict[str, Any]:
    return {"gpu_available": False, "type": "NONE"}


@app.get("/api/model-status")
def model_status() -> dict[str, Any]:
    return {
        "embedding": embedding_runtime.status.__dict__,
        "startup_errors": startup_errors,
    }


@app.get("/actuator/health/readiness")
def readiness() -> JSONResponse:
    ready = embedding_runtime.status.ready
    return JSONResponse(
        status_code=200 if ready else 503,
        content={
            "status": "UP" if ready else "DOWN",
            "components": {
                "embedding": embedding_runtime.status.__dict__,
            },
        },
    )


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/encoder/bi-encoder-embed", response_model=EmbedResponse)
async def embed(request: EmbedRequest) -> EmbedResponse:
    started = time.perf_counter()
    try:
        embeddings = await run_in_threadpool(embedding_runtime.embed, request)
        REQUESTS.labels("embed", "success").inc()
        return EmbedResponse(embeddings=embeddings)
    except Exception:
        REQUESTS.labels("embed", "error").inc()
        raise
    finally:
        LATENCY.labels("embed").observe(time.perf_counter() - started)
