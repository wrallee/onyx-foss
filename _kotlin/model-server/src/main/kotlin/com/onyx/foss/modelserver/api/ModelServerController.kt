package com.onyx.foss.modelserver.api

import com.onyx.foss.modelserver.runtime.GraniteOpenVinoEmbeddingRuntime
import com.onyx.foss.modelserver.runtime.ModelRuntimeUnavailableException
import com.onyx.foss.modelserver.runtime.PrefixingTextPreprocessor
import com.onyx.foss.modelserver.runtime.RERANKER_EXPORT_NOT_CONFIGURED
import com.onyx.foss.modelserver.runtime.RerankerRuntimeGate
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestController
class ModelServerController(
    private val requestValidator: EmbeddingRequestValidator,
    private val preprocessor: PrefixingTextPreprocessor,
    private val embeddingRuntime: GraniteOpenVinoEmbeddingRuntime,
    private val rerankerRuntime: RerankerRuntimeGate,
) {
    @GetMapping("/api/health")
    fun liveness(): ResponseEntity<Void> = ResponseEntity.ok().build()

    @GetMapping("/api/gpu-status")
    fun gpuStatus(): GpuStatusResponse = GpuStatusResponse(gpuAvailable = false, type = "NONE")

    @GetMapping("/api/model-status")
    fun modelStatus(): ModelServerStatusResponse = ModelServerStatusResponse(
        embedding = embeddingRuntime.readiness(),
        reranker = RerankerStatusResponse(
            ready = false,
            code = RERANKER_EXPORT_NOT_CONFIGURED,
            message = rerankerRuntime.readinessMessage(),
        ),
    )

    @PostMapping(
        "/encoder/bi-encoder-embed",
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    fun embed(@RequestBody request: EmbedRequest): EmbedResponse {
        val validatedRequest = requestValidator.validate(request)
        val preparedRequest = preprocessor.prepare(validatedRequest)
        return EmbedResponse(embeddingRuntime.embed(preparedRequest))
    }

    @PostMapping(
        "/encoder/cross-encoder-scores",
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    fun rerank(@RequestBody request: RerankRequest): RerankResponse {
        validateRerankRequest(request)
        return RerankResponse(rerankerRuntime.score(request))
    }

    @GetMapping("/metrics", produces = [MediaType.TEXT_PLAIN_VALUE])
    fun metrics(): ResponseEntity<String> {
        val ready = if (embeddingRuntime.readiness().ready) 1 else 0
        val body = "# HELP onyx_model_server_ready 1 when the model runtime can serve embeddings.\n" +
            "# TYPE onyx_model_server_ready gauge\n" +
            "onyx_model_server_ready " + ready + "\n"
        return ResponseEntity.ok()
            .contentType(MediaType.parseMediaType("text/plain; version=0.0.4; charset=utf-8"))
            .body(body)
    }

    private fun validateRerankRequest(request: RerankRequest) {
        if (request.query.isNullOrEmpty()) throw RequestValidationException("query must not be empty.")
        if (request.documents.isEmpty() || request.documents.any { it.isEmpty() }) {
            throw RequestValidationException("documents must contain only non-empty strings.")
        }
        if (request.modelName.isNullOrBlank()) throw RequestValidationException("model_name must be provided.")
        if (!request.providerType.isNullOrBlank()) {
            throw RequestValidationException("Model server reranking endpoint only accepts local models; provider_type must be null.")
        }
    }
}

@RestControllerAdvice
class ModelServerExceptionHandler {
    @ExceptionHandler(RequestValidationException::class)
    fun invalidRequest(exception: RequestValidationException): ResponseEntity<ApiError> =
        ResponseEntity.badRequest().body(ApiError(code = "INVALID_EMBED_REQUEST", message = exception.message!!))

    @ExceptionHandler(ModelRuntimeUnavailableException::class)
    fun unavailable(exception: ModelRuntimeUnavailableException): ResponseEntity<ApiError> =
        ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(ApiError(code = exception.code, message = exception.message!!))
}
