package com.onyx.foss.modelserver.api

import org.springframework.stereotype.Component

@Component
class EmbeddingRequestValidator {
    fun validate(request: EmbedRequest): ValidatedEmbedRequest {
        if (!request.providerType.isNullOrBlank()) {
            throw RequestValidationException(
                "Model server embedding endpoint only accepts local models; provider_type must be null.",
            )
        }
        if (request.texts.isEmpty()) {
            throw RequestValidationException("No texts to be embedded")
        }
        if (request.texts.any { it.isEmpty() }) {
            throw RequestValidationException("Empty strings are not allowed for embedding.")
        }
        val modelName = request.modelName?.takeIf { it.isNotBlank() }
            ?: throw RequestValidationException("Model name must be provided to run embeddings.")
        val maxContextLength = request.maxContextLength?.takeIf { it > 0 }
            ?: throw RequestValidationException("max_context_length must be greater than zero.")
        val normalizeEmbeddings = request.normalizeEmbeddings
            ?: throw RequestValidationException("normalize_embeddings must be provided.")
        val textType = request.textType
            ?: throw RequestValidationException("text_type must be query or passage.")

        return ValidatedEmbedRequest(
            texts = request.texts,
            modelName = modelName,
            maxContextLength = maxContextLength,
            normalizeEmbeddings = normalizeEmbeddings,
            textType = textType,
            manualQueryPrefix = request.manualQueryPrefix,
            manualPassagePrefix = request.manualPassagePrefix,
        )
    }
}

class RequestValidationException(message: String) : RuntimeException(message)
