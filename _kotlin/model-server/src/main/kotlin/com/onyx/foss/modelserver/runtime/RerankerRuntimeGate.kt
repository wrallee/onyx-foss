package com.onyx.foss.modelserver.runtime

import com.onyx.foss.modelserver.api.RerankRequest
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

const val RERANKER_EXPORT_NOT_CONFIGURED = "RERANKER_EXPORT_NOT_CONFIGURED"

@Component
class RerankerRuntimeGate(
    @Value("\${model.reranker.candidate:Alibaba-NLP/gte-multilingual-reranker-base}")
    private val configuredCandidate: String,
) {
    fun score(request: RerankRequest): List<Float> {
        val requestedModel = request.modelName!!
        if (requestedModel !in SUPPORTED_CANDIDATES) {
            throw ModelRuntimeUnavailableException(
                RERANKER_EXPORT_NOT_CONFIGURED,
                "Unsupported local reranker candidate: $requestedModel",
            )
        }
        throw ModelRuntimeUnavailableException(
            RERANKER_EXPORT_NOT_CONFIGURED,
            "$requestedModel is available only as a safetensors artifact. Supply a pinned ONNX or OpenVINO export and golden score report before enabling reranking. Configured candidate: $configuredCandidate",
        )
    }

    fun readinessMessage(): String =
        "$configuredCandidate has no validated ONNX or OpenVINO export. Reranking is intentionally disabled."

    private companion object {
        val SUPPORTED_CANDIDATES = setOf(
            "Alibaba-NLP/gte-multilingual-reranker-base",
            "BAAI/bge-reranker-v2-m3",
        )
    }
}
