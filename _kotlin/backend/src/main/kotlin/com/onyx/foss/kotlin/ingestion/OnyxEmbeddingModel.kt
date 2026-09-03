package com.onyx.foss.kotlin.ingestion

import com.onyx.foss.kotlin.config.OnyxProperties
import org.springframework.ai.document.Document
import org.springframework.ai.embedding.AbstractEmbeddingModel
import org.springframework.ai.embedding.Embedding
import org.springframework.ai.embedding.EmbeddingRequest
import org.springframework.ai.embedding.EmbeddingResponse
import org.springframework.stereotype.Component

@Component
class OnyxEmbeddingModel(
    private val modelServerClient: ModelServerClient,
    private val properties: OnyxProperties,
) : AbstractEmbeddingModel() {
    override fun dimensions(): Int = properties.modelServer.embeddingDimension

    override fun call(request: EmbeddingRequest): EmbeddingResponse {
        val vectors = modelServerClient.embed(request.instructions)
        val embeddings = vectors.mapIndexed { index, vector ->
            Embedding(vector.map { it.toFloat() }.toFloatArray(), index)
        }
        return EmbeddingResponse(embeddings)
    }

    override fun embed(document: Document): FloatArray {
        val vectors = modelServerClient.embed(listOf(document.text ?: ""))
        return vectors.firstOrNull()?.map { it.toFloat() }?.toFloatArray() ?: FloatArray(0)
    }
}
