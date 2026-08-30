package com.onyx.foss.modelserver.runtime

import com.onyx.foss.modelserver.api.EmbedTextType
import com.onyx.foss.modelserver.api.ValidatedEmbedRequest
import kotlin.math.abs
import kotlin.math.sqrt
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class EmbeddingRuntimeTest {
    private val preprocessor = PrefixingTextPreprocessor()

    @Test
    fun `prefixing happens before tokenizer adapters receive text`() {
        val prepared = preprocessor.prepare(
            request(
                textType = EmbedTextType.QUERY,
                manualQueryPrefix = "search_query: ",
                manualPassagePrefix = "search_document: ",
            ),
        )

        assertEquals(listOf("search_query: Korean 질의"), prepared.texts)
    }

    @Test
    fun `passage uses only the passage prefix`() {
        val prepared = preprocessor.prepare(
            request(
                textType = EmbedTextType.PASSAGE,
                manualQueryPrefix = "search_query: ",
                manualPassagePrefix = "search_document: ",
            ),
        )

        assertEquals(listOf("search_document: Korean 질의"), prepared.texts)
    }

    @Test
    fun `normalizer creates a unit vector without model weights`() {
        val vector = L2Normalizer.normalize(listOf(3f, 4f))
        val norm = sqrt(vector.sumOf { it.toDouble() * it.toDouble() })

        assertTrue(abs(norm - 1.0) <= 1e-6)
    }

    @Test
    fun `normalizer rejects a zero vector`() {
        assertThrows<IllegalArgumentException> {
            L2Normalizer.normalize(listOf(0f, 0f))
        }
    }

    private fun request(
        textType: EmbedTextType,
        manualQueryPrefix: String?,
        manualPassagePrefix: String?,
    ) = ValidatedEmbedRequest(
        texts = listOf("Korean 질의"),
        modelName = "nomic-ai/nomic-embed-text-v1",
        maxContextLength = 512,
        normalizeEmbeddings = true,
        textType = textType,
        manualQueryPrefix = manualQueryPrefix,
        manualPassagePrefix = manualPassagePrefix,
    )
}
