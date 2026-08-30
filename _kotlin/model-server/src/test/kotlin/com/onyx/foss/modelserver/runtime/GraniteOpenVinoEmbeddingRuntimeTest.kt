package com.onyx.foss.modelserver.runtime

import com.onyx.foss.modelserver.api.EmbedTextType
import com.onyx.foss.modelserver.api.ValidatedEmbedRequest
import kotlin.math.abs
import kotlin.math.sqrt
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "model.artifact.manifest=granite-openvino-int8.manifest.json",
        "model.runtime.max-context-length=64",
    ],
)
class GraniteOpenVinoEmbeddingRuntimeTest(
    @Autowired private val runtime: GraniteOpenVinoEmbeddingRuntime,
    @Autowired private val preprocessor: PrefixingTextPreprocessor,
) {
    @Test
    fun realGraniteOpenVinoEmitsNormalizedEnglishAndKoreanEmbeddings() {
        assertTrue(runtime.readiness().ready, runtime.readiness().message)
        val vectors = runtime.embed(
            PreparedEmbeddingRequest(
                texts = listOf("A Kotlin service indexes documents.", "한국어 문서 검색을 위한 임베딩 테스트입니다."),
                modelName = "ibm-granite/granite-embedding-311m-multilingual-r2",
                maxContextLength = 64,
                normalizeEmbeddings = true,
                textType = EmbedTextType.PASSAGE,
            ),
        )

        assertEquals(2, vectors.size)
        vectors.forEach { vector ->
            assertEquals(768, vector.size)
            assertTrue(vector.all { it.isFinite() })
            assertTrue(abs(norm(vector) - 1.0) < 0.0001)
        }
    }

    @Test
    fun realGraniteRuntimeIsDeterministicAndReceivesPreparedPrefixes() {
        val plain = request("Kotlin document search")
        val once = runtime.embed(plain).single()
        val twice = runtime.embed(plain).single()
        assertTrue(cosine(once, twice) > 0.999999)

        val prefixed = preprocessor.prepare(
            ValidatedEmbedRequest(
                texts = listOf("Kotlin document search"),
                modelName = plain.modelName,
                maxContextLength = plain.maxContextLength,
                normalizeEmbeddings = true,
                textType = EmbedTextType.QUERY,
                manualQueryPrefix = "search_query: ",
                manualPassagePrefix = null,
            ),
        )
        assertEquals("search_query: Kotlin document search", prefixed.texts.single())
        val prefixedVector = runtime.embed(prefixed).single()
        assertTrue(cosine(once, prefixedVector) < 0.999999)
    }

    private fun request(text: String) = PreparedEmbeddingRequest(
        texts = listOf(text),
        modelName = "ibm-granite/granite-embedding-311m-multilingual-r2",
        maxContextLength = 64,
        normalizeEmbeddings = true,
        textType = EmbedTextType.QUERY,
    )

    private fun norm(vector: List<Float>): Double = sqrt(vector.sumOf { it.toDouble() * it.toDouble() })

    private fun cosine(left: List<Float>, right: List<Float>): Double =
        left.indices.sumOf { index -> left[index].toDouble() * right[index].toDouble() } /
            (norm(left) * norm(right))
}
