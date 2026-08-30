package com.onyx.foss.modelserver.api

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

@SpringBootTest(properties = ["model.artifact.manifest="])
@AutoConfigureMockMvc
class ModelServerControllerTest(
    @Autowired private val mockMvc: MockMvc,
) {
    @Test
    fun `liveness remains available without a model artifact`() {
        mockMvc.perform(get("/api/health"))
            .andExpect(status().isOk)
            .andExpect(content().string(""))
    }

    @Test
    fun `readiness exposes a missing-artifact failure`() {
        mockMvc.perform(get("/actuator/health/readiness"))
            .andExpect(status().isServiceUnavailable)
            .andExpect(jsonPath("$.status").value("DOWN"))
            .andExpect(jsonPath("$.components.modelArtifact.details.code").value("MODEL_ARTIFACT_NOT_CONFIGURED"))
    }

    @Test
    fun `embedding request never fabricates vectors without an artifact`() {
        mockMvc.perform(
            post("/encoder/bi-encoder-embed")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    """
                    {
                      "texts": ["hello"],
                      "model_name": "nomic-ai/nomic-embed-text-v1",
                      "max_context_length": 512,
                      "normalize_embeddings": true,
                      "text_type": "query"
                    }
                    """.trimIndent(),
                ),
        )
            .andExpect(status().isServiceUnavailable)
            .andExpect(jsonPath("$.code").value("MODEL_ARTIFACT_NOT_CONFIGURED"))
            .andExpect(jsonPath("$.message").value("MODEL_ARTIFACT_MANIFEST is not set."))
    }

    @Test
    fun `empty text list is rejected before the runtime`() {
        mockMvc.perform(
            post("/encoder/bi-encoder-embed")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    """
                    {
                      "texts": [],
                      "model_name": "nomic-ai/nomic-embed-text-v1",
                      "max_context_length": 512,
                      "normalize_embeddings": true,
                      "text_type": "query"
                    }
                    """.trimIndent(),
                ),
        )
            .andExpect(status().isBadRequest)
            .andExpect(jsonPath("$.code").value("INVALID_EMBED_REQUEST"))
            .andExpect(jsonPath("$.message").value("No texts to be embedded"))
    }

    @Test
    fun `reranking is gated until a selected candidate has an approved export`() {
        mockMvc.perform(
            post("/encoder/cross-encoder-scores")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    """
                    {
                      "query": "What is Kotlin?",
                      "documents": ["Kotlin is a JVM language."],
                      "model_name": "Alibaba-NLP/gte-multilingual-reranker-base"
                    }
                    """.trimIndent(),
                ),
        )
            .andExpect(status().isServiceUnavailable)
            .andExpect(jsonPath("$.code").value("RERANKER_EXPORT_NOT_CONFIGURED"))
    }

    @Test
    fun `gpu status and metrics preserve operational endpoints`() {
        mockMvc.perform(get("/api/gpu-status"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.gpu_available").value(false))
            .andExpect(jsonPath("$.type").value("NONE"))

        mockMvc.perform(get("/metrics"))
            .andExpect(status().isOk)
            .andExpect(content().contentTypeCompatibleWith(MediaType.TEXT_PLAIN))
            .andExpect(content().string(org.hamcrest.Matchers.containsString("onyx_model_server_ready 0")))
    }
}
