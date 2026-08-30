package com.onyx.foss.modelserver.runtime

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import kotlin.io.path.writeText
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.io.TempDir

class ModelArtifactInspectorTest {
    @TempDir
    lateinit var temporaryDirectory: Path

    @Test
    fun `missing manifest fails with an explicit artifact error`() {
        val inspector = ModelArtifactInspector(jacksonObjectMapper(), "")

        assertEquals(MODEL_ARTIFACT_NOT_CONFIGURED, inspector.inspection().status)
        assertEquals("MODEL_ARTIFACT_MANIFEST is not set.", inspector.inspection().message)
    }

    @Test
    fun `missing artifact file never advances to a runtime`() {
        val manifest = temporaryDirectory.resolve("manifest.json")
        manifest.writeText(validManifest(modelPath = "missing-model.onnx", tokenizerPath = "missing-tokenizer.json"))

        val inspection = ModelArtifactInspector(jacksonObjectMapper(), manifest.toString()).inspection()

        assertEquals(MODEL_ARTIFACT_NOT_CONFIGURED, inspection.status)
        assertEquals(true, inspection.message.startsWith("ONNX model artifact does not exist:"))
    }

    @Test
    fun `verified artifact is held until a real runtime adapter exists`() {
        val model = temporaryDirectory.resolve("model.onnx")
        val tokenizer = temporaryDirectory.resolve("tokenizer.json")
        Files.writeString(model, "non-proprietary-test-onnx-placeholder")
        Files.writeString(tokenizer, "non-proprietary-test-tokenizer-placeholder")
        val manifest = temporaryDirectory.resolve("manifest.json")
        manifest.writeText(
            validManifest(
                modelPath = model.fileName.toString(),
                tokenizerPath = tokenizer.fileName.toString(),
                modelHash = sha256(model),
                tokenizerHash = sha256(tokenizer),
            ),
        )

        val inspection = ModelArtifactInspector(jacksonObjectMapper(), manifest.toString()).inspection()

        assertEquals(MODEL_ARTIFACT_VERIFIED, inspection.status)
        assertEquals("nomic-ai/nomic-embed-text-v1", inspection.manifest?.model_name)
    }

    @Test
    fun `local Granite OpenVINO manifest verifies when model artifacts are mounted`() {
        val manifest = Path.of("granite-openvino-int8.manifest.json").toAbsolutePath()
        assumeTrue(Files.isRegularFile(manifest), "Granite manifest is not available from this test working directory.")
        val model = manifest.parent.resolve("../models/granite-embedding-311m-multilingual-r2-int8-openvino/openvino/openvino_model_qint8_quantized.xml").normalize()
        assumeTrue(Files.isRegularFile(model), "Granite model artifacts are not mounted for this test.")

        val inspection = ModelArtifactInspector(jacksonObjectMapper(), manifest.toString()).inspection()

        assertEquals(MODEL_ARTIFACT_VERIFIED, inspection.status)
        assertEquals("ibm-granite/granite-embedding-311m-multilingual-r2", inspection.manifest?.model_name)
        assertEquals("openvino", inspection.manifest?.artifact_format)
    }

    private fun validManifest(
        modelPath: String,
        tokenizerPath: String,
        modelHash: String = "0".repeat(64),
        tokenizerHash: String = "1".repeat(64),
    ) =
        """
        {
          "schema_version": 1,
          "model_name": "nomic-ai/nomic-embed-text-v1",
          "artifact_format": "onnx",
          "onnx_model_path": "$modelPath",
          "tokenizer_path": "$tokenizerPath",
          "pooling": "mean",
          "embedding_dimension": 768,
          "max_context_length": 512,
          "model_sha256": "$modelHash",
          "tokenizer_sha256": "$tokenizerHash",
          "source_commit": "60b2c0c3616bba8bd56c1c8ce02d320c79b0b06f",
          "license": "approved-test-license"
        }
        """.trimIndent()

    private fun sha256(path: Path): String =
        MessageDigest.getInstance("SHA-256")
            .digest(Files.readAllBytes(path))
            .joinToString("") { "%02x".format(it) }
}
