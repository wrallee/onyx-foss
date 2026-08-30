package com.onyx.foss.modelserver.runtime

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

const val MODEL_ARTIFACT_NOT_CONFIGURED = "MODEL_ARTIFACT_NOT_CONFIGURED"
const val MODEL_ARTIFACT_VERIFIED = "MODEL_ARTIFACT_VERIFIED"

data class ModelArtifactManifest(
    val schema_version: Int,
    val model_name: String,
    val artifact_format: String,
    val onnx_model_path: String,
    val model_binary_path: String? = null,
    val tokenizer_path: String,
    val pooling: String,
    val embedding_dimension: Int,
    val max_context_length: Int,
    val model_sha256: String,
    val model_binary_sha256: String? = null,
    val tokenizer_sha256: String,
    val source_commit: String,
    val license: String,
)

data class VerifiedModelArtifacts(
    val modelPath: Path,
    val modelBinaryPath: Path?,
    val tokenizerPath: Path,
)

data class ArtifactInspection(
    val status: String,
    val message: String,
    val manifest: ModelArtifactManifest? = null,
    val artifacts: VerifiedModelArtifacts? = null,
)

@Component
class ModelArtifactInspector(
    private val objectMapper: ObjectMapper,
    @Value("\${model.artifact.manifest:}") private val configuredManifestPath: String,
) {
    private val cachedInspection: ArtifactInspection by lazy(::inspect)

    fun inspection(): ArtifactInspection = cachedInspection

    private fun inspect(): ArtifactInspection {
        val configuredPath = configuredManifestPath.trim()
        if (configuredPath.isEmpty()) {
            return notConfigured("MODEL_ARTIFACT_MANIFEST is not set.")
        }

        val manifestPath = runCatching { Path.of(configuredPath).toAbsolutePath().normalize() }
            .getOrElse { return notConfigured("MODEL_ARTIFACT_MANIFEST is not a valid path.") }
        if (!Files.isRegularFile(manifestPath)) {
            return notConfigured("Model artifact manifest does not exist: " + manifestPath)
        }

        val manifest = runCatching {
            Files.newBufferedReader(manifestPath).use { reader ->
                objectMapper.readValue(reader, ModelArtifactManifest::class.java)
            }
        }.getOrElse {
            return notConfigured("Model artifact manifest is invalid: " + (it.message ?: it.javaClass.simpleName))
        }

        return validateManifest(manifest, manifestPath.parent)
            .fold(
                onSuccess = { artifacts ->
                    ArtifactInspection(
                        status = MODEL_ARTIFACT_VERIFIED,
                        message = "Model artifact hashes verified.",
                        manifest = manifest,
                        artifacts = artifacts,
                    )
                },
                onFailure = { error -> notConfigured(error.message ?: "Model artifact validation failed.") },
            )
    }

    private fun validateManifest(
        manifest: ModelArtifactManifest,
        manifestDirectory: Path,
    ): Result<VerifiedModelArtifacts> = runCatching {
        require(manifest.schema_version == 1) { "Unsupported manifest schema_version: " + manifest.schema_version }
        require(manifest.model_name.isNotBlank()) { "Manifest model_name must not be blank." }
        require(manifest.artifact_format in setOf("onnx", "openvino")) {
            "Manifest artifact_format must be onnx or openvino."
        }
        require(manifest.pooling in setOf("mean", "cls", "last_token")) { "Unsupported pooling: " + manifest.pooling }
        require(manifest.embedding_dimension > 0) { "Manifest embedding_dimension must be greater than zero." }
        require(manifest.max_context_length > 0) { "Manifest max_context_length must be greater than zero." }
        require(manifest.source_commit.isNotBlank()) { "Manifest source_commit must not be blank." }
        require(manifest.license.isNotBlank()) { "Manifest license must not be blank." }
        require(SHA_256.matches(manifest.model_sha256)) {
            "Manifest model_sha256 must be 64 lowercase hexadecimal characters."
        }
        require(SHA_256.matches(manifest.tokenizer_sha256)) {
            "Manifest tokenizer_sha256 must be 64 lowercase hexadecimal characters."
        }

        val modelPath = resolveArtifactPath(manifestDirectory, manifest.onnx_model_path)
            ?: error("Manifest onnx_model_path is invalid.")
        val tokenizerPath = resolveArtifactPath(manifestDirectory, manifest.tokenizer_path)
            ?: error("Manifest tokenizer_path is invalid.")
        require(Files.isRegularFile(modelPath)) { "ONNX model artifact does not exist: " + modelPath }
        require(Files.isRegularFile(tokenizerPath)) { "Tokenizer artifact does not exist: " + tokenizerPath }
        require(sha256(modelPath) == manifest.model_sha256) {
            "ONNX model artifact SHA-256 does not match manifest."
        }
        require(sha256(tokenizerPath) == manifest.tokenizer_sha256) {
            "Tokenizer artifact SHA-256 does not match manifest."
        }

        val binaryPath = if (manifest.artifact_format == "openvino") {
            val configuredBinaryPath = manifest.model_binary_path
                ?.let { resolveArtifactPath(manifestDirectory, it) }
                ?: error("OpenVINO artifact requires model_binary_path.")
            val binaryHash = manifest.model_binary_sha256
                ?: error("OpenVINO artifact requires model_binary_sha256.")
            require(SHA_256.matches(binaryHash)) {
                "Manifest model_binary_sha256 must be 64 lowercase hexadecimal characters."
            }
            require(Files.isRegularFile(configuredBinaryPath)) {
                "OpenVINO model binary does not exist: " + configuredBinaryPath
            }
            require(sha256(configuredBinaryPath) == binaryHash) {
                "OpenVINO model binary SHA-256 does not match manifest."
            }
            configuredBinaryPath
        } else {
            null
        }
        VerifiedModelArtifacts(modelPath, binaryPath, tokenizerPath)
    }

    private fun resolveArtifactPath(manifestDirectory: Path, configuredPath: String): Path? =
        runCatching {
            val path = Path.of(configuredPath)
            (if (path.isAbsolute) path else manifestDirectory.resolve(path)).normalize()
        }.getOrNull()

    private fun sha256(path: Path): String {
        val digest = MessageDigest.getInstance("SHA-256")
        Files.newInputStream(path).use { input ->
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            while (true) {
                val read = input.read(buffer)
                if (read < 0) break
                digest.update(buffer, 0, read)
            }
        }
        return digest.digest().joinToString("") { "%02x".format(it) }
    }

    private fun notConfigured(message: String) = ArtifactInspection(
        status = MODEL_ARTIFACT_NOT_CONFIGURED,
        message = message,
    )

    private companion object {
        val SHA_256 = Regex("[a-f0-9]{64}")
    }
}
