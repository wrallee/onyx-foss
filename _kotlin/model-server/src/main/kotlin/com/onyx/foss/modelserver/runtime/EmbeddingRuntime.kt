package com.onyx.foss.modelserver.runtime

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer
import com.onyx.foss.modelserver.api.EmbedTextType
import com.onyx.foss.modelserver.api.ValidatedEmbedRequest
import com.sun.jna.Pointer
import com.sun.jna.ptr.PointerByReference
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component
import java.nio.file.Path
import kotlin.math.sqrt

const val MODEL_RUNTIME_INITIALIZATION_FAILED = "MODEL_RUNTIME_INITIALIZATION_FAILED"
const val MODEL_RUNTIME_UNSUPPORTED_ARTIFACT = "MODEL_RUNTIME_UNSUPPORTED_ARTIFACT"
const val MODEL_RUNTIME_MODEL_NAME_MISMATCH = "MODEL_RUNTIME_MODEL_NAME_MISMATCH"

@Component
@ConfigurationProperties(prefix = "model.runtime")
class ModelRuntimeProperties {
    /** Protects CPU inference from accidental 32K-token request amplification. */
    var maxContextLength: Int = 512
    var device: String = "CPU"
}

data class PreparedEmbeddingRequest(
    val texts: List<String>,
    val modelName: String,
    val maxContextLength: Int,
    val normalizeEmbeddings: Boolean,
    val textType: EmbedTextType,
)

@Component
class PrefixingTextPreprocessor {
    fun prepare(request: ValidatedEmbedRequest): PreparedEmbeddingRequest {
        val prefix = when (request.textType) {
            EmbedTextType.QUERY -> request.manualQueryPrefix
            EmbedTextType.PASSAGE -> request.manualPassagePrefix
        }
        return PreparedEmbeddingRequest(
            texts = request.texts.map { text -> if (prefix.isNullOrEmpty()) text else prefix + text },
            modelName = request.modelName,
            maxContextLength = request.maxContextLength,
            normalizeEmbeddings = request.normalizeEmbeddings,
            textType = request.textType,
        )
    }
}

interface TokenizerAdapter {
    fun tokenize(texts: List<String>, maxContextLength: Int): TokenizedBatch
}

data class TokenizedBatch(
    val inputIds: List<List<Long>>,
    val attentionMask: List<List<Long>>,
)

interface OnnxModelSession {
    fun infer(batch: TokenizedBatch): ModelOutputs
}

data class ModelOutputs(
    val hiddenStates: List<List<List<Float>>>,
)

interface PoolingAdapter {
    fun pool(outputs: ModelOutputs, attentionMask: List<List<Long>>): List<List<Float>>
}

interface EmbeddingRuntime {
    fun embed(request: PreparedEmbeddingRequest): List<List<Float>>
}

class OnnxEmbeddingRuntime(
    private val tokenizer: TokenizerAdapter,
    private val session: OnnxModelSession,
    private val pooling: PoolingAdapter,
) : EmbeddingRuntime {
    override fun embed(request: PreparedEmbeddingRequest): List<List<Float>> {
        val tokenized = tokenizer.tokenize(request.texts, request.maxContextLength)
        val vectors = pooling.pool(session.infer(tokenized), tokenized.attentionMask)
        require(vectors.size == request.texts.size) { "Model output count does not match request count." }
        return if (request.normalizeEmbeddings) vectors.map(L2Normalizer::normalize) else vectors
    }
}

object L2Normalizer {
    fun normalize(vector: List<Float>): List<Float> {
        val norm = sqrt(vector.sumOf { value -> value.toDouble() * value.toDouble() })
        require(norm > 0.0) { "Cannot normalize a zero embedding vector." }
        return vector.map { value -> (value / norm).toFloat() }
    }
}

data class EmbeddingRuntimeReadiness(
    val ready: Boolean,
    val code: String,
    val message: String,
    val modelName: String? = null,
    val device: String? = null,
)

@Component
class GraniteOpenVinoEmbeddingRuntime(
    private val artifactInspector: ModelArtifactInspector,
    private val properties: ModelRuntimeProperties,
) : EmbeddingRuntime {
    private val initialization = lazy(LazyThreadSafetyMode.SYNCHRONIZED) { initialize() }

    @PostConstruct
    fun initializeAtStartup() {
        initialization.value
    }

    fun readiness(): EmbeddingRuntimeReadiness = initialization.value.readiness

    override fun embed(request: PreparedEmbeddingRequest): List<List<Float>> =
        when (val state = initialization.value) {
            is RuntimeState.Ready -> state.embed(request)
            is RuntimeState.Unavailable -> throw ModelRuntimeUnavailableException(
                state.readiness.code,
                state.readiness.message,
            )
        }

    @PreDestroy
    fun close() {
        if (initialization.isInitialized()) {
            (initialization.value as? RuntimeState.Ready)?.close()
        }
    }

    private fun initialize(): RuntimeState {
        val inspection = artifactInspector.inspection()
        val manifest = inspection.manifest
            ?: return unavailable(inspection.status, inspection.message)
        val artifacts = inspection.artifacts
            ?: return unavailable(MODEL_RUNTIME_INITIALIZATION_FAILED, "Verified artifact paths are unavailable.")
        val modelBinaryPath = artifacts.modelBinaryPath
            ?: return unavailable(MODEL_RUNTIME_UNSUPPORTED_ARTIFACT, "Granite OpenVINO artifact requires a model binary.")
        if (manifest.artifact_format != "openvino" || manifest.pooling != "cls" || manifest.embedding_dimension != 768) {
            return unavailable(
                MODEL_RUNTIME_UNSUPPORTED_ARTIFACT,
                "The Granite runtime requires an OpenVINO CLS-pooled 768-dimensional artifact.",
            )
        }
        if (properties.maxContextLength <= 0) {
            return unavailable(
                MODEL_RUNTIME_INITIALIZATION_FAILED,
                "model.runtime.max-context-length must be greater than zero.",
            )
        }

        var tokenizer: HuggingFaceTokenizer? = null
        var core: Pointer? = null
        var model: Pointer? = null
        var compiledModel: Pointer? = null
        try {
            tokenizer = HuggingFaceTokenizer.builder()
                .optTokenizerPath(artifacts.tokenizerPath)                .optPadding(true)                .optTruncation(true)                .optMaxLength(minOf(properties.maxContextLength, manifest.max_context_length))                .build()
            val nativeRuntime = OpenVinoCApiLoader.loaded
            val api = nativeRuntime.api
            val reference = PointerByReference()
            api.requireSuccess(api.ov_core_create(reference), "create OpenVINO Core")
            core = reference.value ?: error("OpenVINO did not return a Core handle.")
            reference.value = null
            api.requireSuccess(
                api.ov_core_read_model(
                    core,
                    artifacts.modelPath.toString(),
                    modelBinaryPath.toString(),
                    reference,
                ),
                "read Granite OpenVINO IR",
            )
            model = reference.value ?: error("OpenVINO did not return a Model handle.")
            reference.value = null
            api.requireSuccess(
                api.ov_core_compile_model(
                    core,
                    model,
                    properties.device,
                    0,
                    reference,
                ),
                "compile Granite OpenVINO model for " + properties.device,
            )
            compiledModel = reference.value ?: error("OpenVINO did not return a CompiledModel handle.")
            api.ov_model_free(model)
            model = null
            return RuntimeState.Ready(
                api = api,
                tokenizer = tokenizer,
                core = core,
                compiledModel = compiledModel,
                manifest = manifest,
                maxContextLength = minOf(properties.maxContextLength, manifest.max_context_length),
                device = properties.device,
            )
        } catch (error: Exception) {
            compiledModel?.let { OpenVinoCApiLoader.loaded.api.ov_compiled_model_free(it) }
            model?.let { OpenVinoCApiLoader.loaded.api.ov_model_free(it) }
            core?.let { OpenVinoCApiLoader.loaded.api.ov_core_free(it) }
            tokenizer?.close()
            return unavailable(
                MODEL_RUNTIME_INITIALIZATION_FAILED,
                "Granite OpenVINO initialization failed: " + (error.message ?: error.javaClass.simpleName),
            )
        }
    }

    private fun unavailable(code: String, message: String): RuntimeState.Unavailable =
        RuntimeState.Unavailable(EmbeddingRuntimeReadiness(false, code, message))

    private sealed interface RuntimeState {
        val readiness: EmbeddingRuntimeReadiness

        class Unavailable(
            override val readiness: EmbeddingRuntimeReadiness,
        ) : RuntimeState

        class Ready(
            private val api: OpenVinoC,
            private val tokenizer: HuggingFaceTokenizer,
            private val core: Pointer,
            private val compiledModel: Pointer,
            private val manifest: ModelArtifactManifest,
            private val maxContextLength: Int,
            private val device: String,
        ) : RuntimeState, AutoCloseable {
            override val readiness = EmbeddingRuntimeReadiness(
                ready = true,
                code = "READY",
                message = "Granite OpenVINO embedding runtime is ready.",
                modelName = manifest.model_name,
                device = device,
            )

            fun embed(request: PreparedEmbeddingRequest): List<List<Float>> {
                if (request.modelName != manifest.model_name) {
                    throw ModelRuntimeUnavailableException(
                        MODEL_RUNTIME_MODEL_NAME_MISMATCH,
                        "Configured Granite model is " + manifest.model_name + ".",
                    )
                }
                val batch = tokenize(request.texts, request.maxContextLength)
                val hiddenStates = infer(batch)
                val vectors = hiddenStates.map { sequence ->
                    require(sequence.size == 768) { "Granite CLS output dimension must be 768." }
                    sequence.toList()
                }
                return if (request.normalizeEmbeddings) vectors.map(L2Normalizer::normalize) else vectors
            }

            private fun tokenize(texts: List<String>, requestedContextLength: Int): InferenceBatch = synchronized(tokenizer) {
                val effectiveMaxLength = minOf(requestedContextLength, maxContextLength)
                val encodings = tokenizer.batchEncode(texts.toTypedArray())
                val sequenceLength = encodings.maxOf { encoding ->
                    minOf(encoding.ids.size, effectiveMaxLength)
                }
                require(sequenceLength > 0) { "Granite tokenizer produced an empty sequence." }
                val inputIds = LongArray(texts.size * sequenceLength)
                val attentionMask = LongArray(texts.size * sequenceLength)
                encodings.forEachIndexed { batchIndex, encoding ->
                    val encodedIds = encoding.ids
                    val encodedMask = encoding.attentionMask
                    val copyLength = minOf(encodedIds.size, sequenceLength)
                    val offset = batchIndex * sequenceLength
                    encodedIds.copyInto(inputIds, offset, 0, copyLength)
                    encodedMask.copyInto(attentionMask, offset, 0, copyLength)
                }
                InferenceBatch(texts.size, sequenceLength, inputIds, attentionMask)
            }

            private fun infer(batch: InferenceBatch): List<FloatArray> {
                val requestReference = PointerByReference()
                api.requireSuccess(
                    api.ov_compiled_model_create_infer_request(compiledModel, requestReference),
                    "create OpenVINO infer request",
                )
                val inferRequest = requestReference.value ?: error("OpenVINO did not return an infer request.")
                try {
                    OpenVinoInputTensor(api, batch.inputIds, batch.batchSize, batch.sequenceLength).use { ids ->
                        OpenVinoInputTensor(api, batch.attentionMask, batch.batchSize, batch.sequenceLength).use { mask ->
                            api.requireSuccess(
                                api.ov_infer_request_set_tensor(inferRequest, "input_ids", ids.pointer),
                                "set input_ids",
                            )
                            api.requireSuccess(
                                api.ov_infer_request_set_tensor(inferRequest, "attention_mask", mask.pointer),
                                "set attention_mask",
                            )
                            api.requireSuccess(api.ov_infer_request_infer(inferRequest), "infer Granite embeddings")
                            val outputReference = PointerByReference()
                            api.requireSuccess(
                                api.ov_infer_request_get_tensor(inferRequest, "last_hidden_state", outputReference),
                                "get last_hidden_state",
                            )
                            val outputTensor = outputReference.value
                                ?: error("OpenVINO did not return last_hidden_state.")
                            try {
                                val dataReference = PointerByReference()
                                api.requireSuccess(api.ov_tensor_data(outputTensor, dataReference), "read last_hidden_state")
                                val output = dataReference.value
                                    ?: error("OpenVINO did not return output data.")
                                val expectedValues = batch.batchSize * batch.sequenceLength * 768
                                val values = output.getFloatArray(0, expectedValues)
                                return List(batch.batchSize) { batchIndex ->
                                    val start = batchIndex * batch.sequenceLength * 768
                                    values.copyOfRange(start, start + 768)
                                }
                            } finally {
                                api.ov_tensor_free(outputTensor)
                            }
                        }
                    }
                } finally {
                    api.ov_infer_request_free(inferRequest)
                }
            }

            override fun close() {
                tokenizer.close()
                api.ov_compiled_model_free(compiledModel)
                api.ov_core_free(core)
            }
        }
    }
}

private data class InferenceBatch(
    val batchSize: Int,
    val sequenceLength: Int,
    val inputIds: LongArray,
    val attentionMask: LongArray,
)

class ModelRuntimeUnavailableException(
    val code: String,
    message: String,
) : RuntimeException(message)
