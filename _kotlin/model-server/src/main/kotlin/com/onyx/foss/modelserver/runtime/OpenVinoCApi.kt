package com.onyx.foss.modelserver.runtime

import com.sun.jna.Library
import com.sun.jna.Memory
import com.sun.jna.Native
import com.sun.jna.Pointer
import com.sun.jna.Structure
import com.sun.jna.ptr.PointerByReference
import org.bytedeco.javacpp.Loader
import java.nio.file.Files
import java.nio.file.Path

/**
 * Minimal, header-verified binding for the OpenVINO 2026.2 C API.
 *
 * JavaCPP's OpenVINO preset packages the native runtime, but its generated Java
 * classes do not expose the C inference functions. This binding uses that exact
 * runtime package and the public OpenVINO C ABI rather than shelling out to
 * Python or fabricating vectors.
 */
internal interface OpenVinoC : Library {
    fun ov_core_create(core: PointerByReference): Int
    fun ov_core_free(core: Pointer)

    fun ov_core_read_model(
        core: Pointer,
        modelPath: String,
        modelBinaryPath: String,
        model: PointerByReference,
    ): Int

    fun ov_model_free(model: Pointer)

    fun ov_core_compile_model(
        core: Pointer,
        model: Pointer,
        deviceName: String,
        propertyArgsSize: Long,
        compiledModel: PointerByReference,
    ): Int

    fun ov_compiled_model_free(compiledModel: Pointer)

    fun ov_compiled_model_create_infer_request(
        compiledModel: Pointer,
        inferRequest: PointerByReference,
    ): Int

    fun ov_infer_request_free(inferRequest: Pointer)

    fun ov_tensor_create_from_host_ptr(
        elementType: Int,
        shape: OvShape.ByValue,
        hostPointer: Pointer,
        tensor: PointerByReference,
    ): Int

    fun ov_tensor_free(tensor: Pointer)

    fun ov_infer_request_set_tensor(
        inferRequest: Pointer,
        tensorName: String,
        tensor: Pointer,
    ): Int

    fun ov_infer_request_infer(inferRequest: Pointer): Int

    fun ov_infer_request_get_tensor(
        inferRequest: Pointer,
        tensorName: String,
        tensor: PointerByReference,
    ): Int

    fun ov_tensor_data(tensor: Pointer, data: PointerByReference): Int

    fun ov_get_last_err_msg(): Pointer?
    fun ov_get_error_info(status: Int): Pointer?
}

internal open class OvShape : Structure {
    @JvmField
    var rank: Long = 0

    @JvmField
    var dims: Pointer? = null

    constructor()

    constructor(rank: Long, dims: Pointer) {
        this.rank = rank
        this.dims = dims
    }

    override fun getFieldOrder(): List<String> = listOf("rank", "dims")

    class ByValue(rank: Long, dims: Pointer) : OvShape(rank, dims), Structure.ByValue
}

internal data class LoadedOpenVinoRuntime(
    val api: OpenVinoC,
    val runtimeDirectory: Path,
)

internal object OpenVinoCApiLoader {
    private const val OPENVINO_RUNTIME_RESOURCE =
        "org/bytedeco/openvino/linux-x86_64/runtime/"

    val loaded: LoadedOpenVinoRuntime by lazy(LazyThreadSafetyMode.SYNCHRONIZED) {
        require(System.getProperty("os.name").lowercase().contains("linux")) {
            "Granite OpenVINO runtime is currently packaged for Linux x86_64 only."
        }

        val extractedRuntime = Loader.cacheResources(OPENVINO_RUNTIME_RESOURCE)
            .singleOrNull()
            ?.toPath()
            ?.toAbsolutePath()
            ?.normalize()
            ?: error("JavaCPP did not extract the bundled OpenVINO Linux runtime.")
        val tbb = extractedRuntime.resolve("3rdparty/tbb/lib/libtbb.so.12")
        val core = extractedRuntime.resolve("lib/intel64/libopenvino.so.2621")
        val cApi = extractedRuntime.resolve("lib/intel64/libopenvino_c.so")
        listOf(tbb, core, cApi).forEach { path ->
            require(Files.isRegularFile(path)) {
                "Bundled OpenVINO runtime is missing " + path
            }
        }

        // libopenvino_c depends on both of these shared objects. Loading the
        // exact extracted files first makes the packaged runtime self-contained.
        System.load(tbb.toString())
        System.load(core.toString())
        LoadedOpenVinoRuntime(
            api = Native.load(cApi.toString(), OpenVinoC::class.java),
            runtimeDirectory = extractedRuntime,
        )
    }
}

internal const val OV_ELEMENT_TYPE_I64 = 10

internal fun OpenVinoC.requireSuccess(status: Int, operation: String) {
    if (status == 0) return
    val message = ov_get_last_err_msg()?.getString(0)
        ?.takeIf { it.isNotBlank() }
        ?: ov_get_error_info(status)?.getString(0)
        ?: ("OpenVINO status " + status)
    throw IllegalStateException(operation + " failed: " + message)
}

internal class OpenVinoInputTensor(
    private val api: OpenVinoC,
    values: LongArray,
    batchSize: Int,
    sequenceLength: Int,
) : AutoCloseable {
    private val dimensions = Memory(Long.SIZE_BYTES.toLong() * 2).also {
        it.setLong(0, batchSize.toLong())
        it.setLong(Long.SIZE_BYTES.toLong(), sequenceLength.toLong())
    }
    private val data = Memory(Long.SIZE_BYTES.toLong() * values.size).also {
        it.write(0, values, 0, values.size)
    }
    val pointer: Pointer = PointerByReference().also { output ->
        api.requireSuccess(
            api.ov_tensor_create_from_host_ptr(
                OV_ELEMENT_TYPE_I64,
                OvShape.ByValue(2, dimensions),
                data,
                output,
            ),
            "create input tensor",
        )
    }.value ?: error("OpenVINO did not return an input tensor.")

    override fun close() {
        api.ov_tensor_free(pointer)
    }
}
