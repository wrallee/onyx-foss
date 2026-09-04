package com.onyx.foss.kotlin.security

import tools.jackson.databind.JsonNode
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.node.ObjectNode
import com.onyx.foss.kotlin.config.OnyxProperties
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

@Component
class CredentialCipher(
    private val properties: OnyxProperties,
    private val environment: Environment,
    private val objectMapper: ObjectMapper,
) {
    private val key: SecretKeySpec by lazy { SecretKeySpec(resolveKey(), "AES") }

    fun encrypt(value: JsonNode): String {
        val nonce = ByteArray(NONCE_SIZE).also(SecureRandom()::nextBytes)
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(Cipher.ENCRYPT_MODE, key, GCMParameterSpec(TAG_BITS, nonce))
        val encrypted = cipher.doFinal(objectMapper.writeValueAsBytes(value))
        return Base64.getEncoder().encodeToString(nonce) + ":" + Base64.getEncoder().encodeToString(encrypted)
    }

    fun decrypt(value: String): JsonNode {
        val parts = value.split(":", limit = 2)
        require(parts.size == 2) { "Stored credential has an invalid encryption format" }
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(
            Cipher.DECRYPT_MODE,
            key,
            GCMParameterSpec(TAG_BITS, Base64.getDecoder().decode(parts[0])),
        )
        return objectMapper.readTree(cipher.doFinal(Base64.getDecoder().decode(parts[1])))
    }

    fun masked(value: JsonNode): JsonNode = when {
        value.isObject -> (value.deepCopy() as ObjectNode).also { node ->
            node.properties().forEach { (name, child) -> node.set(name, masked(child)) }
        }
        value.isArray -> value.deepCopy()
        value.isNull -> value
        else -> objectMapper.nodeFactory.stringNode("********")
    }

    private fun resolveKey(): ByteArray {
        val configured = properties.crypto.key.trim()
        if (configured.isNotBlank()) {
            val decoded = Base64.getDecoder().decode(configured)
            require(decoded.size == 32) { "ONYX_CREDENTIAL_ENCRYPTION_KEY must decode to 32 bytes" }
            return decoded
        }
        if (environment.activeProfiles.any { it == "dev" || it == "test" }) {
            return MessageDigest.getInstance("SHA-256").digest("onyx-kotlin-dev-only".toByteArray())
        }
        error("ONYX_CREDENTIAL_ENCRYPTION_KEY is required outside the dev and test profiles")
    }

    private companion object {
        const val TRANSFORMATION = "AES/GCM/NoPadding"
        const val NONCE_SIZE = 12
        const val TAG_BITS = 128
    }
}
