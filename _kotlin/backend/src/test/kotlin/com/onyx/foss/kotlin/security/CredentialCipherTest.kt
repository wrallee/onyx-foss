package com.onyx.foss.kotlin.security

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.config.OnyxProperties
import org.junit.jupiter.api.Test
import org.springframework.mock.env.MockEnvironment
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class CredentialCipherTest {
    private val mapper = jacksonObjectMapper()
    private val environment = MockEnvironment().also { it.setActiveProfiles("test") }
    private val cipher = CredentialCipher(OnyxProperties(), environment, mapper)

    @Test
    fun encryptsAndDecryptsCredentialJson() {
        val secret = mapper.readTree("""{"token":"top-secret","email":"admin@example.com"}""")
        val encrypted = cipher.encrypt(secret)

        assertNotEquals(secret.toString(), encrypted)
        assertEquals(secret, cipher.decrypt(encrypted))
        assertEquals("********", cipher.masked(secret).path("token").asText())
    }
}
