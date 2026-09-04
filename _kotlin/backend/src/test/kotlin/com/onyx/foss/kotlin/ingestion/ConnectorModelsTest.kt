package com.onyx.foss.kotlin.ingestion

import tools.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class ConnectorModelsTest {
    private val mapper = jacksonObjectMapper()

    @Test
    fun connectorFailureHasExactlyOneTypedTarget() {
        val documentFailure = ConnectorFailure(
            target = FailureTarget.Document(id = "document-1", link = "https://example.com/document-1"),
            message = "Could not index document",
        )
        val entityFailure = ConnectorFailure(
            target = FailureTarget.Entity(id = "entity-1"),
            message = "Could not sync entity",
        )

        assertEquals("document-1", assertIs<FailureTarget.Document>(documentFailure.target).id)
        assertEquals("entity-1", assertIs<FailureTarget.Entity>(entityFailure.target).id)
    }

    @Test
    fun externalAccessRepresentsPublicAndPrivateDocuments() {
        val publicAccess = ExternalAccess(isPublic = true)
        val privateAccess = ExternalAccess(
            externalUserEmails = setOf("member@example.com"),
            externalUserGroupIds = setOf("engineering"),
            isPublic = false,
        )

        assertTrue(publicAccess.isPublic)
        assertEquals(0, publicAccess.numEntries)
        assertFalse(privateAccess.isPublic)
        assertEquals(2, privateAccess.numEntries)
    }

    @Test
    fun checkpointJsonRoundTripsWithoutLosingHasMore() {
        val checkpoint = ConnectorCheckpoint(
            value = mapper.readTree("""{"cursor":"next-page"}"""),
            hasMore = true,
        )

        val restored = mapper.readValue(mapper.writeValueAsString(checkpoint), ConnectorCheckpoint::class.java)

        assertTrue(restored.hasMore)
        assertEquals("next-page", restored.value.path("cursor").asText())
    }
}
