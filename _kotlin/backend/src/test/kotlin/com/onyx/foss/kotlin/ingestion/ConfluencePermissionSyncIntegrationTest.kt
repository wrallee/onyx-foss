package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.domain.AttemptStatus
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairEntity
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorEntity
import com.onyx.foss.kotlin.domain.ConnectorRepository
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.CredentialEntity
import com.onyx.foss.kotlin.domain.CredentialRepository
import com.onyx.foss.kotlin.domain.IndexedDocumentEntity
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.domain.PermissionSyncAttemptRepository
import com.onyx.foss.kotlin.security.CredentialCipher
import com.onyx.foss.kotlin.support.PostgresIntegrationTest
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.bean.override.mockito.MockitoBean

class ConfluencePermissionSyncIntegrationTest : PostgresIntegrationTest() {
    @Autowired private lateinit var worker: PermissionSyncWorker
    @Autowired private lateinit var mapper: ObjectMapper
    @Autowired private lateinit var cipher: CredentialCipher
    @Autowired private lateinit var connectors: ConnectorRepository
    @Autowired private lateinit var credentials: CredentialRepository
    @Autowired private lateinit var pairs: ConnectorCredentialPairRepository
    @Autowired private lateinit var documents: IndexedDocumentRepository
    @Autowired private lateinit var attempts: PermissionSyncAttemptRepository
    @Autowired private lateinit var jdbc: JdbcTemplate
    @MockitoBean private lateinit var indexer: OpenSearchIndexer

    @BeforeEach
    fun resetDatabase() {
        jdbc.execute(
            "TRUNCATE permission_sync_staging, permission_sync_attempts, ingestion_errors, ingestion_jobs, " +
                "ingestion_attempts, ingestion_checkpoints, indexed_documents, connector_credential_pairs, " +
                "connectors, credentials RESTART IDENTITY CASCADE",
        )
    }

    @Test
    fun unresolvedConfluencePermissionStoresPrivateAclAndPartialAttempt() {
        MockWebServer().use { server ->
            server.dispatcher = unresolvedPermissionDispatcher()
            server.start()
            assertPartialSync(server, "MISSING")
        }
    }

    @Test
    fun unresolvedDirectSpaceUserStoresPrivateAclAndPartialAttempt() = assertUnresolvedSpaceSubject(
        """{"operation":{"operationKey":"read"},"subject":{"type":"user","userKey":"missing"}}""",
    )

    @Test
    fun unresolvedNestedSpaceUserStoresPrivateAclAndPartialAttempt() = assertUnresolvedSpaceSubject(
        """{"operation":{"operationKey":"read"},"subjects":{"user":{"results":[{"userKey":"missing"}]}}}""",
    )

    private fun assertUnresolvedSpaceSubject(permission: String) {
        MockWebServer().use { server ->
            server.dispatcher = unresolvedSpaceSubjectDispatcher(permission)
            server.start()
            assertPartialSync(server, "ENG")
        }
    }

    private fun assertPartialSync(server: MockWebServer, spaceKey: String) {
        val base = server.url("/").toString().trimEnd('/')
        val connector = connectors.save(
            ConnectorEntity(
                name = "confluence",
                source = ConnectorSource.CONFLUENCE,
                connectorSpecificConfig = mapper.readTree(
                    """{"wiki_base":"$base","include_attachments":false}""",
                ),
            ),
        )
        val credential = credentials.save(
            CredentialEntity(
                source = ConnectorSource.CONFLUENCE,
                secretJson = cipher.encrypt(mapper.readTree("""{"confluence_access_token":"token"}""")),
            ),
        )
        val pair = pairs.save(
            ConnectorCredentialPairEntity(
                connectorId = requireNotNull(connector.id),
                credentialId = requireNotNull(credential.id),
                name = "confluence",
                accessType = "sync",
            ),
        )
        val pairId = requireNotNull(pair.id)
        val documentId = "$base/spaces/$spaceKey/pages/111/Runbook"
        documents.save(
            IndexedDocumentEntity(
                ccPairId = pairId,
                sourceDocumentId = documentId,
                title = "Runbook",
                contentHash = "hash",
                metadata = mapper.createObjectNode(),
            ),
        )

        worker.process(pairId)

        val stored = documents.findByCcPairIdAndSourceDocumentId(pairId, documentId)?.externalAccess
        assertThat(mapper.treeToValue(stored, ExternalAccess::class.java)).isEqualTo(ExternalAccess(isPublic = false))
        val attempt = attempts.findAllByCcPairIdOrderByIdDesc(pairId).single()
        assertThat(attempt.status).isEqualTo(AttemptStatus.COMPLETED_WITH_ERRORS)
        assertThat(attempt.totalDocsSynced).isEqualTo(1)
        assertThat(attempt.docsWithPermissionErrors).isEqualTo(1)
    }

    private fun unresolvedPermissionDispatcher(): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse = when (request.requestUrl!!.encodedPath) {
            "/rest/api/space" -> json("""{"results":[{"key":"ENG"}]}""")
            "/rest/api/server-information" -> json("""{"version":"10.2.10"}""")
            "/rest/api/space/ENG/permissions" -> json("[]")
            else -> json(
                """{"results":[{
                    "id":"111","title":"Runbook","space":{"key":"MISSING"},
                    "ancestors":[],"restrictions":{},
                    "_links":{"webui":"/spaces/MISSING/pages/111/Runbook"}
                }]}""".trimIndent(),
            )
        }
    }

    private fun unresolvedSpaceSubjectDispatcher(permission: String): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse = when (request.requestUrl!!.encodedPath) {
            "/rest/api/space" -> json("""{"results":[{"key":"ENG"}]}""")
            "/rest/api/server-information" -> json("""{"version":"10.2.10"}""")
            "/rest/api/space/ENG/permissions" -> json("[$permission]")
            "/rest/api/user" -> MockResponse().setResponseCode(404)
            else -> json(
                """{"results":[{
                    "id":"111","title":"Runbook","space":{"key":"ENG"},
                    "ancestors":[],"restrictions":{},
                    "_links":{"webui":"/spaces/ENG/pages/111/Runbook"}
                }]}""".trimIndent(),
            )
        }
    }

    private fun json(body: String): MockResponse =
        MockResponse().setHeader("Content-Type", "application/json").setBody(body)
}
