package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.Test
import org.springframework.web.reactive.function.client.ClientRequest
import org.springframework.web.reactive.function.client.ExchangeFilterFunction
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Base64
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ConfluenceConnectorLoaderTest {
    private val mapper = jacksonObjectMapper()

    @Test
    fun confluenceConnectorSkipImages() = MockWebServer().use { server ->
        server.dispatcher = attachmentDispatcher(
            pageResponse(),
            listOf(imageAttachment(), pdfAttachment()),
            server,
        )

        val documents = loader().load(
            config(server, "\"allow_images\":false,\"include_comments\":false"),
            credentials(),
            null,
        ).flatMap { it.documents.asSequence() }.toList()

        assertEquals(listOf("Runbook", "spec.pdf"), documents.map { it.title })
        assertFalse(documents.any { it.title == "diagram.png" })
    }

    @Test
    fun confluenceConnectorAllowImages() = MockWebServer().use { server ->
        server.dispatcher = attachmentDispatcher(
            pageResponse(),
            listOf(imageAttachment(), pdfAttachment()),
            server,
        )

        val documents = loader().load(
            config(server, "\"allow_images\":true,\"include_comments\":false"),
            credentials(),
            null,
        ).flatMap { it.documents.asSequence() }.toList()

        val image = documents.single { it.title == "diagram.png" }
        assertEquals("", image.content)
        assertEquals(true, image.metadata["image"])
        assertTrue(documents.any { it.title == "spec.pdf" })
    }

    @Test
    fun confluenceConnectorBasic() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path!!.contains("type%3Dcomment") -> json(
                    """{"results":[{"body":{"storage":{"value":"<p>Useful comment</p>"}}}]}""",
                )
                request.path!!.contains("type%3Dattachment") -> json(
                    mapper.writeValueAsString(mapOf("results" to listOf(pdfAttachment()))),
                )
                request.path!!.contains("/download/") -> MockResponse().setBody("attachment text")
                else -> json(pageResponse("<p>Run <b>safely</b></p>"))
            }
        }

        val documents = loader().load(config(server), credentials(), null)
            .flatMap { it.documents.asSequence() }.toList()

        val page = documents.single { it.title == "Runbook" }
        assertEquals("Run safely\nComment:\nUseful comment", page.content)
        assertEquals("ENG", page.metadata["space"])
        assertEquals(listOf("ops"), page.metadata["labels"])
        assertEquals(Instant.parse("2026-01-02T00:00:00Z"), page.updatedAt)
        assertEquals(listOf("owner@example.com"), page.primaryOwners)
        val attachment = documents.single { it.title == "spec.pdf" }
        assertEquals("attachment text", attachment.content)
        assertEquals(page.id, attachment.metadata["parent_page_id"])
    }

    @Test
    fun confluenceConnectorBasicScoped() = MockWebServer().use { server ->
        server.enqueue(json("""{"cloudId":"cloud-1"}"""))
        server.enqueue(json(pageResponse()))
        server.enqueue(json("""{"results":[]}"""))
        val http = RemoteJsonClient(WebClient.builder().filter(rewriteAtlassianRequestsTo(server)))

        val document = ConfluenceConnectorLoader(http, mapper).load(
            config(server, "\"scoped_token\":true,\"is_cloud\":true,\"include_comments\":false"),
            credentials(),
            null,
        ).single().documents.single()

        assertEquals("/_edge/tenant_info", server.takeRequest().requestUrl!!.encodedPath)
        assertEquals("/ex/confluence/cloud-1/rest/api/content/search", server.takeRequest().requestUrl!!.encodedPath)
        assertTrue(document.id.startsWith(server.url("/").toString().trimEnd('/')))
    }

    @Test
    fun confluenceHtmlLinkScope() {
        val text = loader().parseHtml(
            """<p>LINK_BEFORE <a href="https://example.com/target">LINK_TARGET_ONLY</a> LINK_AFTER</p><p>NEXT</p>""",
        )

        assertEquals("LINK_BEFORE [LINK_TARGET_ONLY](https://example.com/target) LINK_AFTER\nNEXT", text)
    }

    @Test
    fun confluenceHtmlTableScope() {
        val text = loader().parseHtml(
            """<p>TABLE_BEFORE</p><table><tr><th>HEADER_ALPHA</th><th>HEADER_BETA</th></tr><tr><td>CELL_ALPHA</td><td>CELL_BETA_LINE_1<br>CELL_BETA_LINE_2</td></tr></table><h2>TABLE_AFTER_HEADING</h2><p>TABLE_AFTER_PARAGRAPH_MUST_BE_SEPARATE</p><ul><li>TABLE_AFTER_LIST_ONE</li><li>TABLE_AFTER_LIST_TWO</li></ul>""",
        )

        assertEquals(
            "TABLE_BEFORE\n\tHEADER_ALPHA\tHEADER_BETA\n\tCELL_ALPHA\tCELL_BETA_LINE_1 CELL_BETA_LINE_2\n" +
                "TABLE_AFTER_HEADING\nTABLE_AFTER_PARAGRAPH_MUST_BE_SEPARATE\n" +
                "- TABLE_AFTER_LIST_ONE\n- TABLE_AFTER_LIST_TWO",
            text,
        )
    }

    @Test
    fun confluenceConnectorPermissions() = MockWebServer().use { server ->
        server.dispatcher = permissionDispatcher(server)

        val fullIds = loader().load(
            config(server, "\"include_permissions\":true,\"include_attachments\":false,\"include_comments\":false"),
            credentials(),
            null,
        ).flatMap { it.documents.asSequence() }.map { it.id }.toSet()
        val slimIds = loader().retrieveAllSlimDocuments(
            config(server, "\"include_attachments\":false"),
            credentials(),
            includePermissions = true,
        ).flatMap { it.documents.asSequence() }.map { it.id }.toSet()

        assertTrue(fullIds.isNotEmpty())
        assertTrue(fullIds.all(slimIds::contains))
    }

    @Test
    fun confluenceConnectorRestrictionHandling() {
        val loader = loader()
        val cache = mutableMapOf<String, JsonNode?>()
        val page = restrictions(users = listOf("page@example.com"), groups = listOf("page-readers"))
        val access = loader.resolveRestrictions(page, emptyList(), cache) { error("must not fetch ancestors") }

        assertEquals(setOf("page@example.com"), assertNotNull(access).externalUserEmails)
        assertEquals(setOf("page-readers"), access.externalUserGroupIds)
        assertFalse(access.isPublic)
    }

    @Test
    fun unresolvedPageRestrictionDoesNotInheritPublicSpace() = MockWebServer().use { server ->
        val restrictedPage = mapper.readTree(page("111")).deepCopy<ObjectNode>().also { page ->
            page.set<JsonNode>(
                "restrictions",
                mapper.readTree(
                    """{"read":{"restrictions":{"user":{"results":[{"username":"missing"}]},"group":{"results":[]}}}}""",
                ),
            )
        }
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when (request.requestUrl!!.encodedPath) {
                "/rest/api/space" -> json("""{"results":[{"key":"ENG"}]}""")
                "/rest/api/server-information" -> json("""{"version":"10.2.10"}""")
                "/rest/api/space/ENG/permissions" -> json(
                    """[{"operation":{"operationKey":"read"},"subject":{"type":"anonymous"}}]""",
                )
                "/rest/api/user" -> MockResponse().setResponseCode(404)
                else -> json(mapper.writeValueAsString(mapOf("results" to listOf(restrictedPage))))
            }
        }

        val access = loader().load(
            config(
                server,
                "\"include_permissions\":true,\"include_comments\":false,\"include_attachments\":false",
            ),
            credentials(),
            null,
        ).single().documents.single().externalAccess

        assertFalse(assertNotNull(access).isPublic)
        assertEquals(0, access.numEntries)
    }

    @Test
    fun reindexSinglePage() = MockWebServer().use { server ->
        server.dispatcher = reindexDispatcher(setOf("111"), server)
        val url = server.url("spaces/ENG/pages/111/Runbook").toString()

        val batch = loader().reindex(config(server, "\"include_comments\":false,\"include_attachments\":false"), credentials(), listOf(failure(url))).single()

        assertEquals(listOf(url), batch.documents.map { it.id })
        assertTrue(batch.failures.isEmpty())
    }

    @Test
    fun reindexMultiplePages() = MockWebServer().use { server ->
        server.dispatcher = reindexDispatcher(setOf("111", "222"), server)
        val urls = listOf(
            server.url("spaces/ENG/pages/111/Runbook").toString(),
            server.url("spaces/ENG/pages/222/Runbook").toString(),
        )

        val batch = loader().reindex(
            config(server, "\"include_comments\":false,\"include_attachments\":false"),
            credentials(),
            urls.map(::failure),
        ).single()

        assertEquals(urls.toSet(), batch.documents.map { it.id }.toSet())
        assertTrue(batch.failures.isEmpty())
    }

    @Test
    fun reindexUnknownPageYieldsFailure() = MockWebServer().use { server ->
        server.dispatcher = reindexDispatcher(emptySet(), server)
        val url = server.url("spaces/ENG/pages/999/Missing").toString()

        val batch = loader().reindex(config(server), credentials(), listOf(failure(url))).single()

        assertTrue(batch.documents.isEmpty())
        assertEquals(url, (batch.failures.single().target as FailureTarget.Document).id)
    }

    @Test
    fun reindexUnparseableUrlYieldsFailure() {
        val url = "https://example.com/wiki/display/SPACE/Page"
        val batch = loader().reindex(config("https://example.com"), credentials(), listOf(failure(url))).single()

        assertEquals(url, (batch.failures.single().target as FailureTarget.Document).id)
        assertContains(batch.failures.single().message, "Cannot extract page id")
    }

    @Test
    fun reindexEmptyErrors() {
        assertTrue(loader().reindex(config("https://example.com"), credentials(), emptyList()).none())
    }

    @Test
    fun reindexEntityFailuresAreSkipped() {
        val failures = listOf(ConnectorFailure(FailureTarget.Entity("stage"), "failed"))
        assertTrue(loader().reindex(config("https://example.com"), credentials(), failures).none())
    }

    @Test
    fun paginatedCqlUserRetrievalWithOverrides() {
        val config = mapper.readTree(
            """{"wiki_base":"https://example.com","is_cloud":false,"confluence_user_profiles_override":[
                {"user_id":"one","username":"user1","display_name":"User One","email":"one@example.com","type":"override"},
                {"user_id":"two","username":"user2","display_name":"User Two","email":"two@example.com","type":"override"}
            ]}""",
        )

        val users = loader().retrieveUsers(config, credentials())

        assertEquals(listOf("one", "two"), users.map { it.userId })
        assertEquals(listOf("one@example.com", "two@example.com"), users.map { it.email })
    }

    @Test
    fun paginatedCqlUserRetrievalNoOverridesServer() = MockWebServer().use { server ->
        server.enqueue(json("""{"results":[{"userKey":"one","username":"user1","displayName":"User One","type":"known"}]}"""))

        val users = loader().retrieveUsers(config(server, "\"is_cloud\":false"), credentials())

        assertEquals("one", users.single().userId)
        assertEquals("/rest/api/user/list", server.takeRequest().requestUrl!!.encodedPath)
    }

    @Test
    fun paginatedCqlUserRetrievalNoOverridesCloud() = MockWebServer().use { server ->
        server.enqueue(
            json(
                """{"results":[{"user":{"accountId":"one","displayName":"User One","email":"one@example.com","accountType":"atlassian"}}]}""",
            ),
        )

        val users = loader().retrieveUsers(config(server, "\"is_cloud\":true"), credentials())

        assertEquals("one", users.single().userId)
        val request = server.takeRequest()
        assertEquals("/rest/api/search/user", request.requestUrl!!.encodedPath)
        assertEquals("type=user", request.requestUrl!!.queryParameter("cql"))
    }

    @Test
    fun paginationRejectsCrossOriginNextWithoutSendingAuthorization() = MockWebServer().use { trusted ->
        MockWebServer().use { malicious ->
            malicious.enqueue(json("""{"results":[]}"""))
            trusted.enqueue(
                json(
                    mapper.writeValueAsString(
                        mapOf(
                            "results" to listOf(mapper.readTree(page("111"))),
                            "_links" to mapOf("next" to malicious.url("steal").toString()),
                        ),
                    ),
                ),
            )

            val error = assertFailsWith<IllegalArgumentException> {
                loader().load(
                    config(trusted, "\"include_comments\":false,\"include_attachments\":false"),
                    credentials(),
                    null,
                ).toList()
            }

            assertContains(error.message.orEmpty(), "origin")
            assertEquals(0, malicious.requestCount)
        }
    }

    @Test
    fun paginationAcceptsConfiguredOriginAbsoluteNext() = MockWebServer().use { server ->
        server.enqueue(
            json(
                mapper.writeValueAsString(
                    mapOf(
                        "results" to listOf(mapper.readTree(page("111"))),
                        "_links" to mapOf(
                            "next" to server.url("rest/api/content/search?cql=type%3Dpage&start=1&limit=50").toString(),
                        ),
                    ),
                ),
            ),
        )
        server.enqueue(json("""{"results":[]}"""))

        val batches = loader().load(
            config(server, "\"is_cloud\":true,\"include_comments\":false,\"include_attachments\":false"),
            credentials(),
            null,
        ).toList()

        assertEquals(2, batches.size)
        assertEquals("Basic ${Base64.getEncoder().encodeToString("user@example.com:token".toByteArray())}", server.takeRequest().getHeader("Authorization"))
        assertEquals("1", server.takeRequest().requestUrl!!.queryParameter("start"))
    }

    @Test
    fun attachmentDownloadRejectsCrossOriginUrlWithoutSendingAuthorization() = MockWebServer().use { trusted ->
        MockWebServer().use { malicious ->
            malicious.enqueue(MockResponse().setBody("stolen"))
            val attachment = pdfAttachment().toMutableMap().also { value ->
                value["_links"] = mapOf(
                    "download" to malicious.url("steal").toString(),
                    "webui" to "/pages/viewpageattachments.action?pageId=222&preview=att-222",
                )
            }
            trusted.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse =
                    if (request.path!!.contains("type%3Dattachment")) {
                        json(mapper.writeValueAsString(mapOf("results" to listOf(attachment))))
                    } else {
                        json(pageResponse())
                    }
            }

            val batch = loader().load(
                config(trusted, "\"include_comments\":false"),
                credentials(),
                null,
            ).single()

            assertEquals(0, malicious.requestCount)
            assertEquals("confluence_attachment_processing", batch.failures.single().errorType)
            assertFalse(batch.documents.any { it.title == "spec.pdf" })
        }
    }

    @Test
    fun `400KeepsPartialAttachmentsAndRecordsFailure`() = MockWebServer().use { server ->
        var attachmentCalls = 0
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path!!.contains("type%3Dattachment") && attachmentCalls++ == 0 -> json(
                    mapper.writeValueAsString(
                        mapOf(
                            "results" to listOf(pdfAttachment()),
                            "_links" to mapOf("next" to "/rest/api/content/search?cql=type%3Dattachment&start=1&limit=50"),
                        ),
                    ),
                )
                request.path!!.contains("type%3Dattachment") -> MockResponse().setResponseCode(400).setBody("bad offset")
                request.path!!.contains("/download/") -> MockResponse().setBody("partial text")
                else -> json(pageResponse())
            }
        }

        val batch = loader().load(
            config(server, "\"include_comments\":false"),
            credentials(),
            null,
        ).single()

        assertTrue(batch.documents.any { it.title == "spec.pdf" && it.content == "partial text" })
        assertEquals("confluence_attachment_pagination", batch.failures.single().errorType)
        assertTrue((batch.failures.single().target as FailureTarget.Document).id.contains("/pages/111/"))
    }

    @Test
    fun `401403SkipsPageAttachments`() {
        listOf(401, 403).forEach { status ->
            MockWebServer().use { server ->
                server.enqueue(json(pageResponse()))
                server.enqueue(MockResponse().setResponseCode(status).setBody("denied"))

                val batch = loader().load(
                    config(server, "\"include_comments\":false"),
                    credentials(),
                    null,
                ).single()

                assertEquals(listOf("Runbook"), batch.documents.map { it.title })
                assertContains(batch.failures.single().message, status.toString())
            }
        }
    }

    @Test
    fun otherHttpErrorsPropagate(): Unit = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse =
                if (request.path!!.contains("type%3Dattachment")) MockResponse().setResponseCode(500) else json(pageResponse())
        }

        assertFailsWith<WebClientResponseException> {
            loader().load(config(server, "\"include_comments\":false"), credentials(), null).toList()
        }
    }

    @Test
    fun `400DateErrorPropagates`(): Unit = MockWebServer().use { server ->
        server.enqueue(json(pageResponse()))
        repeat(2) {
            server.enqueue(MockResponse().setResponseCode(400).setBody("The field 'updated' is invalid"))
        }

        assertFailsWith<WebClientResponseException> {
            loader().load(config(server, "\"include_comments\":false"), credentials(), null).toList()
        }
    }

    @Test
    fun reindexRefetchesAttachments() = MockWebServer().use { server ->
        server.dispatcher = reindexWithAttachmentDispatcher(server, attachmentStatus = 200)
        val url = server.url("spaces/ENG/pages/111/Runbook").toString()

        val batch = loader().reindex(config(server, "\"include_comments\":false"), credentials(), listOf(failure(url))).single()

        assertTrue(batch.documents.any { it.title == "spec.pdf" })
        assertTrue(batch.failures.isEmpty())
    }

    @Test
    fun slimAttachment400RetriesThenSucceeds() = MockWebServer().use { server ->
        var calls = 0
        server.dispatcher = slimAttachmentDispatcher(server) { if (calls++ < 2) 400 else 200 }

        val documents = loader().retrieveAllSlimDocuments(config(server), credentials())
            .flatMap { it.documents.asSequence() }.toList()

        assertTrue(documents.any { it.title == "spec.pdf" })
        assertEquals(3, calls)
    }

    @Test
    fun slimAttachmentPersistent400Raises(): Unit = MockWebServer().use { server ->
        server.dispatcher = slimAttachmentDispatcher(server) { 400 }

        assertFailsWith<WebClientResponseException> {
            loader().retrieveAllSlimDocuments(config(server), credentials()).toList()
        }
    }

    @Test
    fun slimAttachmentOtherErrorsRaiseImmediately() {
        listOf(403 to "denied", 400 to "The field 'updated' is invalid").forEach { (status, body) ->
            MockWebServer().use { server ->
                var calls = 0
                server.dispatcher = object : Dispatcher() {
                    override fun dispatch(request: RecordedRequest): MockResponse =
                        if (request.path!!.contains("type%3Dattachment")) {
                            calls += 1
                            MockResponse().setResponseCode(status).setBody(body)
                        } else {
                            json(pageResponse())
                        }
                }
                assertFailsWith<WebClientResponseException> {
                    loader().retrieveAllSlimDocuments(config(server), credentials()).toList()
                }
                assertEquals(1, calls)
            }
        }

        MockWebServer().use { server ->
            var calls = 0
            server.dispatcher = slimAttachmentDispatcher(server) { if (calls++ < 20) 500 else 200 }
            assertFailsWith<WebClientResponseException> {
                loader().retrieveAllSlimDocuments(config(server), credentials()).toList()
            }
        }
    }

    @Test
    fun reindexRerecordsAttachmentFailure() = MockWebServer().use { server ->
        server.dispatcher = reindexWithAttachmentDispatcher(server, attachmentStatus = 400)
        val url = server.url("spaces/ENG/pages/111/Runbook").toString()

        val batch = loader().reindex(config(server, "\"include_comments\":false"), credentials(), listOf(failure(url))).single()

        assertTrue(batch.documents.any { it.title == "spec.pdf" })
        assertEquals("confluence_attachment_pagination", batch.failures.single().errorType)
    }

    @Test
    fun attachmentPaginationRejectsRepeatedNextLink() = MockWebServer().use { server ->
        var repeatedCalls = 0
        val repeatedPath = "/rest/api/content/search?cql=type%3Dattachment&start=1&limit=50"
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path!!.contains("type%3Dattachment") && !request.path!!.contains("start=1") -> json(
                    """{"results":[{"title":"ignored.bin"}],"_links":{"next":"$repeatedPath"}}""",
                )
                request.path!!.contains("type%3Dattachment") && repeatedCalls++ < 2 -> json(
                    """{"results":[{"title":"ignored.bin"}],"_links":{"next":"$repeatedPath"}}""",
                )
                request.path!!.contains("type%3Dattachment") -> MockResponse().setResponseCode(418)
                else -> json(pageResponse())
            }
        }

        val error = assertFailsWith<IllegalArgumentException> {
            loader().load(
                config(server, "\"is_cloud\":true,\"include_comments\":false"),
                credentials(),
                null,
            ).toList()
        }

        assertContains(error.message.orEmpty(), "cycle")
        assertTrue(repeatedCalls <= 1)
    }

    @Test
    fun attachmentPaginationRejectsMoreThanOneHundredThousandResults() = MockWebServer().use { server ->
        val attachments = buildString {
            append("{\"results\":[")
            repeat(100_001) { index ->
                if (index > 0) append(',')
                append("{\"title\":\"ignored.bin\"}")
            }
            append("]}")
        }
        server.enqueue(json(pageResponse()))
        server.enqueue(json(attachments))

        val error = assertFailsWith<IllegalArgumentException> {
            loader().load(
                config(server, "\"include_comments\":false"),
                credentials(),
                null,
            ).toList()
        }

        assertContains(error.message.orEmpty(), "100000")
    }

    @Test
    fun isConfcloud77618ResponseMatchesCanonicalBody() {
        assertTrue(loader().isConfcloud77618(404, CONFCLOUD_77618_BODY))
    }

    @Test
    fun isConfcloud77618ResponseMatchesOutdatedSibling() {
        assertTrue(loader().isConfcloud77618(404, CONFCLOUD_76424_BODY))
    }

    @Test
    fun isConfcloud77618ResponseRejectsUnrelated404() {
        assertFalse(loader().isConfcloud77618(404, "{\"message\":\"Not found\"}"))
    }

    @Test
    fun isConfcloud77618ResponseRejectsNon404() {
        assertFalse(loader().isConfcloud77618(500, CONFCLOUD_77618_BODY))
    }

    @Test
    fun paginateUrlRaisesConfcloud77618OnSignatureMatch(): Unit = MockWebServer().use { server ->
        server.enqueue(MockResponse().setResponseCode(404).setBody(CONFCLOUD_77618_BODY))

        assertFailsWith<Confcloud77618Exception> {
            loader().paginate(
                config(server, "\"is_cloud\":true"),
                credentials(),
                "/rest/api/content/search?expand=ancestors.restrictions.read.restrictions.user",
                50,
            ).toList()
        }
    }

    @Test
    fun paginateUrlPropagatesUnrelated404(): Unit = MockWebServer().use { server ->
        server.enqueue(MockResponse().setResponseCode(404).setBody("not found"))

        assertFailsWith<WebClientResponseException.NotFound> {
            loader().paginate(
                config(server, "\"is_cloud\":true"),
                credentials(),
                "/rest/api/content/search?expand=ancestors.restrictions.read.restrictions.user",
                50,
            ).toList()
        }
    }

    @Test
    fun paginateUrlDoesNotRaise77618WithoutAncestorExpand(): Unit = MockWebServer().use { server ->
        server.enqueue(MockResponse().setResponseCode(404).setBody(CONFCLOUD_77618_BODY))

        assertFailsWith<WebClientResponseException.NotFound> {
            loader().paginate(config(server, "\"is_cloud\":true"), credentials(), "/rest/api/content/search", 50).toList()
        }
    }

    @Test
    fun fetchContentReadRestrictionsHitsByoperationEndpoint() = MockWebServer().use { server ->
        server.enqueue(json(mapper.writeValueAsString(restrictions(users = listOf("reader@example.com")))))

        val result = loader().fetchContentReadRestrictions(config(server), credentials(), "999")

        assertEquals("reader@example.com", result!!.path("read").path("restrictions").path("user").path("results")[0].path("email").asText())
        assertEquals("/rest/api/content/999/restriction/byOperation", server.takeRequest().requestUrl!!.encodedPath)
    }

    @Test
    fun fetchContentReadRestrictionsReturnsNoneOn403() = assertRestrictionFetchReturnsNull(403)

    @Test
    fun fetchContentReadRestrictionsReturnsNoneOn404() = assertRestrictionFetchReturnsNull(404)

    @Test
    fun fetchContentReadRestrictionsRaisesOn500(): Unit = MockWebServer().use { server ->
        server.enqueue(MockResponse().setResponseCode(500).setBody("boom"))
        assertFailsWith<WebClientResponseException> {
            loader().fetchContentReadRestrictions(config(server), credentials(), "999")
        }
    }

    @Test
    fun perAncestorFetchShortCircuitsOnPageLevelRestriction() {
        var calls = 0
        val access = loader().resolveRestrictions(
            restrictions(users = listOf("page@example.com")),
            listOf(mapper.readTree("""{"id":"parent"}""")),
            mutableMapOf(),
        ) { calls += 1; null }

        assertEquals(setOf("page@example.com"), assertNotNull(access).externalUserEmails)
        assertEquals(0, calls)
    }

    @Test
    fun perAncestorFetchWalksAncestorsImmediateParentFirst() {
        val seen = mutableListOf<String>()
        val access = loader().resolveRestrictions(
            mapper.createObjectNode(),
            listOf(mapper.readTree("""{"id":"root"}"""), mapper.readTree("""{"id":"parent"}""")),
            mutableMapOf(),
        ) { id ->
            seen += id
            restrictions(users = listOf("$id@example.com"))
        }

        assertEquals(listOf("parent"), seen)
        assertEquals(setOf("parent@example.com"), assertNotNull(access).externalUserEmails)
    }

    @Test
    fun perAncestorFetchSkipsDraftsAndContinuesUp() {
        val access = loader().resolveRestrictions(
            mapper.createObjectNode(),
            listOf(mapper.readTree("""{"id":"grandparent"}"""), mapper.readTree("""{"id":"draft"}""")),
            mutableMapOf(),
        ) { id -> if (id == "draft") null else restrictions(users = listOf("gp@example.com")) }

        assertEquals(setOf("gp@example.com"), assertNotNull(access).externalUserEmails)
    }

    @Test
    fun perAncestorFetchReturnsNoneWhenNoRestrictionsAnywhere() {
        val access = loader().resolveRestrictions(
            mapper.createObjectNode(),
            listOf(mapper.readTree("""{"id":"one"}"""), mapper.readTree("""{"id":"two"}""")),
            mutableMapOf(),
        ) { mapper.createObjectNode() }

        assertNull(access)
    }

    @Test
    fun perAncestorFetchCachesSharedAncestorsAcrossCalls() {
        val cache = mutableMapOf<String, JsonNode?>()
        var calls = 0
        repeat(5) {
            loader().resolveRestrictions(
                mapper.createObjectNode(),
                listOf(mapper.readTree("""{"id":"one"}"""), mapper.readTree("""{"id":"two"}""")),
                cache,
            ) { calls += 1; mapper.createObjectNode() }
        }
        assertEquals(2, calls)
    }

    @Test
    fun perAncestorFetchCachesNoneForDrafts() {
        val cache = mutableMapOf<String, JsonNode?>()
        var calls = 0
        repeat(3) {
            loader().resolveRestrictions(
                mapper.createObjectNode(),
                listOf(mapper.readTree("""{"id":"draft"}""")),
                cache,
            ) { calls += 1; null }
        }
        assertEquals(1, calls)
        assertTrue(cache.containsKey("draft"))
        assertNull(cache["draft"])
    }

    @Test
    fun permSyncRetriesInPerPageModeOn77618() = MockWebServer().use { server ->
        var fastPathCalls = 0
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path!!.contains("/api/v2/spaces") -> json("""{"results":[]}""")
                request.path!!.contains("ancestors.restrictions") && fastPathCalls++ == 0 -> json(
                    mapper.writeValueAsString(
                        mapOf(
                            "results" to listOf(mapper.readTree(page("111"))),
                            "_links" to mapOf(
                                "next" to "/rest/api/content/search?cql=type%3Dpage&expand=ancestors.restrictions.read.restrictions.user&start=1&limit=5000",
                            ),
                        ),
                    ),
                )
                request.path!!.contains("ancestors.restrictions") -> MockResponse().setResponseCode(404).setBody(CONFCLOUD_77618_BODY)
                request.path!!.contains("/restriction/byOperation") -> json("{}")
                request.path!!.contains("type%3Dattachment") -> json("""{"results":[]}""")
                request.path!!.contains("content/search") -> json(
                    mapper.writeValueAsString(
                        mapOf("results" to listOf(mapper.readTree(page("111")), mapper.readTree(page("222")))),
                    ),
                )
                else -> json("{}")
            }
        }

        val documents = loader().retrieveAllSlimDocuments(
            config(server, "\"is_cloud\":true,\"include_attachments\":false"),
            credentials(),
            includePermissions = true,
        ).flatMap { it.documents.asSequence() }.toList()

        assertEquals(listOf("111", "111", "222"), documents.map { it.metadata["confluence_page_id"] })
        assertEquals(2, fastPathCalls)
        assertTrue(server.requestCount >= 3)
    }

    @Test
    fun permSyncNoRetryWhenFirstAttemptSucceeds() = MockWebServer().use { server ->
        server.dispatcher = permissionDispatcher(server)

        val documents = loader().retrieveAllSlimDocuments(
            config(server, "\"include_attachments\":false"),
            credentials(),
            includePermissions = true,
        ).flatMap { it.documents.asSequence() }.toList()

        assertEquals(1, documents.size)
        val requests = server.takeRequests(server.requestCount)
        assertEquals(1, requests.count { it.path!!.contains("content/search") })
        assertFalse(requests.any { it.path!!.contains("restriction/byOperation") })
    }

    @Test
    fun pruningExpandSkipsRestrictionsButKeepsHierarchy() = MockWebServer().use { server ->
        server.enqueue(json("""{"results":[]}"""))

        loader().retrieveAllSlimDocuments(config(server), credentials()).toList()

        val expand = server.takeRequest().requestUrl!!.queryParameter("expand").orEmpty()
        assertContains(expand, "space")
        assertContains(expand, "ancestors")
        assertFalse(expand.contains("restrictions.read.restrictions"))
    }

    @Test
    fun attachmentSectionLinkUsesPlatformSpecificUrl() {
        listOf(false, true).forEach { isCloud ->
            MockWebServer().use { server ->
                server.dispatcher = attachmentDispatcher(pageResponse(), listOf(pdfAttachment()), server)
                val attachment = loader().load(
                    config(server, "\"is_cloud\":$isCloud,\"include_comments\":false"),
                    credentials(),
                    null,
                ).single().documents.single { it.title == "spec.pdf" }

                val expected = if (isCloud) {
                    server.url("wiki/download/attachments/111/spec.pdf").toString()
                } else {
                    server.url("download/attachments/222/spec.pdf").toString()
                }
                assertEquals(expected, attachment.link)
            }
        }
    }

    @Test
    fun attachmentFailureUsesAttachmentDocumentId() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path!!.contains("type%3Dattachment") -> json(
                    mapper.writeValueAsString(mapOf("results" to listOf(pdfAttachment()))),
                )
                request.path!!.contains("/download/") -> MockResponse().setResponseCode(500).setBody("failed")
                else -> json(pageResponse())
            }
        }

        val failure = loader().load(
            config(server, "\"include_comments\":false"),
            credentials(),
            null,
        ).single().failures.single()

        val target = failure.target as FailureTarget.Document
        assertEquals(server.url("pages/viewpageattachments.action?pageId=222&preview=att-222").toString(), target.id)
        assertEquals(server.url("download/attachments/222/spec.pdf").toString(), target.link)
    }

    @Test
    fun getCqlQueryWithSpace() {
        val query = loader().constructPageCql(
            mapper.readTree("""{"space":"TEST","timezone_offset":0}"""),
            Instant.parse("2023-01-01T00:00:00Z"),
            Instant.parse("2023-01-02T00:00:00Z"),
        )

        assertContains(query, "space='TEST'")
        assertContains(query, "lastmodified >= '2023-01-01 00:00'")
        assertContains(query, "lastmodified <= '2023-01-02 00:00'")
    }

    @Test
    fun getCqlQueryWithoutSpace() {
        val query = loader().constructPageCql(
            mapper.readTree("""{"timezone_offset":0}"""),
            Instant.parse("2023-01-01T00:00:00Z"),
            Instant.parse("2023-01-02T00:00:00Z"),
        )

        assertFalse(query.contains("space="))
        assertContains(query, "lastmodified >= '2023-01-01 00:00'")
        assertContains(query, "lastmodified <= '2023-01-02 00:00'")
    }

    @Test
    fun loadFromCheckpointHappyPath() = MockWebServer().use { server ->
        server.enqueue(
            json(
                mapper.writeValueAsString(
                    mapOf(
                        "results" to listOf(mapper.readTree(page("1")), mapper.readTree(page("2"))),
                        "_links" to mapOf("next" to "/rest/api/content/search?cql=type%3Dpage&start=2&limit=2"),
                    ),
                ),
            ),
        )
        repeat(4) { server.enqueue(json("""{"results":[]}""")) }
        server.enqueue(json(mapper.writeValueAsString(mapOf("results" to listOf(mapper.readTree(page("3")))))))
        repeat(2) { server.enqueue(json("""{"results":[]}""")) }

        val batches = loader().load(
            config(server, "\"batch_size\":2"),
            credentials(),
            null,
        ).toList()

        assertEquals(listOf(2, 1), batches.map { it.documents.size })
        assertEquals(
            "/rest/api/content/search?cql=type%3Dpage&start=2&limit=2",
            mapper.treeToValue(batches[0].checkpoint.value, ConfluenceCheckpoint::class.java).nextPageUrl,
        )
        assertFalse(mapper.treeToValue(batches[1].checkpoint.value, ConfluenceCheckpoint::class.java).hasMore)
    }

    @Test
    fun loadFromCheckpointWithPageProcessingError() = MockWebServer().use { server ->
        val bad = mapper.readTree(page("2")).deepCopy<com.fasterxml.jackson.databind.node.ObjectNode>().also { it.remove("version") }
        server.enqueue(
            json(
                mapper.writeValueAsString(
                    mapOf("results" to listOf(mapper.readTree(page("1")), bad)),
                ),
            ),
        )
        repeat(4) { server.enqueue(json("""{"results":[]}""")) }

        val batch = loader().load(config(server), credentials(), null).single()

        assertEquals(listOf("1"), batch.documents.map { it.metadata["confluence_page_id"] })
        assertEquals("2", (batch.failures.single().target as FailureTarget.Document).id)
    }

    @Test
    fun retrieveAllSlimDocsPermSync() = MockWebServer().use { server ->
        server.dispatcher = permissionDispatcher(server)

        val batch = loader().retrieveAllSlimDocuments(
            config(server, "\"include_attachments\":false"),
            credentials(),
            includePermissions = true,
        ).single()

        val document = batch.documents.single()
        assertEquals("", document.content)
        assertEquals(setOf("readers"), assertNotNull(document.externalAccess).externalUserGroupIds)
        assertTrue(batch.failures.isEmpty())
    }

    @Test
    fun missingSlimPermissionEmitsPrivateDocumentFailure() = MockWebServer().use { server ->
        val unresolved = mapper.readTree(page("111")).deepCopy<ObjectNode>().also {
            (it.path("space") as ObjectNode).put("key", "MISSING")
        }
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when (request.requestUrl!!.encodedPath) {
                "/rest/api/space" -> json("""{"results":[{"key":"ENG"}]}""")
                "/rest/api/server-information" -> json("""{"version":"10.2.10"}""")
                "/rest/api/space/ENG/permissions" -> json("[]")
                else -> json(mapper.writeValueAsString(mapOf("results" to listOf(unresolved))))
            }
        }

        val batch = loader().retrieveAllSlimDocuments(
            config(server, "\"include_attachments\":false"),
            credentials(),
            includePermissions = true,
        ).single()

        val document = batch.documents.single()
        assertEquals(ExternalAccess(isPublic = false), document.externalAccess)
        assertEquals(document.id, (batch.failures.single().target as FailureTarget.Document).id)
        assertEquals("confluence_permission_unresolved", batch.failures.single().errorType)
    }

    @Test
    fun validateConnectorSettingsErrors() {
        listOf(401 to "expired", 403 to "permissions", 404 to "Unexpected Confluence error").forEach { (status, message) ->
            MockWebServer().use { server ->
                server.enqueue(MockResponse().setResponseCode(status).setBody("upstream"))
                val error = assertFailsWith<IllegalArgumentException> {
                    loader().validate(config(server, "\"is_cloud\":false"), credentials())
                }
                assertTrue(error.message.orEmpty().contains(message, ignoreCase = true))
            }
        }
    }

    @Test
    fun validateConnectorSettingsSuccess() = MockWebServer().use { server ->
        server.enqueue(json("""{"results":[{"key":"TEST"}]}"""))
        server.enqueue(json("""{"key":"TEST"}"""))

        loader().validate(config(server, "\"is_cloud\":false,\"space\":\"TEST\""), credentials())

        assertEquals("/rest/api/space", server.takeRequest().requestUrl!!.encodedPath)
        assertEquals("/rest/api/space/TEST", server.takeRequest().requestUrl!!.encodedPath)
    }

    @Test
    fun checkpointProgress() = MockWebServer().use { server ->
        server.enqueue(
            json(
                mapper.writeValueAsString(
                    mapOf(
                        "results" to listOf(mapper.readTree(page("1"))),
                        "_links" to mapOf("next" to "/rest/api/content/search?cql=type%3Dpage&start=1&limit=1"),
                    ),
                ),
            ),
        )
        repeat(2) { server.enqueue(json("""{"results":[]}""")) }
        val iterator = loader().load(config(server, "\"batch_size\":1"), credentials(), null).iterator()
        val first = iterator.next()
        val checkpoint = mapper.treeToValue(first.checkpoint.value, ConfluenceCheckpoint::class.java)

        assertTrue(checkpoint.hasMore)
        assertContains(assertNotNull(checkpoint.nextPageUrl), "start=1")
    }

    @Test
    fun completedCheckpointRestartsForNewPollWindow() = MockWebServer().use { server ->
        server.enqueue(json(pageResponse()))
        val completed = mapper.valueToTree<JsonNode>(
            ConfluenceCheckpoint(hasMore = false, nextPageUrl = "/must-not-resume?start=99"),
        )

        loader().load(
            config(server, "\"include_comments\":false,\"include_attachments\":false"),
            credentials(),
            completed,
            start = Instant.parse("2026-01-01T00:00:00Z"),
            end = Instant.parse("2026-01-02T00:00:00Z"),
        ).single()

        val request = server.takeRequest()
        assertEquals("/rest/api/content/search", request.requestUrl!!.encodedPath)
        val cql = request.requestUrl!!.queryParameter("cql").orEmpty()
        assertContains(cql, "lastmodified >= '2026-01-01 00:00'")
        assertContains(cql, "lastmodified <= '2026-01-02 00:00'")
        assertFalse(request.path!!.contains("must-not-resume"))
    }

    @Test
    fun inProgressCheckpointResumesNextPageUrl() = MockWebServer().use { server ->
        server.enqueue(json(pageResponse()))
        val active = mapper.valueToTree<JsonNode>(
            ConfluenceCheckpoint(
                hasMore = true,
                nextPageUrl = "/rest/api/content/search?cql=type%3Dpage&start=7&limit=2",
            ),
        )

        loader().load(
            config(server, "\"include_comments\":false,\"include_attachments\":false"),
            credentials(),
            active,
            start = Instant.parse("2026-01-01T00:00:00Z"),
            end = Instant.parse("2026-01-02T00:00:00Z"),
        ).single()

        val request = server.takeRequest()
        assertEquals("7", request.requestUrl!!.queryParameter("start"))
        assertEquals("type=page", request.requestUrl!!.queryParameter("cql"))
    }

    @Test
    fun usernameEmailCacheIsInstanceIsolated() = MockWebServer().use { server ->
        val loader = loader()
        val calls = mutableMapOf<String, Int>()
        server.dispatcher = userResolutionDispatcher(restrictedPages("username", "jsmith")) { request ->
            val authorization = request.getHeader("Authorization").orEmpty()
            calls[authorization] = calls.getOrDefault(authorization, 0) + 1
            json("""{"email":"${if (authorization.endsWith("token-a")) "a" else "b"}@example.com"}""")
        }

        val first = loadRestricted(loader, server, "token-a")
        val second = loadRestricted(loader, server, "token-b")

        assertTrue(first.all { it.externalAccess?.externalUserEmails == setOf("a@example.com") })
        assertTrue(second.all { it.externalAccess?.externalUserEmails == setOf("b@example.com") })
        assertEquals(mapOf("Bearer token-a" to 1, "Bearer token-b" to 1), calls)
    }

    @Test
    fun userkeyEmailCacheIsInstanceIsolated() = MockWebServer().use { server ->
        val loader = loader()
        val calls = mutableMapOf<String, Int>()
        server.dispatcher = userResolutionDispatcher(restrictedPages("userKey", "key")) { request ->
            val authorization = request.getHeader("Authorization").orEmpty()
            calls[authorization] = calls.getOrDefault(authorization, 0) + 1
            json("""{"email":"${if (authorization.endsWith("token-a")) "a" else "b"}@example.com"}""")
        }

        val first = loadRestricted(loader, server, "token-a")
        val second = loadRestricted(loader, server, "token-b")

        assertTrue(first.all { it.externalAccess?.externalUserEmails == setOf("a@example.com") })
        assertTrue(second.all { it.externalAccess?.externalUserEmails == setOf("b@example.com") })
        assertEquals(mapOf("Bearer token-a" to 1, "Bearer token-b" to 1), calls)
    }

    @Test
    fun displayNameCacheIsInstanceIsolated() = MockWebServer().use { server ->
        val loader = loader()
        val taggedPages = listOf("1", "2").map { id ->
            mapper.readTree(page(id, """<p><ri:user ri:userkey="user-1"/></p>"""))
        }
        val calls = mutableMapOf<String, Int>()
        server.dispatcher = userResolutionDispatcher(taggedPages) { request ->
            val authorization = request.getHeader("Authorization").orEmpty()
            calls[authorization] = calls.getOrDefault(authorization, 0) + 1
            json("""{"displayName":"${if (authorization.endsWith("token-a")) "Alice A" else "Bob B"}"}""")
        }

        val first = loadWithoutExtras(loader, server, "token-a")
        val second = loadWithoutExtras(loader, server, "token-b")

        assertTrue(first.all { it.content == "@Alice A" })
        assertTrue(second.all { it.content == "@Bob B" })
        assertEquals(mapOf("Bearer token-a" to 1, "Bearer token-b" to 1), calls)
    }

    @Test
    fun dateLozengeTextIsPreserved() {
        val text = loader().parseHtml("""<p>Meeting on <span class="date-lozenger-container">April 22, 2026</span> at noon.</p>""")
        assertContains(text, "April 22, 2026")
    }

    @Test
    fun pageWithoutDateLozengeUnaffected() {
        assertEquals("No dates here.", loader().parseHtml("<p>No dates here.</p>"))
    }

    @Test
    fun slimDocsIncludeAttachmentsByDefault() = MockWebServer().use { server ->
        server.dispatcher = slimAttachmentDispatcher(server) { 200 }

        val documents = loader().retrieveAllSlimDocuments(
            config(server, "\"allow_images\":true"),
            credentials(),
        ).flatMap { it.documents.asSequence() }.toList()

        assertTrue(documents.any { it.title == "spec.pdf" })
    }

    @Test
    fun mainPassSkipsAttachmentFetchWhenDisabled() = MockWebServer().use { server ->
        server.enqueue(json(pageResponse()))

        val documents = loader().load(
            config(server, "\"include_attachments\":false,\"include_comments\":false"),
            credentials(),
            null,
        ).single().documents

        assertEquals(listOf("Runbook"), documents.map { it.title })
        assertEquals(1, server.requestCount)
    }

    @Test
    fun slimDocsSkipAttachmentsWhenDisabled() = MockWebServer().use { server ->
        server.enqueue(json(pageResponse()))

        val documents = loader().retrieveAllSlimDocuments(
            config(server, "\"include_attachments\":false"),
            credentials(),
        ).single().documents

        assertEquals(listOf("Runbook"), documents.map { it.title })
        assertEquals(1, server.requestCount)
    }

    @Test
    fun retrieveConfluenceSpacesServerPaginatesPastCappedPage() = MockWebServer().use { server ->
        val keys = listOf("AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH")
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val start = request.requestUrl!!.queryParameter("start")?.toInt() ?: 0
                val results = keys.drop(start).take(3).map { mapOf("key" to it) }
                val links = if (start + results.size < keys.size) mapOf("next" to "/rest/api/space?limit=5000&start=${start + 3}") else emptyMap()
                return json(mapper.writeValueAsString(mapOf("results" to results, "_links" to links)))
            }
        }

        val spaces = loader().retrieveSpaces(config(server, "\"is_cloud\":false"), credentials(), 5_000)

        assertEquals(keys, spaces.map { it.path("key").asText() })
        assertEquals(3, server.requestCount)
    }

    @Test
    fun paginateUrlServerReDerivesStartWhenDcUnderCounts() = MockWebServer().use { server ->
        val ids = (1..8).toList()
        val starts = mutableListOf<Int>()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val start = request.requestUrl!!.queryParameter("start")?.toInt() ?: 0
                starts += start
                val results = ids.drop(start).take(3).map { mapOf("id" to it) }
                val links = if (start + results.size < ids.size) {
                    mapOf("next" to "/rest/api/content/search?cql=type%3Dpage&limit=10&start=${start + 10}")
                } else {
                    emptyMap()
                }
                return json(mapper.writeValueAsString(mapOf("results" to results, "_links" to links)))
            }
        }

        val results = loader().paginate(config(server, "\"is_cloud\":false"), credentials(), "/rest/api/content/search?cql=type%3Dpage", 10)

        assertEquals(ids, results.map { it.path("id").asInt() })
        assertEquals(listOf(0, 3, 6), starts)
    }

    @Test
    fun retrieveConfluenceSpacesServerStopsWhenNextLinkAbsent() = MockWebServer().use { server ->
        var page = 0
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                page += 1
                return json(
                    mapper.writeValueAsString(
                        mapOf(
                            "results" to listOf(mapOf("key" to "SPACE$page")),
                            "_links" to if (page == 1) mapOf("next" to "/rest/api/space?limit=5000&start=1") else emptyMap(),
                        ),
                    ),
                )
            }
        }

        assertEquals(
            listOf("SPACE1", "SPACE2"),
            loader().retrieveSpaces(config(server, "\"is_cloud\":false"), credentials(), 5_000).map { it.path("key").asText() },
        )
        assertEquals(2, server.requestCount)
    }

    @Test
    fun jsonrpcWebsudoHtmlResponseRaisesValidationError() = MockWebServer().use { server ->
        server.enqueue(
            MockResponse().setResponseCode(200).setHeader("Content-Type", "text/html;charset=UTF-8")
                .setBody("<html><h1>WebSudoRequiredException</h1></html>"),
        )

        val error = assertFailsWith<IllegalArgumentException> {
            loader().getAllSpacePermissionsServerJsonRpc(config(server), credentials(), "TST")
        }

        assertContains(error.message.orEmpty(), "Secure Administrator Sessions")
        assertContains(error.message.orEmpty(), "TST")
        assertContains(error.message.orEmpty(), "support.atlassian.com")
        assertContains(error.message.orEmpty(), "HTTP 200")
        assertContains(error.message.orEmpty(), "text/html")
        assertContains(error.message.orEmpty(), "WebSudoRequiredException")
    }

    @Test
    fun supportsRestSpacePermissionsTrueForDc91Plus() = MockWebServer().use { server ->
        server.enqueue(json("""{"version":"10.2.10"}"""))
        val loader = loader()
        val config = config(server, "\"is_cloud\":false")

        assertTrue(loader.supportsRestSpacePermissions(config, credentials()))
        assertTrue(loader.supportsRestSpacePermissions(config, credentials()))
        assertEquals(1, server.requestCount)
        assertEquals("/rest/api/server-information", server.takeRequest().requestUrl!!.encodedPath)
    }

    @Test
    fun supportsRestSpacePermissionsFalseForDcPre91() = MockWebServer().use { server ->
        server.enqueue(json("""{"version":"8.9.1"}"""))
        assertFalse(loader().supportsRestSpacePermissions(config(server, "\"is_cloud\":false"), credentials()))
    }

    @Test
    fun supportsRestSpacePermissionsFalseWhenProbeFails() = MockWebServer().use { server ->
        server.enqueue(MockResponse().setResponseCode(500))
        val loader = loader()
        val config = config(server, "\"is_cloud\":false")

        assertFalse(loader.supportsRestSpacePermissions(config, credentials()))
        assertFalse(loader.supportsRestSpacePermissions(config, credentials()))
        assertEquals(1, server.requestCount)
    }

    @Test
    fun getAllSpacePermissionsServerRest404RaisesUnavailable() = MockWebServer().use { server ->
        server.enqueue(MockResponse().setResponseCode(404))
        val error = assertFailsWith<ConfluenceRestSpacePermissionsNotAvailableException> {
            loader().getAllSpacePermissionsServerRest(config(server, "\"is_cloud\":false"), credentials(), "ENG")
        }
        assertContains(error.message.orEmpty(), "ENG")
        assertContains(error.message.orEmpty(), "9.1")
    }

    @Test
    fun getAllSpacePermissionsServerRest500RaisesInsufficientPermissions() = MockWebServer().use { server ->
        server.enqueue(MockResponse().setResponseCode(500))
        val error = assertFailsWith<IllegalArgumentException> {
            loader().getAllSpacePermissionsServerRest(config(server, "\"is_cloud\":false"), credentials(), "ENG")
        }
        assertContains(error.message.orEmpty(), "CONFSERVER-99908")
        assertContains(error.message.orEmpty().lowercase(), "admin")
    }

    @Test
    fun getAllSpacePermissionsServerRestHappyPath() = MockWebServer().use { server ->
        server.enqueue(
            json(
                """[
                    {"operation":{"targetType":"space","operationKey":"read"},"subject":{"type":"group","name":"readers"}},
                    {"operation":{"targetType":"space","operationKey":"read"},"subject":{"type":"user","userKey":"alice"}}
                ]""",
            ),
        )

        val result = loader().getAllSpacePermissionsServerRest(config(server, "\"is_cloud\":false"), credentials(), "ENG")

        assertEquals(2, result.size)
        assertEquals("/rest/api/space/ENG/permissions", server.takeRequest().requestUrl!!.encodedPath)
    }

    @Test
    fun getUserEmailFromUserkeyCachesLookups() = MockWebServer().use { server ->
        val loader = loader()
        var calls = 0
        server.dispatcher = userResolutionDispatcher(restrictedPages("userKey", "alice")) {
            calls += 1
            json("""{"email":"alice@example.com"}""")
        }

        val documents = loadRestricted(loader, server, "token-a")

        assertTrue(documents.all { it.externalAccess?.externalUserEmails == setOf("alice@example.com") })
        assertEquals(1, calls)
    }

    @Test
    fun getUserEmailFromUserkeyCachesNegativeResult() = MockWebServer().use { server ->
        val loader = loader()
        val calls = mutableMapOf<String, Int>()
        server.dispatcher = userResolutionDispatcher(restrictedPages("userKey", "missing")) { request ->
            val authorization = request.getHeader("Authorization").orEmpty()
            calls[authorization] = calls.getOrDefault(authorization, 0) + 1
            if (authorization.endsWith("token-a")) {
                MockResponse().setResponseCode(404)
            } else {
                json("""{"email":"recovered@example.com"}""")
            }
        }

        val first = loadRestricted(loader, server, "token-a")
        val second = loadRestricted(loader, server, "token-b")

        assertTrue(first.all { it.externalAccess?.numEntries == 0 })
        assertTrue(second.all { it.externalAccess?.externalUserEmails == setOf("recovered@example.com") })
        assertEquals(mapOf("Bearer token-a" to 1, "Bearer token-b" to 1), calls)
    }

    @Test
    fun paginatedCqlRetrievalHandlesPaginationError() = MockWebServer().use { server ->
        server.dispatcher = recoverablePaginationDispatcher(failAllRecoveryItems = false)

        val ids = loader().paginate(
            config(server, "\"is_cloud\":false"),
            credentials(),
            "/rest/api/content/search?cql=type%3Dpage",
            3,
        ).map { it.path("id").asInt() }

        assertEquals(listOf(1, 2, 3, 4, 6, 7), ids)
    }

    @Test
    fun paginatedCqlRetrievalSkipsCompletelyFailingPage() = MockWebServer().use { server ->
        server.dispatcher = recoverablePaginationDispatcher(failAllRecoveryItems = true)

        val ids = loader().paginate(
            config(server, "\"is_cloud\":false"),
            credentials(),
            "/rest/api/content/search?cql=type%3Dpage",
            3,
        ).map { it.path("id").asInt() }

        assertEquals(listOf(1, 2, 3, 7), ids)
    }

    @Test
    fun paginatedCqlRetrievalCloudReducesLimitOnError() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val limit = request.requestUrl!!.queryParameter("limit")!!.toInt()
                val start = request.requestUrl!!.queryParameter("start")
                return if (limit == 10 && start == null) {
                    json(
                        mapper.writeValueAsString(
                            mapOf(
                                "results" to (1..10).map { mapOf("id" to it) },
                                "_links" to mapOf("next" to "/rest/api/content/search?cql=type%3Dpage&limit=10&start=10"),
                            ),
                        ),
                    )
                } else {
                    MockResponse().setResponseCode(500)
                }
            }
        }

        assertFailsWith<WebClientResponseException> {
            loader().paginate(config(server, "\"is_cloud\":true"), credentials(), "/rest/api/content/search?cql=type%3Dpage", 10)
        }
        assertEquals(3, server.requestCount)
    }

    @Test
    fun paginateUrlReducesLimitOn504Cloud() = MockWebServer().use { server ->
        server.dispatcher = reducedLimitSuccessDispatcher(504)

        val ids = loader().paginate(
            config(server, "\"is_cloud\":true"),
            credentials(),
            "/rest/api/content/search?cql=type%3Dpage",
            20,
        ).map { it.path("id").asInt() }

        assertEquals(listOf(1, 2, 3), ids)
        assertEquals(3, server.requestCount)
    }

    @Test
    fun cqlPaginateAllExpansionsHandlesInternalPaginationError() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val path = request.path!!
                return when {
                    path.contains("content/search") -> json(
                        """{"results":[{"id":1,"children":{"results":[],"_links":{"next":"/rest/api/content/1/child?limit=3"}}}]}""",
                    )
                    path.contains("limit=3") && !path.contains("start=") -> MockResponse().setResponseCode(500)
                    path.contains("start=0") -> json("""{"results":[{"id":101}]}""")
                    path.contains("start=1") -> MockResponse().setResponseCode(500)
                    path.contains("start=2") -> json("""{"results":[{"id":103}]}""")
                    else -> json("""{"results":[]}""")
                }
            }
        }

        val item = loader().cqlPaginateAllExpansions(
            config(server, "\"is_cloud\":false"),
            credentials(),
            "type=page",
            "children",
            3,
        ).single()

        assertEquals(listOf(101, 103), item.path("children").path("results").map { it.path("id").asInt() })
    }

    @Test
    fun paginateUrlReducesLimitOn500Server() = MockWebServer().use { server ->
        server.dispatcher = reducedLimitSuccessDispatcher(500, includeNext = false)

        val ids = loader().paginate(
            config(server, "\"is_cloud\":false"),
            credentials(),
            "/rest/api/content/search?cql=type%3Dpage",
            20,
        ).map { it.path("id").asInt() }

        assertEquals(listOf(1, 2), ids)
        assertEquals(2, server.requestCount)
    }

    @Test
    fun paginateUrlServerFallsBackToOneByOneAfterLimitFloor() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val start = request.requestUrl!!.queryParameter("start")
                val limit = request.requestUrl!!.queryParameter("limit")
                return when {
                    limit == "5" -> MockResponse().setResponseCode(500)
                    start == "0" -> json("""{"results":[{"id":1}]}""")
                    start == "1" -> json("""{"results":[{"id":2}]}""")
                    else -> json("""{"results":[]}""")
                }
            }
        }

        val ids = loader().paginate(
            config(server, "\"is_cloud\":false"),
            credentials(),
            "/rest/api/content/search?cql=type%3Dpage",
            5,
        ).map { it.path("id").asInt() }

        assertEquals(listOf(1, 2), ids)
    }

    @Test
    fun paginateUrl504HalvesMultipleTimes() = MockWebServer().use { server ->
        val limits = mutableListOf<Int>()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val limit = request.requestUrl!!.queryParameter("limit")!!.toInt()
                limits += limit
                return if (limit > 5) MockResponse().setResponseCode(504) else json("""{"results":[{"id":99}]}""")
            }
        }

        val ids = loader().paginate(
            config(server, "\"is_cloud\":true"),
            credentials(),
            "/rest/api/content/search?cql=type%3Dpage",
            40,
        ).map { it.path("id").asInt() }

        assertEquals(listOf(99), ids)
        assertEquals(listOf(40, 20, 10, 5), limits)
    }

    @Test
    fun nonRateLimitError(): Unit = MockWebServer().use { server ->
        server.enqueue(MockResponse().setResponseCode(418).setBody("not rate limited"))
        val checkpoint = mapper.valueToTree<JsonNode>(
            ConfluenceCheckpoint(
                hasMore = true,
                nextPageUrl = "/rest/api/content/search?cql=type%3Dpage&start=2&limit=50",
            ),
        )

        assertFailsWith<WebClientResponseException> {
            loader().load(config(server), credentials(), checkpoint).toList()
        }
        assertEquals(1, server.requestCount)
    }

    @Test
    fun rateLimitHonorsRetryAfter() {
        val farFuture = DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC).plusYears(1))
        listOf(
            "0" to 2_000L,
            "5" to 5_000L,
            "999" to 60_000L,
            farFuture to 60_000L,
        ).forEach { (retryAfter, expectedDelay) ->
            MockWebServer().use { server ->
                server.enqueue(MockResponse().setResponseCode(429).setHeader("Retry-After", retryAfter))
                server.enqueue(json("""{"results":[{"id":1}]}"""))
                val delays = mutableListOf<Long>()
                val loader = loader().also { it.sleepMillis = delays::add }

                val result = loader.paginate(config(server), credentials(), "/rest/api/content/search", 50)

                assertEquals(1, result.single().path("id").asInt())
                assertEquals(listOf(expectedDelay), delays)
            }
        }
    }

    @Test
    fun slimDocsSkipImagesWhenAllowImagesFalse() = MockWebServer().use { server ->
        server.dispatcher = slimMixedAttachmentDispatcher(server)

        val documents = loader().retrieveAllSlimDocuments(
            config(server, "\"allow_images\":false"),
            credentials(),
        ).flatMap { it.documents.asSequence() }.toList()

        assertFalse(documents.any { it.title == "diagram.png" })
        assertTrue(documents.any { it.title == "spec.pdf" })
    }

    @Test
    fun slimDocsIncludeImagesWhenAllowImagesTrue() = MockWebServer().use { server ->
        server.dispatcher = slimMixedAttachmentDispatcher(server)

        val documents = loader().retrieveAllSlimDocuments(
            config(server, "\"allow_images\":true"),
            credentials(),
        ).flatMap { it.documents.asSequence() }.toList()

        assertTrue(documents.any { it.title == "diagram.png" })
        assertTrue(documents.any { it.title == "spec.pdf" })
    }

    private fun loader(): ConfluenceConnectorLoader =
        ConfluenceConnectorLoader(RemoteJsonClient(WebClient.builder()), mapper).also { it.sleepMillis = {} }

    private fun config(server: MockWebServer, extra: String = "") = config(
        server.startAndBase(),
        extra,
    )

    private fun config(base: String, extra: String = ""): JsonNode = mapper.readTree(
        """{"wiki_base":"$base"${if (extra.isBlank()) "" else ",$extra"}}""",
    )

    private fun credentials(token: String = "token"): JsonNode = mapper.readTree(
        """{"confluence_username":"user@example.com","confluence_access_token":"$token"}""",
    )

    private fun failure(url: String) = ConnectorFailure(FailureTarget.Document(url, url), "retry")

    private fun pageResponse(content: String = "<p>Run safely</p>"): String =
        mapper.writeValueAsString(mapOf("results" to listOf(mapper.readTree(page("111", content)))))

    private fun page(id: String, content: String = "<p>Run safely</p>"): String = """{
        "id":"$id",
        "title":"Runbook",
        "body":{"storage":{"value":${mapper.writeValueAsString(content)}}},
        "_links":{"webui":"/spaces/ENG/pages/$id/Runbook"},
        "space":{"key":"ENG","name":"Engineering"},
        "version":{"when":"2026-01-02T00:00:00Z","by":{"displayName":"Owner","email":"owner@example.com"}},
        "history":{"createdDate":"2026-01-01T00:00:00Z"},
        "metadata":{"labels":{"results":[{"name":"ops"}]}},
        "ancestors":[],
        "restrictions":{}
    }"""

    private fun pdfAttachment(): Map<String, Any> = mapOf(
        "id" to "att-222",
        "title" to "spec.pdf",
        "metadata" to mapOf("mediaType" to "application/pdf"),
        "extensions" to mapOf("fileSize" to 100),
        "_links" to mapOf(
            "download" to "/download/attachments/222/spec.pdf",
            "webui" to "/pages/viewpageattachments.action?pageId=222&preview=att-222",
        ),
        "space" to mapOf("key" to "ENG", "name" to "Engineering"),
        "version" to mapOf("when" to "2026-01-02T00:00:00Z"),
        "history" to mapOf("createdDate" to "2026-01-01T00:00:00Z"),
        "restrictions" to emptyMap<String, Any>(),
    )

    private fun imageAttachment(): Map<String, Any> = mapOf(
        "id" to "att-image",
        "title" to "diagram.png",
        "metadata" to mapOf("mediaType" to "image/png"),
        "extensions" to mapOf("fileSize" to 100),
        "_links" to mapOf(
            "download" to "/download/attachments/111/diagram.png",
            "webui" to "/pages/viewpageattachments.action?pageId=111&preview=att-image",
        ),
        "space" to mapOf("key" to "ENG", "name" to "Engineering"),
        "version" to mapOf("when" to "2026-01-02T00:00:00Z"),
        "history" to mapOf("createdDate" to "2026-01-01T00:00:00Z"),
        "restrictions" to emptyMap<String, Any>(),
    )

    private fun restrictions(users: List<String> = emptyList(), groups: List<String> = emptyList()): JsonNode = mapper.valueToTree(
        mapOf(
            "read" to mapOf(
                "restrictions" to mapOf(
                    "user" to mapOf("results" to users.map { mapOf("email" to it) }),
                    "group" to mapOf("results" to groups.map { mapOf("name" to it) }),
                ),
            ),
        ),
    )

    private fun restrictedPages(field: String, value: String): List<JsonNode> = listOf("1", "2").map { id ->
        mapper.readTree(page(id)).deepCopy<ObjectNode>().also { page ->
            page.set<JsonNode>(
                "restrictions",
                mapper.readTree(
                    """{"read":{"restrictions":{"user":{"results":[{"$field":"$value"}]},"group":{"results":[]}}}}""",
                ),
            )
        }
    }

    private fun userResolutionDispatcher(
        pages: List<JsonNode>,
        resolveUser: (RecordedRequest) -> MockResponse,
    ): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse = when (request.requestUrl!!.encodedPath) {
            "/rest/api/space" -> json("""{"results":[]}""")
            "/rest/api/user" -> resolveUser(request)
            else -> json(mapper.writeValueAsString(mapOf("results" to pages)))
        }
    }

    private fun loadRestricted(
        loader: ConfluenceConnectorLoader,
        server: MockWebServer,
        token: String,
    ): List<SourceDocument> = loader.load(
        config(
            server,
            "\"include_permissions\":true,\"include_comments\":false,\"include_attachments\":false",
        ),
        credentials(token),
        null,
    ).single().documents

    private fun loadWithoutExtras(
        loader: ConfluenceConnectorLoader,
        server: MockWebServer,
        token: String,
    ): List<SourceDocument> = loader.load(
        config(server, "\"include_comments\":false,\"include_attachments\":false"),
        credentials(token),
        null,
    ).single().documents

    private fun attachmentDispatcher(
        pages: String,
        attachments: List<Map<String, Any>>,
        server: MockWebServer,
    ): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse = when {
            request.path!!.contains("type%3Dattachment") ->
                json(mapper.writeValueAsString(mapOf("results" to attachments)))
            request.path!!.endsWith("/download") || request.path!!.contains("/download/") -> {
                val body = if (request.path!!.contains("diagram")) "image bytes" else "attachment text"
                MockResponse().setBody(body)
            }
            request.path!!.contains("type%3Dcomment") -> json("""{"results":[]}""")
            else -> json(pages)
        }
    }

    private fun permissionDispatcher(server: MockWebServer): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            val path = request.requestUrl!!.encodedPath
            return when {
                path == "/rest/api/space" -> json("""{"results":[{"key":"ENG","name":"Engineering"}]}""")
                path == "/rest/api/server-information" -> json("""{"version":"10.2.10"}""")
                path == "/rest/api/space/ENG/permissions" -> json(
                    """[{"operation":{"targetType":"space","operationKey":"read"},"subject":{"type":"group","name":"readers"}}]""",
                )
                path.endsWith("/permissions/anonymous") -> json("[]")
                request.path!!.contains("type%3Dcomment") || request.path!!.contains("type%3Dattachment") -> json("""{"results":[]}""")
                path.contains("content/search") -> json(pageResponse())
                else -> json("{}")
            }
        }
    }

    private fun reindexDispatcher(ids: Set<String>, server: MockWebServer): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            val pages = ids.map { mapper.readTree(page(it)) }
            return json(mapper.writeValueAsString(mapOf("results" to pages)))
        }
    }

    private fun reindexWithAttachmentDispatcher(server: MockWebServer, attachmentStatus: Int): Dispatcher =
        object : Dispatcher() {
            var attachmentPage = 0

            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path!!.contains("type%3Dpage") -> json(pageResponse())
                request.path!!.contains("type%3Dattachment") && attachmentPage++ == 0 -> json(
                    mapper.writeValueAsString(
                        mapOf(
                            "results" to listOf(pdfAttachment()),
                            "_links" to if (attachmentStatus == 400) {
                                mapOf("next" to "/rest/api/content/search?cql=type%3Dattachment&start=1&limit=50")
                            } else {
                                emptyMap()
                            },
                        ),
                    ),
                )
                request.path!!.contains("type%3Dattachment") -> MockResponse().setResponseCode(attachmentStatus)
                request.path!!.contains("/download/") -> MockResponse().setBody("attachment text")
                else -> json("""{"results":[]}""")
            }
        }

    private fun slimAttachmentDispatcher(server: MockWebServer, attachmentStatus: () -> Int): Dispatcher =
        object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path!!.contains("type%3Dattachment") -> {
                    val status = attachmentStatus()
                    if (status == 200) json(mapper.writeValueAsString(mapOf("results" to listOf(pdfAttachment()))))
                    else MockResponse().setResponseCode(status).setBody("attachment failure")
                }
                else -> json(pageResponse())
            }
        }

    private fun slimMixedAttachmentDispatcher(server: MockWebServer): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse =
            if (request.path!!.contains("type%3Dattachment")) {
                json(mapper.writeValueAsString(mapOf("results" to listOf(imageAttachment(), pdfAttachment()))))
            } else {
                json(pageResponse())
            }
    }

    private fun recoverablePaginationDispatcher(failAllRecoveryItems: Boolean): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            val start = request.requestUrl!!.queryParameter("start")?.toInt() ?: 0
            val limit = request.requestUrl!!.queryParameter("limit")?.toInt() ?: 3
            return when {
                start == 0 && limit == 3 -> json(
                    """{"results":[{"id":1},{"id":2},{"id":3}],"_links":{"next":"/rest/api/content/search?cql=type%3Dpage&limit=3&start=3"}}""",
                )
                start == 3 && limit == 3 -> MockResponse().setResponseCode(500)
                start in 3..5 && limit == 1 && (failAllRecoveryItems || start == 4) -> MockResponse().setResponseCode(500)
                start == 3 && limit == 1 -> json("""{"results":[{"id":4}]}""")
                start == 5 && limit == 1 -> json("""{"results":[{"id":6}]}""")
                start == 6 && limit == 3 -> json("""{"results":[{"id":7}]}""")
                else -> json("""{"results":[]}""")
            }
        }
    }

    private fun reducedLimitSuccessDispatcher(status: Int, includeNext: Boolean = true): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            val limit = request.requestUrl!!.queryParameter("limit")!!.toInt()
            val start = request.requestUrl!!.queryParameter("start")
            return when {
                limit == 20 -> MockResponse().setResponseCode(status)
                start == null -> json(
                    mapper.writeValueAsString(
                        mapOf(
                            "results" to listOf(mapOf("id" to 1), mapOf("id" to 2)),
                            "_links" to if (includeNext) {
                                mapOf("next" to "/rest/api/content/search?cql=type%3Dpage&limit=20&start=2")
                            } else {
                                emptyMap()
                            },
                        ),
                    ),
                )
                start == "2" -> json("""{"results":[{"id":3}]}""")
                else -> error("Unexpected request ${request.path}")
            }
        }
    }

    private fun assertRestrictionFetchReturnsNull(status: Int) = MockWebServer().use { server ->
        server.enqueue(MockResponse().setResponseCode(status))
        assertNull(loader().fetchContentReadRestrictions(config(server), credentials(), "999"))
    }

    private fun json(body: String): MockResponse =
        MockResponse().setHeader("Content-Type", "application/json").setBody(body)

    private fun MockWebServer.startAndBase(): String {
        if (port == -1) start()
        return url("/").toString().trimEnd('/')
    }

    private fun MockWebServer.takeRequests(count: Int): List<RecordedRequest> =
        (1..count).map { takeRequest() }

    private fun rewriteAtlassianRequestsTo(server: MockWebServer): ExchangeFilterFunction =
        ExchangeFilterFunction.ofRequestProcessor { request ->
            if (request.url().host != "api.atlassian.com") {
                reactor.core.publisher.Mono.just(request)
            } else {
                val local = server.url(request.url().rawPath + (request.url().rawQuery?.let { "?$it" } ?: ""))
                reactor.core.publisher.Mono.just(ClientRequest.from(request).url(URI.create(local.toString())).build())
            }
        }

    private companion object {
        const val CONFCLOUD_77618_BODY =
            "{\"statusCode\":404,\"errors\":[{\"message\":\"No content with id <ContentId{id=12345}> can be found\"}]}"
        const val CONFCLOUD_76424_BODY =
            "{\"message\":\"Cannot find content. Outdated version/old_draft/trashed? Please provide valid ContentId.\"}"
    }
}
