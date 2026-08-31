package com.onyx.foss.kotlin.domain

import com.onyx.foss.kotlin.support.PostgresIntegrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate

class SchemaV5IntegrationTest : PostgresIntegrationTest() {
    @Autowired private lateinit var jdbc: JdbcTemplate

    @Test
    fun v5AddsDocumentSetSyncOutbox() {
        val columns = jdbc.queryForList(
            """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'document_set_sync_outbox'
                ORDER BY ordinal_position
            """.trimIndent(),
            String::class.java,
        )

        assertThat(columns).contains(
            "id",
            "cc_pair_ids",
            "status",
            "attempt_count",
            "last_error",
            "created_at",
            "updated_at",
        )
    }
}
