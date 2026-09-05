package com.onyx.foss.kotlin.domain

import com.onyx.foss.kotlin.support.H2IntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import kotlin.test.assertTrue

class SchemaV2IntegrationTest : H2IntegrationTest() {
    @Autowired
    lateinit var jdbc: JdbcTemplate

    @Test
    fun v2AddsIngestionAndPermissionSchema() {
        assertTrue(hasColumn("indexed_documents", "external_access"))
        assertTrue(hasColumn("ingestion_errors", "entity_id"))
        assertTrue(hasColumn("ingestion_errors", "failed_time_range_start"))
        assertTrue(hasColumn("ingestion_errors", "failed_time_range_end"))
        kotlin.test.assertFalse(hasTable("permission_sync_attempts"))
    }

    private fun hasColumn(table: String, column: String): Boolean =
        jdbc.queryForObject(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = ? AND column_name = ?
            )
            """.trimIndent(),
            Boolean::class.java,
            table,
            column,
        ) ?: false

    private fun hasTable(table: String): Boolean =
        jdbc.queryForObject(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = ?
            )
            """.trimIndent(),
            Boolean::class.java,
            table,
        ) ?: false
}
