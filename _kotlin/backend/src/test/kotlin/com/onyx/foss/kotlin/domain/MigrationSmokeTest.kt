package com.onyx.foss.kotlin.domain

import com.onyx.foss.kotlin.support.PostgresIntegrationTest
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate

class MigrationSmokeTest : PostgresIntegrationTest() {
    @Autowired
    lateinit var jdbc: JdbcTemplate

    @Test
    fun `flyway creates the current schema`() {
        val tables = jdbc.queryForList(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            String::class.java,
        )
        assertTrue("ingestion_jobs" in tables)
        assertTrue("indexed_documents" in tables)
        assertTrue(
            jdbc.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'ingestion_attempts' AND column_name = 'prune_only')",
                Boolean::class.java,
            ) == true,
        )
    }
}
