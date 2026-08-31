package com.onyx.foss.kotlin.domain

import com.onyx.foss.kotlin.support.PostgresIntegrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate

class SchemaV6IntegrationTest : PostgresIntegrationTest() {
    @Autowired private lateinit var jdbc: JdbcTemplate

    @Test
    fun v6AddsLeaseAndSingletonClaimLock() {
        val lockedAt = jdbc.queryForObject(
            """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'document_set_sync_outbox'
                      AND column_name = 'locked_at'
                )
            """.trimIndent(),
            Boolean::class.java,
        )
        val lockRows = jdbc.queryForObject(
            "SELECT COUNT(*) FROM document_set_sync_claim_lock WHERE id = 1",
            Long::class.java,
        )

        assertThat(lockedAt).isTrue()
        assertThat(lockRows).isEqualTo(1)
    }
}
