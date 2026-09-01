package com.onyx.foss.kotlin.domain

import com.onyx.foss.kotlin.support.H2IntegrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate

class SchemaV7IntegrationTest : H2IntegrationTest() {
    @Autowired private lateinit var jdbc: JdbcTemplate

    @Test
    fun v7AddsClaimToken() {
        val dataType = jdbc.queryForObject(
            """
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'document_set_sync_outbox'
                  AND column_name = 'claim_token'
            """.trimIndent(),
            String::class.java,
        )

        assertThat(dataType).isEqualTo("uuid")
    }
}
