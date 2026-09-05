package com.onyx.foss.kotlin.domain

import com.onyx.foss.kotlin.support.H2IntegrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate

class SchemaV4IntegrationTest : H2IntegrationTest() {
    @Autowired private lateinit var jdbc: JdbcTemplate

    @Test
    fun v16DropsPermissionStagingAndAttempts() {
        assertThat(hasTable("permission_sync_staging")).isFalse()
        assertThat(hasTable("permission_sync_attempts")).isFalse()
    }

    private fun hasColumn(table: String, column: String): Boolean = jdbc.queryForObject(
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

    private fun hasTable(table: String): Boolean = jdbc.queryForObject(
        """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = ?
            )
        """.trimIndent(),
        Boolean::class.java,
        table,
    ) ?: false

    private fun primaryKeyColumns(table: String): List<String> = jdbc.queryForList(
        """
            SELECT key_column_usage.column_name
            FROM information_schema.table_constraints
            JOIN information_schema.key_column_usage
              ON table_constraints.constraint_name = key_column_usage.constraint_name
             AND table_constraints.table_schema = key_column_usage.table_schema
            WHERE table_constraints.table_schema = 'public'
              AND table_constraints.table_name = ?
              AND table_constraints.constraint_type = 'PRIMARY KEY'
            ORDER BY key_column_usage.ordinal_position
        """.trimIndent(),
        String::class.java,
        table,
    ).filterNotNull()
}
