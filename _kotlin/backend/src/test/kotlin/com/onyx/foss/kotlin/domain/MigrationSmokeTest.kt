package com.onyx.foss.kotlin.domain

import com.onyx.foss.kotlin.support.H2IntegrationTest
import org.assertj.core.api.Assertions.assertThat
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import java.util.UUID

class MigrationSmokeTest : H2IntegrationTest() {
    @Autowired
    lateinit var jdbc: JdbcTemplate

    @Test
    fun `flyway creates the current schema`() {
        val tables = jdbc.queryForList(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            String::class.java,
        )
        assertThat(tables).contains("ingestion_jobs", "indexed_documents")
        assertThat(
            jdbc.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'ingestion_attempts' AND column_name = 'prune_only')",
                Boolean::class.java,
            ),
        ).isTrue()
    }

    @Test
    fun `existing V7 data migrates through V15 and duplicate active jobs are cleaned`() {
        val schema = "migration_${UUID.randomUUID().toString().replace("-", "")}"
        jdbc.execute("CREATE SCHEMA $schema")
        try {
            flyway(schema, MigrationVersion.fromVersion("7")).migrate()
            val legacy = JdbcTemplate(
                DriverManagerDataSource(scopedDatabaseUrl(schema), databaseUsername, databasePassword),
            )
            legacy.update(
                "INSERT INTO credentials(id, source, secret_json) VALUES (101, 'file', 'encrypted')",
            )
            legacy.update(
                "INSERT INTO connectors(id, name, source, input_type) VALUES (101, 'legacy', 'file', 'load_state')",
            )
            legacy.update(
                """
                    INSERT INTO connector_credential_pairs(id, connector_id, credential_id, name)
                    VALUES (101, 101, 101, 'legacy')
                """.trimIndent(),
            )
            legacy.update(
                """
                    INSERT INTO ingestion_attempts(id, cc_pair_id, status)
                    VALUES (201, 101, 'IN_PROGRESS'), (202, 101, 'IN_PROGRESS'), (203, 101, 'NOT_STARTED')
                """.trimIndent(),
            )
            legacy.update(
                """
                    INSERT INTO ingestion_jobs(id, attempt_id, state)
                    VALUES (301, 201, 'QUEUED'), (302, 202, 'RUNNING'), (303, 203, 'QUEUED')
                """.trimIndent(),
            )
            legacy.update(
                """
                    INSERT INTO indexed_documents(cc_pair_id, source_document_id, title, content_hash)
                    VALUES (101, 'legacy-document', 'Legacy', 'hash')
                """.trimIndent(),
            )
            legacy.update(
                "INSERT INTO permission_sync_attempts(cc_pair_id) VALUES (101)",
            )
            legacy.update(
                "INSERT INTO document_set_sync_outbox(cc_pair_ids) VALUES ('[101]')",
            )

            flyway(schema).migrate()

            assertThat(
                legacy.queryForObject(
                    "SELECT COUNT(*) FROM ingestion_jobs WHERE state IN ('QUEUED', 'RUNNING')",
                    Int::class.java,
                ),
            ).isEqualTo(1)
            assertThat(
                legacy.queryForObject(
                    "SELECT id FROM ingestion_jobs WHERE state IN ('QUEUED', 'RUNNING')",
                    Long::class.java,
                ),
            ).isEqualTo(302L)
            assertThat(
                legacy.queryForList(
                    "SELECT id FROM ingestion_jobs WHERE state = 'FAILED' ORDER BY id",
                    Long::class.java,
                ),
            ).containsExactly(301L, 303L)
            assertThat(
                legacy.queryForList(
                    """
                        SELECT id FROM ingestion_attempts
                        WHERE status = 'FAILED' AND error_msg = 'Superseded by the active job migration'
                        ORDER BY id
                    """.trimIndent(),
                    Long::class.java,
                ),
            ).containsExactly(201L, 203L)
            assertThat(
                legacy.queryForList("SELECT DISTINCT cc_pair_id FROM ingestion_jobs", Long::class.java),
            ).containsExactly(101L)
            assertThat(
                legacy.queryForObject(
                    "SELECT enumeration_complete FROM ingestion_attempts WHERE id = 202",
                    Boolean::class.java,
                ),
            ).isFalse()
            assertThat(
                legacy.queryForObject(
                    "SELECT claim_token IS NULL AND lease_expires_at IS NULL FROM ingestion_jobs WHERE id = 302",
                    Boolean::class.java,
                ),
            ).isTrue()
            assertThat(
                legacy.queryForObject(
                    "SELECT CAST(primary_owners AS VARCHAR) || CAST(secondary_owners AS VARCHAR) " +
                        "FROM indexed_documents WHERE source_document_id = 'legacy-document'",
                    String::class.java,
                ),
            ).isEqualTo("[][]")
            assertThat(
                legacy.queryForObject(
                    "SELECT follow_up_requested FROM permission_sync_attempts",
                    Boolean::class.java,
                ),
            ).isFalse()
            assertThat(
                legacy.queryForObject(
                    "SELECT document_set_ids IS NULL FROM document_set_sync_outbox",
                    Boolean::class.java,
                ),
            ).isTrue()
            assertThat(
                legacy.queryForObject("SELECT deleting FROM connectors WHERE id = 101", Boolean::class.java),
            ).isFalse()
        } finally {
            jdbc.execute("DROP SCHEMA $schema CASCADE")
        }
    }

    private fun flyway(schema: String, target: MigrationVersion? = null): Flyway {
        val configuration = Flyway.configure()
            .dataSource(scopedDatabaseUrl(schema), databaseUsername, databasePassword)
            .locations("classpath:db/migration")
            .schemas(schema)
        if (target != null) configuration.target(target)
        return configuration.load()
    }

    private fun scopedDatabaseUrl(schema: String): String {
        return "$databaseUrl;SCHEMA=$schema"
    }
}
