package com.onyx.foss.kotlin.support

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.annotation.DirtiesContext

const val H2_TEST_URL =
    "jdbc:h2:mem:onyx;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH"

@SpringBootTest(
    properties = [
        "spring.datasource.url=$H2_TEST_URL",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "onyx.worker.enabled=false",
        "onyx.worker.heartbeat-interval-ms=25",
        "onyx.storage.root=/tmp/onyx-kotlin-tests",
        "onyx.crypto.key=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
    ],
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
abstract class H2IntegrationTest {
    @Autowired
    private lateinit var h2Jdbc: JdbcTemplate

    protected val databaseUrl = H2_TEST_URL
    protected val databaseUsername = "sa"
    protected val databasePassword = ""

    protected fun truncateTables(vararg tables: String) {
        h2Jdbc.execute("SET REFERENTIAL_INTEGRITY FALSE")
        try {
            (ALWAYS_RESET_TABLES + tables).distinct().forEach {
                h2Jdbc.execute("TRUNCATE TABLE $it RESTART IDENTITY")
            }
        } finally {
            h2Jdbc.execute("SET REFERENTIAL_INTEGRITY TRUE")
        }
    }

    private companion object {
        val ALWAYS_RESET_TABLES = listOf(
            "ingestion_enumerated_documents",
            "file_assets",
        )
    }
}
