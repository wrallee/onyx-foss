package com.onyx.foss.kotlin.domain

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ConnectorSourceTest {
    @Test
    fun acceptsOnlyEnabledFossSources() {
        assertEquals(ConnectorSource.FILE, ConnectorSource.fromValue("file"))
        assertEquals(ConnectorSource.GITHUB, ConnectorSource.fromValue("GITHUB"))
        assertFailsWith<IllegalArgumentException> { ConnectorSource.fromValue("slack") }
    }
}
