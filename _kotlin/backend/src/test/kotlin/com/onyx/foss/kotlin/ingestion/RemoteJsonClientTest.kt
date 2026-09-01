package com.onyx.foss.kotlin.ingestion

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RemoteJsonClientTest {
    @Test
    fun connectorRequestTimeoutIsBelowPermissionLease() {
        assertThat(REMOTE_CONNECTOR_TIMEOUT).isLessThan(PERMISSION_SYNC_LEASE)
    }
}
