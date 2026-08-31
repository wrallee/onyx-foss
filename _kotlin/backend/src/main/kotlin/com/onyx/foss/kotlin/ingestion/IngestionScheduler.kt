package com.onyx.foss.kotlin.ingestion

import com.onyx.foss.kotlin.config.OnyxProperties
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.service.AdminService
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.time.Instant

@Component
class IngestionScheduler(
    private val properties: OnyxProperties,
    private val pairs: ConnectorCredentialPairRepository,
    private val admin: AdminService,
) {
    @Scheduled(fixedDelayString = "\${onyx.worker.poll-delay-ms:1000}")
    @Transactional
    fun schedule() {
        if (properties.worker.enabled) scheduleDue(Instant.now())
    }

    @Transactional
    fun scheduleDue(now: Instant) {
        pairs.findSchedulable().forEach { pair ->
            val pruneDue = pair.pruneFreq?.let { frequency ->
                pair.lastPrunedAt?.plusSeconds(frequency)?.isAfter(now) != true
            } == true
            val refreshDue = pair.refreshFreq?.let { frequency ->
                pair.lastAttemptAt?.plusSeconds(frequency)?.isAfter(now) != true
            } == true
            when {
                pruneDue -> admin.enqueuePair(pair.pairId, fromBeginning = false, pruneOnly = true)
                refreshDue -> admin.enqueuePair(pair.pairId, fromBeginning = false)
            }
        }
    }
}
