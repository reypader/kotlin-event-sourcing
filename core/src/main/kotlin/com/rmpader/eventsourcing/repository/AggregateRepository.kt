package com.rmpader.eventsourcing.repository

import kotlinx.coroutines.flow.Flow
import java.time.OffsetDateTime

interface AggregateRepository<E, S> {
    data class EventRecord<E>(
        val entityId: String,
        val event: E,
        val sequenceNumber: Long,
        val timestamp: OffsetDateTime,
    )

    data class OutboxRecord<E>(
        val eventId: String,
        val event: E,
    )

    data class SnapshotRecord<S>(
        val entityId: String,
        val state: S,
        val sequenceNumber: Long,
        val timestamp: OffsetDateTime,
    )

    fun loadEvents(
        entityId: String,
        fromSequenceNumber: Long,
    ): Flow<EventRecord<E>>

    suspend fun loadLatestSnapshot(entityId: String): SnapshotRecord<S>?

    suspend fun storeEvent(eventRecord: EventRecord<E>)

    suspend fun deleteFromOutbox(eventIds: Set<String>)

    suspend fun pollOutbox(limit: Int): Flow<OutboxRecord<E>>

    suspend fun cleanupStaleOutboxClaims(staleAfterMillis: Long): Long
}
