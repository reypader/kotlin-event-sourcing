package com.rmpader.evensourcing

import kotlinx.coroutines.flow.Flow
import java.time.OffsetDateTime

interface AggregateRepository<E, S> {
    data class EventRecord<E>(
        val entityId: String,
        val event: E,
        val sequenceNumber: Long,
        val timestamp: OffsetDateTime,
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

    suspend fun deleteFromOutbox(eventId: String)
}
