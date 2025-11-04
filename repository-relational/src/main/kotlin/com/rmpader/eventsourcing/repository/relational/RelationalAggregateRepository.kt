package com.rmpader.eventsourcing.repository.relational

import com.rmpader.eventsourcing.AggregateKey
import com.rmpader.eventsourcing.Serializer
import com.rmpader.eventsourcing.repository.AggregateRepository
import com.rmpader.eventsourcing.repository.EventSourcingRepositoryException
import io.r2dbc.spi.Blob
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
class RelationalAggregateRepository<E, S>(
    val connectionFactory: ConnectionFactory,
    val serializer: Serializer<E>,
    val stateSerializer: Serializer<S>,
) : AggregateRepository<E, S> {
    override fun loadEvents(
        aggregateKey: AggregateKey,
        fromSequenceNumber: Long,
    ): Flow<AggregateRepository.EventRecord<E>> {
        try {
            return connectionFactory
                .create()
                .asFlow()
                .flatMapConcat {
                    it
                        .createStatement(
                            """
                        SELECT ENTITY_ID, EVENT_DATA, SEQUENCE_NUMBER, TIMESTAMP, ORIGIN_COMMAND_ID
                        FROM EVENT_JOURNAL
                        WHERE ENTITY_ID = $1 AND SEQUENCE_NUMBER >= $2
                        ORDER BY SEQUENCE_NUMBER ASC
                        """,
                        ).bind("$1", aggregateKey.toString())
                        .bind("$2", fromSequenceNumber)
                        .execute()
                        .asFlow()
                }.flatMapConcat {
                    it
                        .map { row, _ ->
                            row
                        }.asFlow()
                        .map { row ->
                            AggregateRepository.EventRecord(
                                aggregateKey = AggregateKey.from(row.get("ENTITY_ID", String::class.java)!!),
                                event =
                                    serializer.deserialize(
                                        row.get("EVENT_DATA", Blob::class.java)!!.toByteArray(),
                                    ),
                                sequenceNumber = row.get("SEQUENCE_NUMBER", Number::class.java)!!.toLong(),
                                timestamp =
                                    row
                                        .get("TIMESTAMP", LocalDateTime::class.java)!!
                                        .atOffset(ZoneOffset.UTC),
                                originCommandId = row.get("ORIGIN_COMMAND_ID", String::class.java)!!,
                            )
                        }
                }
        } catch (ex: Exception) {
            throw EventSourcingRepositoryException(ex)
        }
    }

    override suspend fun loadLatestSnapshot(aggregateKey: AggregateKey): AggregateRepository.SnapshotRecord<S>? {
        try {
            return connectionFactory
                .create()
                .asFlow()
                .flatMapConcat {
                    it
                        .createStatement(
                            """
                        SELECT ENTITY_ID, SEQUENCE_NUMBER, STATE_DATA, TIMESTAMP
                        FROM SNAPSHOTS
                        WHERE ENTITY_ID = $1
                        ORDER BY SEQUENCE_NUMBER DESC LIMIT 1
                        """,
                        ).bind("$1", aggregateKey.toString())
                        .execute()
                        .asFlow()
                }.flatMapConcat {
                    it
                        .map { row, _ ->
                            row
                        }.asFlow()
                        .map { row ->
                            AggregateRepository.SnapshotRecord(
                                aggregateKey =
                                    AggregateKey.from(
                                        row.get(
                                            "ENTITY_ID",
                                            String::class.java,
                                        )!!,
                                    ),
                                state =
                                    stateSerializer.deserialize(
                                        row.get("STATE_DATA", Blob::class.java)!!.toByteArray(),
                                    ),
                                sequenceNumber = row.get("SEQUENCE_NUMBER", Number::class.java)!!.toLong(),
                                timestamp =
                                    row
                                        .get("TIMESTAMP", LocalDateTime::class.java)!!
                                        .atOffset(ZoneOffset.UTC),
                            )
                        }
                }.singleOrNull()
        } catch (ex: Exception) {
            throw EventSourcingRepositoryException(ex)
        }
    }

    override suspend fun storeEvent(eventRecord: AggregateRepository.EventRecord<E>) {
        try {
            connectionFactory
                .create()
                .asFlow()
                .onEach { it.beginTransaction().awaitFirstOrNull() }
                .onEach { persistEventStore(it, eventRecord) }
                .onEach { persistOutbox(it, eventRecord) }
                .single()
                .commitTransaction()
                .awaitFirstOrNull()
        } catch (ex: EventSourcingRepositoryException) {
            throw ex
        } catch (e: Exception) {
            throw EventSourcingRepositoryException(e)
        }
    }

    private suspend fun persistOutbox(
        connection: Connection,
        eventRecord: AggregateRepository.EventRecord<E>,
    ) {
        try {
            val rowsAffected =
                connection
                    .createStatement(
                        """
                            INSERT INTO EVENT_OUTBOX 
                            (EVENT_ID, EVENT_DATA)
                            VALUES ($1, $2)
                        """,
                    ).bind("$1", "${eventRecord.aggregateKey}|${eventRecord.sequenceNumber}")
                    .bind("$2", serializer.serialize(eventRecord.event))
                    .execute()
                    .asFlow()
                    .flatMapConcat { result -> result.rowsUpdated.asFlow() }
                    .single()
            if (rowsAffected != 1L) {
                error("Expected 1 row affected in event_outbox, but got $rowsAffected")
            }
        } catch (ex: Exception) {
            connection.rollbackTransaction().awaitFirstOrNull()
            throw EventSourcingRepositoryException(ex)
        }
    }

    private suspend fun persistEventStore(
        connection: Connection,
        eventRecord: AggregateRepository.EventRecord<E>,
    ) {
        try {
            val rowsAffected =
                connection
                    .createStatement(
                        """
                            INSERT INTO EVENT_JOURNAL 
                            (ENTITY_ID, SEQUENCE_NUMBER, EVENT_DATA, ORIGIN_COMMAND_ID)
                            VALUES ($1, $2, $3, $4)
                        """,
                    ).bind("$1", eventRecord.aggregateKey.toString())
                    .bind("$2", eventRecord.sequenceNumber)
                    .bind("$3", serializer.serialize(eventRecord.event))
                    .bind("$4", eventRecord.originCommandId)
                    .execute()
                    .asFlow()
                    .flatMapConcat { result -> result.rowsUpdated.asFlow() }
                    .single()
            if (rowsAffected != 1L) {
                error("Expected 1 row affected in event_journal, but got $rowsAffected")
            }
        } catch (ex: Exception) {
            connection.rollbackTransaction().awaitFirstOrNull()
            throw EventSourcingRepositoryException(ex)
        }
    }

    override suspend fun deleteFromOutbox(eventIds: Set<String>) {
        try {
            if (eventIds.isEmpty()) return

            val sql = """
                        DELETE 
                        FROM EVENT_OUTBOX
                        WHERE EVENT_ID IN (${eventIds.indices.joinToString(",") { $$"$$${it + 1}" }})
                        """
            val rowsAffected =
                connectionFactory
                    .create()
                    .asFlow()
                    .flatMapConcat {
                        @Suppress("SqlSourceToSinkFlow")
                        val statement = it.createStatement(sql)
                        eventIds.forEachIndexed { index, id ->
                            statement.bind(index, id)
                        }
                        statement
                            .execute()
                            .asFlow()
                            .flatMapConcat { result -> result.rowsUpdated.asFlow() }
                    }.single()
            val expectedRows = eventIds.size.toLong()
            if (rowsAffected != expectedRows) {
                logger.warn("Expected $expectedRows rows deleted from event_outbox, but got $rowsAffected")
            }
        } catch (e: Exception) {
            throw EventSourcingRepositoryException(e)
        }
    }

    override suspend fun pollOutbox(limit: Long): Flow<AggregateRepository.OutboxRecord<E>> {
        try {
            val claimId = UUID.randomUUID().toString()

            return connectionFactory
                .create()
                .asFlow()
                .onEach { it.beginTransaction().awaitFirstOrNull() }
                .onEach { claimRecords(it, claimId, limit) }
                .onEach { it.commitTransaction().awaitFirstOrNull() }
                .flatMapConcat { extractRecords(it, claimId) }
        } catch (ex: EventSourcingRepositoryException) {
            throw ex
        } catch (ex: Exception) {
            throw EventSourcingRepositoryException(ex)
        }
    }

    private suspend fun claimRecords(
        connection: Connection,
        claimId: String,
        limit: Long,
    ) {
        try {
            val rowsAffected =
                connection
                    .createStatement(
                        """
                                    UPDATE EVENT_OUTBOX
                                    SET CLAIM_ID = $1, CLAIMED_AT = CURRENT_TIMESTAMP
                                    WHERE EVENT_ID IN (
                                        SELECT EVENT_ID
                                        FROM EVENT_OUTBOX
                                        WHERE CLAIM_ID IS NULL
                                        ORDER BY CREATED_AT ASC
                                        LIMIT $2
                                        FOR UPDATE SKIP LOCKED
                                    )
                                    """,
                    ).bind("$1", claimId)
                    .bind("$2", limit)
                    .execute()
                    .asFlow()
                    .flatMapConcat { result -> result.rowsUpdated.asFlow() }
                    .single()
            if (rowsAffected > limit) {
                error("Expected 1 row affected in event_journal, but got $rowsAffected")
            }
        } catch (ex: Exception) {
            connection.rollbackTransaction().awaitFirstOrNull()
            throw EventSourcingRepositoryException(ex)
        }
    }

    private fun extractRecords(
        connection: Connection,
        claimId: String,
    ): Flow<AggregateRepository.OutboxRecord<E>> {
        try {
            return connection
                .createStatement(
                    """
                                    SELECT EVENT_ID, EVENT_DATA
                                    FROM EVENT_OUTBOX
                                    WHERE CLAIM_ID = $1
                                    """,
                ).bind("$1", claimId)
                .execute()
                .asFlow()
                .flatMapConcat {
                    it
                        .map { row, _ ->
                            row
                        }.asFlow()
                        .map { row ->
                            AggregateRepository.OutboxRecord(
                                eventId = row.get("EVENT_ID", String::class.java)!!,
                                event =
                                    serializer.deserialize(
                                        row.get("EVENT_DATA", Blob::class.java)!!.toByteArray(),
                                    ),
                            )
                        }
                }
        } catch (ex: Exception) {
            throw EventSourcingRepositoryException(ex)
        }
    }

    override suspend fun cleanupStaleOutboxClaims(staleAfterMillis: Long): Long {
        try {
            return connectionFactory
                .create()
                .asFlow()
                .flatMapConcat {
                    it
                        .createStatement(
                            """
                            UPDATE EVENT_OUTBOX
                            SET CLAIM_ID = NULL, CLAIMED_AT = NULL
                            WHERE CLAIM_ID IS NOT NULL
                            AND CLAIMED_AT < DATEADD('MILLISECOND', -$1, CURRENT_TIMESTAMP)
                            """,
                        ).bind("$1", staleAfterMillis)
                        .execute()
                        .asFlow()
                }.flatMapConcat { result -> result.rowsUpdated.asFlow() }
                .single()
        } catch (e: Exception) {
            throw EventSourcingRepositoryException(e)
        }
    }

    suspend fun Blob.toByteArray(): ByteArray =
        this
            .stream()
            .asFlow()
            .fold(ByteArray(0)) { acc, buffer ->
                val bytes = ByteArray(buffer.remaining())
                buffer.get(bytes)
                acc + bytes
            }

    companion object {
        private val logger = LoggerFactory.getLogger(RelationalAggregateRepository::class.java)
    }
}
