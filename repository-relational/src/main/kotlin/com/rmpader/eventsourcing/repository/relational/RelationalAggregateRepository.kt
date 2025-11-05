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
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
@Suppress("SqlSourceToSinkFlow")
class RelationalAggregateRepository<E, S>(
    private val tablePrefix: String,
    private val connectionFactory: ConnectionFactory,
    private val serializer: Serializer<E>,
    private val stateSerializer: Serializer<S>,
) : AggregateRepository<E, S> {
    init {
        require(tablePrefix.isNotBlank()) { "tablePrefix must not be blank" }
        require(tablePrefix.matches(Regex("^[a-zA-Z_][a-zA-Z0-9_]*$"))) { "tablePrefix must be a valid identifier" }
    }

    private suspend inline fun <R> useConnection(crossinline block: suspend (Connection) -> R): R {
        try {
            val connection = connectionFactory.create().awaitSingle()
            try {
                return block(connection)
            } finally {
                connection.close().awaitFirstOrNull()
            }
        } catch (ex: EventSourcingRepositoryException) {
            throw ex
        } catch (ex: Exception) {
            throw EventSourcingRepositoryException(ex)
        }
    }

    private inline fun <R> flowConnection(crossinline block: suspend (Connection) -> Flow<R>) =
        flow {
            val connection = connectionFactory.create().awaitSingle()
            block(connection)
                .onCompletion { cause ->
                    connection.close().awaitFirstOrNull()
                    if (cause != null) {
                        logger.error("Error while using connection to generate flow", cause)
                    }
                }.collect { emit(it) }
        }.catch { e ->
            throw EventSourcingRepositoryException(e)
        }

    override fun loadEvents(
        aggregateKey: AggregateKey,
        fromSequenceNumber: Long,
    ): Flow<AggregateRepository.EventRecord<E>> =
        flowConnection {
            it
                .createStatement(
                    """
                        SELECT ENTITY_ID, EVENT_DATA, SEQUENCE_NUMBER, TIMESTAMP, ORIGIN_COMMAND_ID
                        FROM ${tablePrefix}_EVENT_JOURNAL
                        WHERE ENTITY_ID = $1 AND SEQUENCE_NUMBER >= $2
                        ORDER BY SEQUENCE_NUMBER ASC
                        """,
                ).bind("$1", aggregateKey.toString())
                .bind("$2", fromSequenceNumber)
                .execute()
                .asFlow()
                .flatMapConcat { result ->
                    result
                        .map { row, _ -> row }
                        .asFlow()
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
        }

    override suspend fun loadLatestSnapshot(aggregateKey: AggregateKey): AggregateRepository.SnapshotRecord<S>? =
        useConnection {
            val result =
                it
                    .createStatement(
                        """
                        SELECT ENTITY_ID, SEQUENCE_NUMBER, STATE_DATA, TIMESTAMP
                        FROM ${tablePrefix}_SNAPSHOTS
                        WHERE ENTITY_ID = $1
                        ORDER BY SEQUENCE_NUMBER DESC LIMIT 1
                        """,
                    ).bind("$1", aggregateKey.toString())
                    .execute()
                    .awaitSingle()
                    .map { row, _ -> row }
                    .awaitFirstOrNull()

            result?.let { row ->
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
        }

    override suspend fun storeEvent(eventRecord: AggregateRepository.EventRecord<E>) =
        useConnection {
            try {
                it.beginTransaction().awaitFirstOrNull()
                persistEventStore(it, eventRecord)
                persistOutbox(it, eventRecord)
                it.commitTransaction().awaitFirstOrNull()
                return@useConnection
            } catch (ex: Exception) {
                it.rollbackTransaction().awaitFirstOrNull()
                throw EventSourcingRepositoryException(ex)
            }
        }

    private suspend fun persistOutbox(
        connection: Connection,
        eventRecord: AggregateRepository.EventRecord<E>,
    ) {
        val result =
            connection
                .createStatement(
                    """
                            INSERT INTO ${tablePrefix}_EVENT_OUTBOX 
                            (EVENT_ID, EVENT_DATA)
                            VALUES ($1, $2)
                        """,
                ).bind("$1", "${eventRecord.aggregateKey}|${eventRecord.sequenceNumber}")
                .bind("$2", serializer.serialize(eventRecord.event))
                .execute()
                .awaitSingle()
        val rowsAffected = result.rowsUpdated.awaitSingle()
        if (rowsAffected != 1L) {
            error("Expected 1 row affected in event_outbox, but got $rowsAffected")
        }
    }

    private suspend fun persistEventStore(
        connection: Connection,
        eventRecord: AggregateRepository.EventRecord<E>,
    ) {
        val result =
            connection
                .createStatement(
                    """
                            INSERT INTO ${tablePrefix}_EVENT_JOURNAL 
                            (ENTITY_ID, SEQUENCE_NUMBER, EVENT_DATA, ORIGIN_COMMAND_ID)
                            VALUES ($1, $2, $3, $4)
                        """,
                ).bind("$1", eventRecord.aggregateKey.toString())
                .bind("$2", eventRecord.sequenceNumber)
                .bind("$3", serializer.serialize(eventRecord.event))
                .bind("$4", eventRecord.originCommandId)
                .execute()
                .awaitSingle()
        val rowsAffected = result.rowsUpdated.awaitSingle()
        if (rowsAffected != 1L) {
            error("Expected 1 row affected in event_journal, but got $rowsAffected")
        }
    }

    override suspend fun deleteFromOutbox(eventIds: Set<String>) =
        useConnection { connection ->
            if (eventIds.isNotEmpty()) {
                val sql = """
                        DELETE 
                        FROM ${tablePrefix}_EVENT_OUTBOX
                        WHERE EVENT_ID IN (${eventIds.indices.joinToString(",") { $$"$$${it + 1}" }})
                        """

                val statement = connection.createStatement(sql)
                eventIds.forEachIndexed { index, id ->
                    statement.bind(index, id)
                }
                val result = statement.execute().awaitSingle()
                val rowsAffected = result.rowsUpdated.awaitSingle()
                val expectedRows = eventIds.size.toLong()
                if (rowsAffected != expectedRows) {
                    logger.warn("Expected $expectedRows rows deleted from event_outbox, but got $rowsAffected")
                }
            }
        }

    override fun pollOutbox(limit: Long): Flow<AggregateRepository.OutboxRecord<E>> =
        flowConnection {
            val claimId = UUID.randomUUID().toString()
            claimRecords(it, claimId, limit)
            extractRecords(it, claimId)
        }

    private suspend fun claimRecords(
        connection: Connection,
        claimId: String,
        limit: Long,
    ) {
        try {
            connection.beginTransaction().awaitFirstOrNull()
            val result =
                connection
                    .createStatement(
                        """
                                    UPDATE ${tablePrefix}_EVENT_OUTBOX
                                    SET CLAIM_ID = $1, CLAIMED_AT = CURRENT_TIMESTAMP
                                    WHERE EVENT_ID IN (
                                        SELECT EVENT_ID
                                        FROM ${tablePrefix}_EVENT_OUTBOX
                                        WHERE CLAIM_ID IS NULL
                                        ORDER BY CREATED_AT ASC
                                        LIMIT $2
                                        FOR UPDATE SKIP LOCKED
                                    )
                                    """,
                    ).bind("$1", claimId)
                    .bind("$2", limit)
                    .execute()
                    .awaitSingle()
            val rowsAffected = result.rowsUpdated.awaitSingle()
            if (rowsAffected > limit) {
                error("Expected 1 row affected in event_journal, but got $rowsAffected")
            }
            connection.commitTransaction().awaitFirstOrNull()
        } catch (ex: Exception) {
            connection.rollbackTransaction().awaitFirstOrNull()
            throw EventSourcingRepositoryException(ex)
        }
    }

    private fun extractRecords(
        connection: Connection,
        claimId: String,
    ): Flow<AggregateRepository.OutboxRecord<E>> =
        connection
            .createStatement(
                """
                                    SELECT EVENT_ID, EVENT_DATA
                                    FROM ${tablePrefix}_EVENT_OUTBOX
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

    override suspend fun cleanupStaleOutboxClaims(staleAfterMillis: Long): Long =
        useConnection {
            it
                .createStatement(
                    """
                            UPDATE ${tablePrefix}_EVENT_OUTBOX
                            SET CLAIM_ID = NULL, CLAIMED_AT = NULL
                            WHERE CLAIM_ID IS NOT NULL
                            AND CLAIMED_AT < DATEADD('MILLISECOND', -$1, CURRENT_TIMESTAMP)
                            """,
                ).bind("$1", staleAfterMillis)
                .execute()
                .awaitSingle()
                .rowsUpdated
                .awaitSingle()
        }

    companion object {
        private val logger = LoggerFactory.getLogger(RelationalAggregateRepository::class.java)
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
