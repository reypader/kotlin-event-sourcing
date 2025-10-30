package com.rmpader.eventsourcing.repository

import com.rmpader.eventsourcing.EventSerializer
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Result
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.ZoneOffset

class RelationalAggregateRepository<E, S>(
    val connectionFactory: ConnectionFactory,
    val eventSerializer: EventSerializer<E>,
    val stateSerializer: EventSerializer<S>,
) : AggregateRepository<E, S> {
    override fun loadEvents(
        entityId: String,
        fromSequenceNumber: Long,
    ): Flow<AggregateRepository.EventRecord<E>> {
        try {
            return Flux
                .usingWhen(
                    Mono.from(connectionFactory.create()),
                    { connection ->
                        Flux
                            .from(
                                connection
                                    .createStatement(
                                        """
                        SELECT ENTITY_ID, EVENT_DATA, SEQUENCE_NUMBER, TIMESTAMP
                        FROM EVENT_JOURNAL
                        WHERE ENTITY_ID = $1 AND SEQUENCE_NUMBER >= $2
                        ORDER BY SEQUENCE_NUMBER ASC
                        """,
                                    ).bind(0, entityId)
                                    .bind(1, fromSequenceNumber)
                                    .execute(),
                            ).flatMap { result ->
                                result.map { row, _ ->
                                    AggregateRepository.EventRecord(
                                        entityId = row.get("ENTITY_ID", String::class.java)!!,
                                        event =
                                            eventSerializer.deserialize(
                                                row.get("EVENT_DATA", String::class.java)!!,
                                            ),
                                        sequenceNumber = row.get("SEQUENCE_NUMBER", Number::class.java)!!.toLong(),
                                        timestamp =
                                            row
                                                .get("TIMESTAMP", java.time.LocalDateTime::class.java)!!
                                                .atOffset(ZoneOffset.UTC),
                                    )
                                }
                            }
                    },
                    { connection -> Mono.from(connection.close()) },
                    { connection, _ -> Mono.from(connection.close()) },
                    { connection -> Mono.from(connection.close()) },
                ).asFlow()
        } catch (ex: Exception) {
            throw EventSourcingRepositoryException(ex)
        }
    }

    override suspend fun loadLatestSnapshot(entityId: String): AggregateRepository.SnapshotRecord<S>? {
        try {
            return Mono
                .usingWhen(
                    Mono.from(connectionFactory.create()),
                    { connection ->
                        Mono
                            .from(
                                connection
                                    .createStatement(
                                        """
                        SELECT ENTITY_ID, SEQUENCE_NUMBER, STATE_DATA, TIMESTAMP
                        FROM SNAPSHOTS
                        WHERE ENTITY_ID = $1
                        ORDER BY SEQUENCE_NUMBER DESC LIMIT 1
                        """,
                                    ).bind(0, entityId)
                                    .execute(),
                            ).flatMap { result ->
                                Mono.from(
                                    result.map { row, _ ->
                                        AggregateRepository.SnapshotRecord(
                                            entityId = row.get("ENTITY_ID", String::class.java)!!,
                                            state =
                                                stateSerializer.deserialize(
                                                    row.get("STATE_DATA", String::class.java)!!,
                                                ),
                                            sequenceNumber = row.get("SEQUENCE_NUMBER", Number::class.java)!!.toLong(),
                                            timestamp =
                                                row
                                                    .get("TIMESTAMP", java.time.LocalDateTime::class.java)!!
                                                    .atOffset(ZoneOffset.UTC),
                                        )
                                    },
                                )
                            }
                    },
                    { connection -> Mono.from(connection.close()) },
                    { connection, _ -> Mono.from(connection.close()) },
                    { connection -> Mono.from(connection.close()) },
                ).awaitSingleOrNull()
        } catch (ex: Exception) {
            throw EventSourcingRepositoryException(ex)
        }
    }

    override suspend fun storeEvent(eventRecord: AggregateRepository.EventRecord<E>) {
        try {
            Mono
                .usingWhen(
                    Mono.from(connectionFactory.create()),
                    { connection ->
                        Mono
                            .from(connection.beginTransaction())
                            .then(write { persistEventStore(connection, eventRecord) })
                            .then(write { persistOutbox(connection, eventRecord) })
                            .then(Mono.from(connection.commitTransaction()))
                            .then(Mono.just(Unit))
                    },
                    { connection -> Mono.from(connection.close()) },
                    { connection, _ ->
                        Mono
                            .from(connection.rollbackTransaction())
                            .then(Mono.from(connection.close()))
                    },
                    { connection -> Mono.from(connection.close()) },
                ).awaitSingle()
        } catch (e: Exception) {
            throw EventSourcingRepositoryException(e)
        }
    }

    override suspend fun deleteFromOutbox(eventIds: Set<String>) {
        try {
            if (eventIds.isEmpty()) return

            Mono
                .usingWhen(
                    Mono.from(connectionFactory.create()),
                    { connection ->
                        val sql = """
                        DELETE 
                        FROM EVENT_OUTBOX
                        WHERE EVENT_ID IN (${eventIds.indices.joinToString(",") { $$"$$${it + 1}" }})
                        """

                        @Suppress("SqlSourceToSinkFlow")
                        val statement = connection.createStatement(sql)
                        eventIds.forEachIndexed { index, id ->
                            statement.bind(index, id)
                        }

                        write(eventIds.size.toLong()) {
                            Mono.from(statement.execute())
                        }.then(Mono.just(Unit))
                    },
                    { connection -> Mono.from(connection.close()) },
                    { connection, _ -> Mono.from(connection.close()) },
                    { connection -> Mono.from(connection.close()) },
                ).awaitSingle()
        } catch (e: Exception) {
            throw EventSourcingRepositoryException(e)
        }
    }

    override suspend fun pollOutbox(limit: Int): Flow<AggregateRepository.OutboxRecord<E>> {
        try {
            val claimId =
                java.util.UUID
                    .randomUUID()
                    .toString()

            return Flux
                .usingWhen(
                    Mono.from(connectionFactory.create()),
                    { connection ->
                        Mono
                            .from(connection.beginTransaction())
                            .then(claimRecords(claimId, limit, connection))
                            .then(Mono.from(connection.commitTransaction()))
                            .thenMany(extractRecords(claimId, connection))
                    },
                    { connection -> Mono.from(connection.close()) },
                    { connection, _ ->
                        Mono
                            .from(connection.rollbackTransaction())
                            .then(Mono.from(connection.close()))
                    },
                    { connection -> Mono.from(connection.close()) },
                ).asFlow()
        } catch (ex: Exception) {
            throw EventSourcingRepositoryException(ex)
        }
    }

    override suspend fun cleanupStaleOutboxClaims(staleAfterMillis: Long): Long {
        try {
            return Mono
                .usingWhen(
                    Mono.from(connectionFactory.create()),
                    { connection ->
                        Mono
                            .from(
                                connection
                                    .createStatement(
                                        """
                            UPDATE EVENT_OUTBOX
                            SET CLAIM_ID = NULL, CLAIMED_AT = NULL
                            WHERE CLAIM_ID IS NOT NULL
                            AND CLAIMED_AT < DATEADD('MILLISECOND', -$1, CURRENT_TIMESTAMP)
                            """,
                                    ).bind(0, staleAfterMillis)
                                    .execute(),
                            ).flatMap { Mono.from(it.rowsUpdated) }
                    },
                    { connection -> Mono.from(connection.close()) },
                    { connection, _ -> Mono.from(connection.close()) },
                    { connection -> Mono.from(connection.close()) },
                ).awaitSingle()
        } catch (e: Exception) {
            throw EventSourcingRepositoryException(e)
        }
    }

    private fun extractRecords(
        claimId: String,
        connection: Connection,
    ): Flux<AggregateRepository.OutboxRecord<E>> =
        Flux
            .from(
                connection
                    .createStatement(
                        """
                                    SELECT EVENT_ID, EVENT_DATA
                                    FROM EVENT_OUTBOX
                                    WHERE CLAIM_ID = $1
                                    """,
                    ).bind(0, claimId)
                    .execute(),
            ).flatMap { result ->
                result.map { row, _ ->
                    AggregateRepository.OutboxRecord(
                        eventId = row.get("EVENT_ID", String::class.java)!!,
                        event =
                            eventSerializer.deserialize(
                                row.get("EVENT_DATA", String::class.java)!!,
                            ),
                    )
                }
            }

    private fun claimRecords(
        claimId: String,
        limit: Int,
        connection: Connection,
    ): Mono<Unit> =
        Mono
            .from(
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
                    ).bind(0, claimId)
                    .bind(1, limit)
                    .execute(),
            ).flatMap { Mono.from(it.rowsUpdated) }
            .then(Mono.empty())

    private fun write(
        expectedRows: Long = 1L,
        block: () -> Mono<Result>,
    ): Mono<Unit> =
        block()
            .flatMap { Mono.from(it.rowsUpdated) }
            .flatMap { rowsAffected ->
                if (rowsAffected != expectedRows) {
                    Mono.error(
                        IllegalStateException("Expected $expectedRows row/s affected in event_journal, but got $rowsAffected"),
                    )
                } else {
                    Mono.empty()
                }
            }

    private fun persistOutbox(
        connection: Connection,
        eventRecord: AggregateRepository.EventRecord<E>,
    ): Mono<Result> =
        Mono.from(
            connection
                .createStatement(
                    """
                            INSERT INTO EVENT_OUTBOX 
                            (EVENT_ID, EVENT_DATA)
                            VALUES ($1, $2)
                        """,
                ).bind(0, "${eventRecord.entityId}|${eventRecord.sequenceNumber}")
                .bind(1, eventSerializer.serialize(eventRecord.event))
                .execute(),
        )

    private fun persistEventStore(
        connection: Connection,
        eventRecord: AggregateRepository.EventRecord<E>,
    ): Mono<Result> =
        Mono.from(
            connection
                .createStatement(
                    """
                            INSERT INTO EVENT_JOURNAL 
                            (ENTITY_ID, SEQUENCE_NUMBER, EVENT_DATA)
                            VALUES ($1, $2, $3)
                        """,
                ).bind(0, eventRecord.entityId)
                .bind(1, eventRecord.sequenceNumber)
                .bind(2, eventSerializer.serialize(eventRecord.event))
                .execute(),
        )
}
