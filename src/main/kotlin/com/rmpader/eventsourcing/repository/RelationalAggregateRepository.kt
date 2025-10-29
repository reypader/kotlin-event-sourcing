package com.rmpader.evensourcing

import com.rmpader.com.rmpader.eventsourcing.EventSerializer
import com.rmpader.com.rmpader.eventsourcing.repository.EventSourcingRepositoryException
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
import java.util.UUID

class RelationalAggregateRepository<E, S>(
    val connectionFactory: ConnectionFactory,
    val eventSerializer: EventSerializer<E>,
    val stateSerializer: EventSerializer<S>
) : AggregateRepository<E, S> {

    override fun loadEvents(
        entityId: String,
        fromSequenceNumber: Long
    ): Flow<AggregateRepository.EventRecord<E>> {
        try {
            return Flux.usingWhen(
                Mono.from(connectionFactory.create()),
                { connection ->
                    Flux.from(
                        connection.createStatement(
                            """
                        SELECT ENTITY_ID, EVENT_DATA, SEQUENCE_NUMBER, TIMESTAMP
                        FROM EVENT_JOURNAL
                        WHERE ENTITY_ID = $1 AND SEQUENCE_NUMBER >= $2
                        ORDER BY SEQUENCE_NUMBER ASC
                        """
                        )
                            .bind(0, entityId)
                            .bind(1, fromSequenceNumber)
                            .execute()
                    )
                        .flatMap { result ->
                            result.map { row, metadata ->
                                AggregateRepository.EventRecord(
                                    entityId = row.get("ENTITY_ID", String::class.java)!!,
                                    event = eventSerializer.deserialize(
                                        row.get("EVENT_DATA", String::class.java)!!
                                    ),
                                    sequenceNumber = row.get("SEQUENCE_NUMBER", Number::class.java)!!.toLong(),
                                    timestamp = row.get("TIMESTAMP", java.time.LocalDateTime::class.java)!!
                                        .atOffset(ZoneOffset.UTC)
                                )
                            }
                        }
                },
                { connection -> Mono.from(connection.close()) },
                { connection, error -> Mono.from(connection.close()) },
                { connection -> Mono.from(connection.close()) }
            ).asFlow()
        } catch (ex: Exception) {
            throw EventSourcingRepositoryException(ex)
        }
    }

    override suspend fun loadLatestSnapshot(entityId: String): AggregateRepository.SnapshotRecord<S>? {
        try {
            return Mono.usingWhen(
                Mono.from(connectionFactory.create()),
                { connection ->
                    Mono.from(
                        connection.createStatement(
                            """
                        SELECT ENTITY_ID, SEQUENCE_NUMBER, STATE_DATA, TIMESTAMP
                        FROM SNAPSHOTS
                        WHERE ENTITY_ID = $1
                        ORDER BY SEQUENCE_NUMBER DESC LIMIT 1
                        """
                        )
                            .bind(0, entityId)
                            .execute()
                    )
                        .flatMap { result ->
                            Mono.from(
                                result.map { row, metadata ->
                                    AggregateRepository.SnapshotRecord(
                                        entityId = row.get("ENTITY_ID", String::class.java)!!,
                                        state = stateSerializer.deserialize(
                                            row.get("STATE_DATA", String::class.java)!!
                                        ),
                                        sequenceNumber = row.get("SEQUENCE_NUMBER", Number::class.java)!!.toLong(),
                                        timestamp = row.get("TIMESTAMP", java.time.LocalDateTime::class.java)!!
                                            .atOffset(ZoneOffset.UTC)
                                    )
                                }
                            )
                        }
                },
                { connection -> Mono.from(connection.close()) },
                { connection, error -> Mono.from(connection.close()) },
                { connection -> Mono.from(connection.close()) }
            ).awaitSingleOrNull()
        } catch (ex: Exception) {
            throw EventSourcingRepositoryException(ex)
        }
    }

    override suspend fun storeEvent(eventRecord: AggregateRepository.EventRecord<E>) {
        try {
            Mono.usingWhen(
                Mono.from(connectionFactory.create()),
                { connection ->
                    Mono.from(connection.beginTransaction())
                        .then(write { persistEventStore(connection, eventRecord) })
                        .then(write { persistOutbox(connection, eventRecord) })
                        .then(Mono.from(connection.commitTransaction()))
                        .then(Mono.just(Unit))
                },
                { connection -> Mono.from(connection.close()) },
                { connection, error ->
                    Mono.from(connection.rollbackTransaction())
                        .then(Mono.from(connection.close()))
                },
                { connection -> Mono.from(connection.close()) }
            ).awaitSingle()
        } catch (e: Exception) {
            throw EventSourcingRepositoryException(e)
        }
    }

    private fun write(
        block: () -> Mono<Result>
    ): Mono<Unit> {
        return block().flatMap { Mono.from(it.rowsUpdated) }
            .flatMap { rowsAffected ->
                if (rowsAffected != 1L) {
                    Mono.error(
                        IllegalStateException("Expected 1 row inserted into event_journal, but got $rowsAffected")
                    )
                } else {
                    Mono.empty()
                }
            }
    }

    private fun persistOutbox(
        connection: Connection,
        eventRecord: AggregateRepository.EventRecord<E>
    ): Mono<Result> {
        return Mono.from(
            connection.createStatement(
                """
                            INSERT INTO EVENT_OUTBOX 
                            (ENTITY_ID, SEQUENCE_NUMBER, EVENT_DATA, EVENT_ID)
                            VALUES ($1, $2, $3, $4)
                        """
            )
                .bind(0, eventRecord.entityId)
                .bind(1, eventRecord.sequenceNumber)
                .bind(2, eventSerializer.serialize(eventRecord.event))
                .bind(3, UUID.randomUUID().toString())
                .execute()
        )

    }

    private fun persistEventStore(
        connection: Connection,
        eventRecord: AggregateRepository.EventRecord<E>
    ): Mono<Result> {
        return Mono.from(
            connection.createStatement(
                """
                            INSERT INTO EVENT_JOURNAL 
                            (ENTITY_ID, SEQUENCE_NUMBER, EVENT_DATA)
                            VALUES ($1, $2, $3)
                        """
            )
                .bind(0, eventRecord.entityId)
                .bind(1, eventRecord.sequenceNumber)
                .bind(2, eventSerializer.serialize(eventRecord.event))
                .execute()
        )

    }


}