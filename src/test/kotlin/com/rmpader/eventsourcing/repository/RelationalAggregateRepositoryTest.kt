package com.rmpader.eventsourcing.repository

import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RelationalAggregateRepositoryTest {
    private lateinit var repository: RelationalAggregateRepository<TestEvent, TestState>

    @BeforeEach
    fun setup() {
        TestDatabase.initializeSchema()
        repository =
            RelationalAggregateRepository(
                TestDatabase.createR2dbcConnectionFactory(),
                TestEventSerializer(),
                TestStateSerializer(),
            )
    }

    @AfterEach
    fun teardown() {
        TestDatabase.cleanDatabase()
    }

    @Test
    fun `storeEvent should persist event to journal and outbox transactionally`() =
        runTest {
            // Given
            val entityId = "test-entity-1"
            val event = TestEvent("test-event", 42)
            val eventRecord =
                AggregateRepository.EventRecord(
                    entityId = entityId,
                    event = event,
                    sequenceNumber = 1L,
                    timestamp = OffsetDateTime.now(ZoneOffset.UTC),
                )

            // When
            repository.storeEvent(eventRecord)

            // Then
            TestDatabase.assertEventJournalContains(
                entityId = entityId,
                eventData = "test-event|42",
                sequenceNumber = 1L,
            )

            TestDatabase.assertEventOutboxContains(
                eventId = "$entityId|1",
                eventData = "test-event|42",
            )

            TestDatabase.assertEventJournalCount(1L)
            TestDatabase.assertEventOutboxCount(1L)
        }

    @Test
    fun `storeEvent should rollback transaction on duplicate persist`() =
        runTest {
            // Given
            val entityId = "test-entity-duplicate"
            repository.storeEvent(
                AggregateRepository.EventRecord(
                    entityId,
                    TestEvent("event-1", 1),
                    1L,
                    OffsetDateTime.now(ZoneOffset.UTC),
                ),
            )
            TestDatabase.assertEventJournalCount(1L)
            TestDatabase.assertEventOutboxCount(1L)

            // When
            try {
                repository.storeEvent(
                    AggregateRepository.EventRecord(
                        entityId,
                        TestEvent("event-duplicate", 2),
                        1L,
                        OffsetDateTime.now(ZoneOffset.UTC),
                    ),
                )
                throw AssertionError("Expected duplicate key exception")
            } catch (e: Exception) {
                assertTrue(
                    e.message?.contains("duplicate", ignoreCase = true) == true ||
                        e.message?.contains("unique", ignoreCase = true) == true ||
                        e.message?.contains("constraint", ignoreCase = true) == true,
                )
            }

            // Then
            TestDatabase.assertEventJournalCount(1L)
            TestDatabase.assertEventOutboxCount(1L)

            TestDatabase.assertEventJournalContains(entityId, "event-1|1", 1L)
            TestDatabase.assertEventOutboxContains("$entityId|1", "event-1|1")
        }

    @Test
    fun `storeEvent should rollback when conflicting with an orphan journal`() =
        runTest {
            println("kotlinx.coroutines.debug = ${System.getProperty("kotlinx.coroutines.debug")}")
            // Given
            val entityId = "test-entity-orphan-journal"
            TestDatabase.insertJournalDirect(entityId, "orphan-event|99", 1L)

            TestDatabase.assertEventJournalCount(1L)
            TestDatabase.assertEventOutboxCount(0L)

            // When
            try {
                repository.storeEvent(
                    AggregateRepository.EventRecord(
                        entityId,
                        TestEvent("new-event", 1),
                        1L,
                        OffsetDateTime.now(ZoneOffset.UTC),
                    ),
                )
                throw AssertionError("Expected constraint violation due to existing journal entry")
            } catch (e: Exception) {
                assertTrue(e is EventSourcingRepositoryException)
                assertTrue(e.cause is R2dbcDataIntegrityViolationException)
            }

            // Then
            TestDatabase.assertEventJournalCount(1L)
            TestDatabase.assertEventOutboxCount(0L)

            // Original orphaned journal data intact
            TestDatabase.assertEventJournalContains(entityId, "orphan-event|99", 1L)
        }

    @Test
    fun `storeEvent should rollback when conflicting with an orphan outbox`() =
        runTest {
            // Given
            val entityId = "test-entity-orphan-outbox"
            TestDatabase.insertOutboxDirect("$entityId|1", "orphan-event|99")

            TestDatabase.assertEventJournalCount(0L)
            TestDatabase.assertEventOutboxCount(1L)

            // When
            try {
                repository.storeEvent(
                    AggregateRepository.EventRecord(
                        entityId,
                        TestEvent("new-event", 1),
                        1L,
                        OffsetDateTime.now(ZoneOffset.UTC),
                    ),
                )
                throw AssertionError("Expected constraint violation due to existing outbox entry")
            } catch (e: Exception) {
                assertTrue(e is EventSourcingRepositoryException)
                assertTrue(e.cause is R2dbcDataIntegrityViolationException)
            }

            // Then
            TestDatabase.assertEventJournalCount(0L)
            TestDatabase.assertEventOutboxCount(1L)

            // Original orphaned outbox data intact
            TestDatabase.assertEventOutboxContains("$entityId|1", "orphan-event|99")
        }

    @Test
    fun `loadEvents should return empty flow when no events exist`() =
        runTest {
            // Given nothing inserted
            val entityId = "non-existent"

            // When
            val events = repository.loadEvents(entityId, 1L).toList()

            // Then
            assertEquals(0, events.size)
            TestDatabase.assertEventJournalCount(0L)
        }

    @Test
    fun `loadEvents should return events ordered by sequence number`() =
        runTest {
            // Given
            val entityId = "test-entity-2"
            TestDatabase.insertJournalDirect(entityId, "event-3|3", 3L)
            TestDatabase.insertJournalDirect(entityId, "event-2|2", 2L)
            TestDatabase.insertJournalDirect(entityId, "event-1|1", 1L)

            // When
            val events = repository.loadEvents(entityId, 1L).toList()

            // Then
            assertEquals(3, events.size)
            assertEquals("event-1", events[0].event.name)
            assertEquals(1, events[0].event.value)
            assertEquals(1L, events[0].sequenceNumber)

            assertEquals("event-2", events[1].event.name)
            assertEquals(2, events[1].event.value)
            assertEquals(2L, events[1].sequenceNumber)

            assertEquals("event-3", events[2].event.name)
            assertEquals(3, events[2].event.value)
            assertEquals(3L, events[2].sequenceNumber)
        }

    @Test
    fun `storeEvent should persist multiple events for same entity`() =
        runTest {
            // Given
            val entityId = "test-entity-5"

            // When
            repository.storeEvent(
                AggregateRepository.EventRecord(
                    entityId,
                    TestEvent("event-1", 1),
                    1L,
                    OffsetDateTime.now(ZoneOffset.UTC),
                ),
            )
            repository.storeEvent(
                AggregateRepository.EventRecord(
                    entityId,
                    TestEvent("event-2", 2),
                    2L,
                    OffsetDateTime.now(ZoneOffset.UTC),
                ),
            )
            repository.storeEvent(
                AggregateRepository.EventRecord(
                    entityId,
                    TestEvent("event-3", 3),
                    3L,
                    OffsetDateTime.now(ZoneOffset.UTC),
                ),
            )

            // Then
            TestDatabase.assertEventJournalCount(3L)
            TestDatabase.assertEventJournalContains(entityId, "event-1|1", 1L)
            TestDatabase.assertEventJournalContains(entityId, "event-2|2", 2L)
            TestDatabase.assertEventJournalContains(entityId, "event-3|3", 3L)
        }

    @Test
    fun `loadLatestSnapshot should return null when no snapshot exists`() =
        runTest {
            // Given
            val entityId = "non-existent"

            // When
            val snapshot = repository.loadLatestSnapshot(entityId)

            // Then
            assertNull(snapshot)
            TestDatabase.assertSnapshotCount(0)
        }

    @Test
    fun `deleteFromOutbox should delete only the specified event ID`() =
        runTest {
            // Given
            val entityId = "test-entity-2"
            val targetEventIds: List<String> = listOf("$entityId|3", "$entityId|2")
            TestDatabase.insertOutboxDirect("$entityId|4", "event-4|4")
            TestDatabase.insertOutboxDirect("$entityId|3", "event-3|3")
            TestDatabase.insertOutboxDirect("$entityId|2", "event-2|2")
            TestDatabase.insertOutboxDirect("$entityId|1", "event-1|1")

            // When
            repository.deleteFromOutbox(targetEventIds.toSet())

            // Then
            TestDatabase.assertEventOutboxCount(2L)
            TestDatabase.assertEventOutboxContains("$entityId|4", "event-4|4")
            TestDatabase.assertEventOutboxContains("$entityId|1", "event-1|1")
        }

    @Test
    fun `pollOutbox should return events up to the specified limit (10 of 3)`() =
        runTest {
            // Given
            val entityId = "test-entity-2"
            TestDatabase.insertOutboxDirect("$entityId|3", "event-3|3")
            TestDatabase.insertOutboxDirect("$entityId|2", "event-2|2")
            TestDatabase.insertOutboxDirect("$entityId|1", "event-1|1")

            // When
            val outboxRecords = repository.pollOutbox(10).toList().sortedBy { it.eventId }

            // Then
            assertEquals(3, outboxRecords.size)

            assertEquals("event-1", outboxRecords[0].event.name)
            assertEquals(1, outboxRecords[0].event.value)
            assertEquals("$entityId|1", outboxRecords[0].eventId)

            assertEquals("event-2", outboxRecords[1].event.name)
            assertEquals(2, outboxRecords[1].event.value)
            assertEquals("$entityId|2", outboxRecords[1].eventId)

            assertEquals("event-3", outboxRecords[2].event.name)
            assertEquals(3, outboxRecords[2].event.value)
            assertEquals("$entityId|3", outboxRecords[2].eventId)
        }

    @Test
    fun `pollOutbox should return events up to the specified limit (10 of 11)`() =
        runTest {
            // Given
            val entityId = "test-entity-2"
            TestDatabase.insertOutboxDirect("$entityId|11", "event-11|11")
            TestDatabase.insertOutboxDirect("$entityId|10", "event-10|10")
            TestDatabase.insertOutboxDirect("$entityId|9", "event-9|9")
            TestDatabase.insertOutboxDirect("$entityId|8", "event-8|8")
            TestDatabase.insertOutboxDirect("$entityId|7", "event-7|7")
            TestDatabase.insertOutboxDirect("$entityId|6", "event-6|6")
            TestDatabase.insertOutboxDirect("$entityId|5", "event-5|5")
            TestDatabase.insertOutboxDirect("$entityId|4", "event-4|4")
            TestDatabase.insertOutboxDirect("$entityId|3", "event-3|3")
            TestDatabase.insertOutboxDirect("$entityId|2", "event-2|2")
            TestDatabase.insertOutboxDirect("$entityId|1", "event-1|1")

            // When
            val outboxRecords = repository.pollOutbox(10).toList().sortedBy { it.eventId }

            // Then
            assertEquals(10, outboxRecords.size)

            // no need to assert mapping
        }

    @Test
    fun `pollOutbox should return events excluding locked entries`() =
        runTest {
            // Prepare
            val entityId = "test-entity-2"
            TestDatabase.insertOutboxDirect("$entityId|11", "event-11|11")
            TestDatabase.insertOutboxDirect("$entityId|10", "event-10|10")
            TestDatabase.insertOutboxDirect("$entityId|9", "event-9|9")
            TestDatabase.insertOutboxDirect("$entityId|8", "event-8|8")
            TestDatabase.insertOutboxDirect("$entityId|7", "event-7|7")
            TestDatabase.insertOutboxDirect("$entityId|6", "event-6|6")
            TestDatabase.lockRecordsThen {
                // Given
                TestDatabase.insertOutboxDirect("$entityId|5", "event-5|5")
                TestDatabase.insertOutboxDirect("$entityId|4", "event-4|4")
                TestDatabase.insertOutboxDirect("$entityId|3", "event-3|3")
                TestDatabase.insertOutboxDirect("$entityId|2", "event-2|2")
                TestDatabase.insertOutboxDirect("$entityId|1", "event-1|1")

                // When
                val outboxRecords = repository.pollOutbox(10).toList().sortedBy { it.eventId }

                // Then
                assertEquals(5, outboxRecords.size)

                assertEquals("event-1", outboxRecords[0].event.name)
                assertEquals(1, outboxRecords[0].event.value)
                assertEquals("$entityId|1", outboxRecords[0].eventId)

                assertEquals("event-2", outboxRecords[1].event.name)
                assertEquals(2, outboxRecords[1].event.value)
                assertEquals("$entityId|2", outboxRecords[1].eventId)

                assertEquals("event-3", outboxRecords[2].event.name)
                assertEquals(3, outboxRecords[2].event.value)
                assertEquals("$entityId|3", outboxRecords[2].eventId)

                assertEquals("event-4", outboxRecords[3].event.name)
                assertEquals(4, outboxRecords[3].event.value)
                assertEquals("$entityId|4", outboxRecords[3].eventId)

                assertEquals("event-5", outboxRecords[4].event.name)
                assertEquals(5, outboxRecords[4].event.value)
                assertEquals("$entityId|5", outboxRecords[4].eventId)
            }
        }

    @Test
    fun `pollOutbox should return nothing when all records are locked`() =
        runTest {
            // Prepare
            val entityId = "test-entity-2"
            TestDatabase.insertOutboxDirect("$entityId|5", "event-5|5")
            TestDatabase.insertOutboxDirect("$entityId|4", "event-4|4")
            TestDatabase.insertOutboxDirect("$entityId|3", "event-3|3")
            TestDatabase.insertOutboxDirect("$entityId|2", "event-2|2")
            TestDatabase.insertOutboxDirect("$entityId|1", "event-1|1")
            TestDatabase.lockRecordsThen {
                // Given
                // nothing

                // When
                val outboxRecords = repository.pollOutbox(10).toList().sortedBy { it.eventId }

                // Then
                assertEquals(0, outboxRecords.size)
            }
        }

    @Test
    fun `pollOutbox should return nothing when all records are claimed`() =
        runTest {
            // Given
            val entityId = "test-entity-2"
            TestDatabase.insertOutboxDirect("$entityId|5", "event-5|5")
            TestDatabase.insertOutboxDirect("$entityId|4", "event-4|4")
            TestDatabase.insertOutboxDirect("$entityId|3", "event-3|3")
            TestDatabase.insertOutboxDirect("$entityId|2", "event-2|2")
            TestDatabase.insertOutboxDirect("$entityId|1", "event-1|1")
            repository.pollOutbox(10).toList().sortedBy { it.eventId }

            // When
            val outboxRecords = repository.pollOutbox(10).toList().sortedBy { it.eventId }

            // Then
            assertEquals(0, outboxRecords.size)
        }
}
