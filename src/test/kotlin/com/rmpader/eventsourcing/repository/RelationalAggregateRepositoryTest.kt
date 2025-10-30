package com.rmpader.eventsourcing.repository

import com.rmpader.com.rmpader.eventsourcing.repository.EventSourcingRepositoryException
import com.rmpader.evensourcing.AggregateRepository
import com.rmpader.evensourcing.RelationalAggregateRepository
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
            // Given some event to be persisted
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
                entityId = entityId,
                eventData = "test-event|42",
                sequenceNumber = 1L,
                processed = false,
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
            TestDatabase.assertEventOutboxContains(entityId, "event-1|1", 1L, false)
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
                println(e.stackTraceToString())
                assertTrue(e is EventSourcingRepositoryException)
                assertTrue(e.cause is R2dbcDataIntegrityViolationException)
            }

            // Then orphaned journal entry still exists, no outbox entry created
            TestDatabase.assertEventJournalCount(1L)
            TestDatabase.assertEventOutboxCount(0L)

            // Original orphaned journal data intact
            TestDatabase.assertEventJournalContains(entityId, "orphan-event|99", 1L)
        }

    @Test
    fun `storeEvent should rollback when conflicting with an orphan outbox`() =
        runTest {
            // Given orphaned outbox entry (inconsistent state)
            val entityId = "test-entity-orphan-outbox"
            TestDatabase.insertOutboxDirect(entityId, "orphan-event|99", 1L)

            // Verify inconsistent state: outbox has entry, journal doesn't
            TestDatabase.assertEventJournalCount(0L)
            TestDatabase.assertEventOutboxCount(1L)

            // When attempt to store event with same sequence number
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
                // Expected outbox constraint violation
                assertTrue(
                    e.message?.contains("duplicate", ignoreCase = true) == true ||
                        e.message?.contains("unique", ignoreCase = true) == true ||
                        e.message?.contains("constraint", ignoreCase = true) == true,
                )
            }

            // Then orphaned outbox entry still exists, no journal entry created
            TestDatabase.assertEventJournalCount(0L)
            TestDatabase.assertEventOutboxCount(1L)

            // Original orphaned outbox data intact
            TestDatabase.assertEventOutboxContains(entityId, "orphan-event|99", 1L, false)
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
            // Given insert using JDBC
            val entityId = "test-entity-2"
            TestDatabase.insertJournalDirect(entityId, "event-3|3", 3L)
            TestDatabase.insertJournalDirect(entityId, "event-2|2", 2L)
            TestDatabase.insertJournalDirect(entityId, "event-1|1", 1L)

            // When use R2DBC repository to load
            val events = repository.loadEvents(entityId, 1L).toList()

            // Then verify repository returned correct data
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

            // Then assert exact database state
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
}
