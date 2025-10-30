package com.rmpader.eventsourcing.repository

import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
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
                assertTrue(e is EventSourcingRepositoryException)
                assertTrue(e.cause is R2dbcDataIntegrityViolationException)
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
            TestDatabase.insertUnclaimedOutboxDirect("$entityId|1", "orphan-event|99")

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
            (1L..3L).forEach {
                TestDatabase.insertJournalDirect(entityId, "event-$it|$it", it)
            }

            // When
            val events = repository.loadEvents(entityId, 1L).toList()

            // Then
            assertEquals(3, events.size)
            (1..3).forEach {
                assertEquals("event-$it", events[it - 1].event.name)
                assertEquals(it, events[it - 1].event.value)
                assertEquals(it.toLong(), events[it - 1].sequenceNumber)
            }
        }

    @Test
    fun `loadEvents should filter by fromSequenceNumber`() =
        runTest {
            val entityId = "test-entity"
            (1L..3L).forEach {
                TestDatabase.insertJournalDirect(entityId, "event-$it|$it", it)
            }

            // Should only return events 2 and 3
            val events = repository.loadEvents(entityId, 2L).toList()

            assertEquals(2, events.size)
            assertEquals(2L, events[0].sequenceNumber)
            assertEquals(3L, events[1].sequenceNumber)
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
            (1L..3L).forEach {
                TestDatabase.assertEventJournalContains(entityId, "event-$it|$it", it)
            }
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
    fun `loadLatestSnapshot should return latest snapshot`() =
        runTest {
            val entityId = "test-entity"
            TestDatabase.insertSnapshotDirect(entityId, "status-1|1", 1L)
            TestDatabase.insertSnapshotDirect(entityId, "status-5|5", 5L)
            TestDatabase.insertSnapshotDirect(entityId, "status-3|3", 3L)

            val snapshot = repository.loadLatestSnapshot(entityId)

            assertNotNull(snapshot)
            assertEquals(5L, snapshot.sequenceNumber)
            assertEquals("status-5", snapshot.state.status)
            assertEquals(5, snapshot.state.count)
        }

    @Test
    fun `deleteFromOutbox should delete only the specified event ID`() =
        runTest {
            // Given
            val entityId = "test-entity-2"
            val targetEventIds: List<String> = listOf("$entityId|3", "$entityId|2")
            (1L..4L).forEach {
                TestDatabase.insertUnclaimedOutboxDirect("$entityId|$it", "event-$it|$it")
            }

            // When
            repository.deleteFromOutbox(targetEventIds.toSet())

            // Then
            TestDatabase.assertEventOutboxCount(2L)
            TestDatabase.assertEventOutboxContains("$entityId|4", "event-4|4")
            TestDatabase.assertEventOutboxContains("$entityId|1", "event-1|1")
        }

    @Test
    fun `deleteFromOutbox should handle empty set`() =
        runTest {
            TestDatabase.insertUnclaimedOutboxDirect("test|1", "event-1|1")

            repository.deleteFromOutbox(emptySet())

            TestDatabase.assertEventOutboxCount(1L)
        }

    @Test
    fun `deleteFromOutbox should delete both claimed and unclaimed records`() =
        runTest {
            TestDatabase.insertUnclaimedOutboxDirect("test|1", "event-1|1")
            TestDatabase.insertClaimedOutboxDirect("test|2", "event-2|2")

            repository.deleteFromOutbox(setOf("test|1", "test|2"))

            TestDatabase.assertEventOutboxCount(0L)
        }

    @Test
    fun `pollOutbox should return events up to the specified limit (10 of 3)`() =
        runTest {
            // Given
            val entityId = "test-entity-2"
            (1L..3L).forEach {
                TestDatabase.insertUnclaimedOutboxDirect("$entityId|$it", "event-$it|$it")
            }

            // When
            val outboxRecords = repository.pollOutbox(10).toList().sortedBy { it.eventId }

            // Then
            assertEquals(3, outboxRecords.size)

            (1..3).forEach {
                assertEquals("event-$it", outboxRecords[it - 1].event.name)
                assertEquals(it, outboxRecords[it - 1].event.value)
                assertEquals("$entityId|$it", outboxRecords[it - 1].eventId)
            }
        }

    @Test
    fun `pollOutbox should return events up to the specified limit (10 of 11)`() =
        runTest {
            // Given
            val entityId = "test-entity-2"
            (1..11).forEach {
                TestDatabase.insertUnclaimedOutboxDirect("$entityId|$it", "event-$it|$it")
            }
            (1..11).forEach {
                TestDatabase.assertEventOutboxContains("$entityId|$it", "event-$it|$it", false)
            }

            // When
            val outboxRecords = repository.pollOutbox(10).toList().sortedBy { it.eventId }

            // Then
            assertEquals(10, outboxRecords.size)

            (1..10).forEach {
                TestDatabase.assertEventOutboxContains("$entityId|$it", "event-$it|$it", true)
            }
            // sequence 11 is last to be inserted and since poll is querying for the oldest 10 records, this gets left out
            TestDatabase.assertEventOutboxContains("$entityId|11", "event-11|11", false)
        }

    @Test
    fun `pollOutbox should return events excluding locked entries`() =
        runTest {
            // Prepare
            val entityId = "test-entity-2"
            (6..11).forEach {
                TestDatabase.insertUnclaimedOutboxDirect("$entityId|$it", "event-$it|$it")
            }
            TestDatabase.lockRecordsThen {
                // Given
                (1..5).forEach {
                    TestDatabase.insertUnclaimedOutboxDirect("$entityId|$it", "event-$it|$it")
                }

                // When
                val outboxRecords = repository.pollOutbox(10).toList().sortedBy { it.eventId }

                // Then
                assertEquals(5, outboxRecords.size)

                (1..5).forEach {
                    assertEquals("event-$it", outboxRecords[it - 1].event.name)
                    assertEquals(it, outboxRecords[it - 1].event.value)
                    assertEquals("$entityId|$it", outboxRecords[it - 1].eventId)
                }
            }
        }

    @Test
    fun `pollOutbox should return nothing when all records are locked`() =
        runTest {
            // Prepare
            val entityId = "test-entity-2"
            (1..5).forEach {
                TestDatabase.insertUnclaimedOutboxDirect("$entityId|$it", "event-$it|$it")
            }
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
    fun `pollOutbox should return empty flow when outbox is empty`() =
        runTest {
            val records = repository.pollOutbox(10).toList()
            assertEquals(0, records.size)
        }

    @Test
    fun `pollOutbox should return nothing when all records are claimed`() =
        runTest {
            // Given
            val entityId = "test-entity-2"
            (1..5).forEach {
                TestDatabase.insertUnclaimedOutboxDirect("$entityId|$it", "event-$it|$it")
            }
            repository.pollOutbox(10).toList().sortedBy { it.eventId }

            // When
            val outboxRecords = repository.pollOutbox(10).toList().sortedBy { it.eventId }

            // Then
            assertEquals(0, outboxRecords.size)
        }

    @Test
    fun `concurrent pollOutbox should not return duplicate records`() =
        runTest {
            val entityId = "test-entity"
            (1..10).forEach {
                TestDatabase.insertUnclaimedOutboxDirect("$entityId|$it", "event-$it|$it")
            }

            // Simulate concurrent polling
            val r1 = async { repository.pollOutbox(5).toList() }
            val r2 = async { repository.pollOutbox(5).toList() }
            val records1 = r1.await()
            val records2 = r2.await()
            assertEquals(5, records1.size)
            assertEquals(5, records2.toList().size)

            // Verify no overlap
            val eventIds1 = records1.map { it.eventId }.toSet()
            val eventIds2 = records2.map { it.eventId }.toSet()
            assertEquals(0, eventIds1.intersect(eventIds2).size)
        }

    @Test
    fun `cleanupStaleOutboxClaims should return 0 when no claims exist`() =
        runTest {
            TestDatabase.insertUnclaimedOutboxDirect("test|1", "event-1|1")

            val cleaned = repository.cleanupStaleOutboxClaims(1L)

            assertEquals(0L, cleaned)
        }

    @Test
    fun `cleanupStaleOutboxClaims should return 0 when all claims are fresh`() =
        runTest {
            TestDatabase.insertClaimedOutboxDirect("test|1", "event-1|1")

            val cleaned = repository.cleanupStaleOutboxClaims(60000L) // 1 minute

            assertEquals(0L, cleaned)
        }

    @Test
    fun `cleanupStaleOutboxClaims should only update stale claims`() =
        runTest {
            // Given
            val entityId = "test-entity-2"
            TestDatabase.insertClaimedOutboxDirect("$entityId|5", "event-5|5")
            TestDatabase.insertClaimedOutboxDirect("$entityId|4", "event-4|4")
            TestDatabase.insertClaimedOutboxDirect("$entityId|3", "event-3|3", OffsetDateTime.now().minusSeconds(2))
            TestDatabase.insertClaimedOutboxDirect("$entityId|2", "event-2|2", OffsetDateTime.now().minusSeconds(2))
            TestDatabase.insertClaimedOutboxDirect("$entityId|1", "event-1|1")

            // When
            val updatedRows = repository.cleanupStaleOutboxClaims(1000)

            // Then
            assertEquals(2, updatedRows)
            TestDatabase.assertEventOutboxCount(5L)

            // Original orphaned outbox data intact
            TestDatabase.assertEventOutboxContains("$entityId|1", "event-1|1", true)
            TestDatabase.assertEventOutboxContains("$entityId|2", "event-2|2", false)
            TestDatabase.assertEventOutboxContains("$entityId|3", "event-3|3", false)
            TestDatabase.assertEventOutboxContains("$entityId|4", "event-4|4", true)
            TestDatabase.assertEventOutboxContains("$entityId|5", "event-5|5", true)
        }
}
