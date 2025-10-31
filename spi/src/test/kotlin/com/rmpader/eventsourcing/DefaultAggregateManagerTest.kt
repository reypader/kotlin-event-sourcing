package com.rmpader.eventsourcing

import com.rmpader.eventsourcing.repository.AggregateRepository
import com.rmpader.eventsourcing.repository.EventSourcingRepositoryException
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertSame

class DefaultAggregateManagerTest {
    // Test domain model
    sealed interface TestCommand {
        data class CreateOrder(
            val amount: Int,
        ) : TestCommand

        data class InvalidCommand(
            val reason: String,
        ) : TestCommand
    }

    sealed interface TestEvent {
        data class OrderCreated(
            val finalAmount: Int,
            val originalAmount: Int,
            val additionalAmount: Int,
        ) : TestEvent
    }

    data class TestState(
        val orderId: String,
        val amount: Int = 0,
        val created: Boolean = false,
    ) : AggregateEntity<TestCommand, TestEvent, TestState> {
        override fun handleCommand(command: TestCommand): TestEvent =
            when (command) {
                is TestCommand.CreateOrder -> TestEvent.OrderCreated(amount + command.amount, amount, command.amount)
                is TestCommand.InvalidCommand ->
                    throw CommandRejectionException(
                        reason = command.reason,
                        errorCode = "INVALID_COMMAND",
                    )
            }

        override fun applyEvent(event: TestEvent): TestState =
            when (event) {
                is TestEvent.OrderCreated -> copy(amount = event.finalAmount, created = true)
            }

        companion object {
            fun empty(id: String) = TestState(id)
        }
    }

    private val repository = mockk<AggregateRepository<TestEvent, TestState>>()

    private val manager =
        object : DefaultAggregateManager<TestCommand, TestEvent, TestState>(
            repository = repository,
        ) {
            override fun initializeAggregate(entityId: String) = TestState.empty(entityId)
        }

    @AfterEach
    fun cleanup() {
        clearAllMocks()
    }

    @Test
    fun `successful command execution - no snapshot, no events`() =
        runTest {
            // Given: Empty aggregate
            coEvery { repository.loadLatestSnapshot("order-1") } returns null
            coEvery { repository.loadEvents("order-1", 1) } returns flowOf()
            coEvery { repository.storeEvent(any()) } just Runs

            // When: Execute command
            manager.acceptCommand("order-1", TestCommand.CreateOrder(100))

            // Then: Event stored with correct sequence
            coVerify(exactly = 1) {
                repository.storeEvent(
                    match {
                        it.entityId == "order-1" &&
                            it.sequenceNumber == 1L &&
                            it.event == TestEvent.OrderCreated(100, 0, 100)
                    },
                )
            }
        }

    @Test
    fun `successful command execution - with snapshot and events`() =
        runTest {
            // Given: Aggregate with snapshot at sequence 5
            val snapshot =
                AggregateRepository.SnapshotRecord(
                    entityId = "order-1",
                    state = TestState("order-1", amount = 50, created = true),
                    sequenceNumber = 5,
                    timestamp = OffsetDateTime.now(),
                )
            coEvery { repository.loadLatestSnapshot("order-1") } returns snapshot

            // And: Additional events after snapshot
            val events: Flow<AggregateRepository.EventRecord<TestEvent>> =
                flowOf(
                    AggregateRepository.EventRecord(
                        entityId = "order-1",
                        event = TestEvent.OrderCreated(25, 50, -25),
                        sequenceNumber = 6,
                        timestamp = OffsetDateTime.now(),
                    ),
                    AggregateRepository.EventRecord(
                        entityId = "order-1",
                        event = TestEvent.OrderCreated(25, 25, 0),
                        sequenceNumber = 7,
                        timestamp = OffsetDateTime.now(),
                    ),
                )
            coEvery { repository.loadEvents("order-1", 6) } returns events
            coEvery { repository.storeEvent(any()) } just Runs

            // When: Execute command
            manager.acceptCommand("order-1", TestCommand.CreateOrder(100))

            // Then: Event stored with next sequence number
            coVerify(exactly = 1) {
                repository.storeEvent(
                    match {
                        it.entityId == "order-1" &&
                            it.sequenceNumber == 8L && // After sequence 7
                            it.event == TestEvent.OrderCreated(125, 25, 100)
                    },
                )
            }
        }

    @Test
    fun `command rejection - domain error propagated`() =
        runTest {
            // Given: Valid aggregate state
            coEvery { repository.loadLatestSnapshot("order-1") } returns null
            coEvery { repository.loadEvents("order-1", 1) } returns flowOf()

            // When/Then: Domain rejection throws CommandRejectionException
            val exception =
                assertThrows<CommandRejectionException> {
                    manager.acceptCommand("order-1", TestCommand.InvalidCommand("bad request"))
                }

            assertEquals("bad request", exception.reason)
            assertEquals("INVALID_COMMAND", exception.errorCode)

            // And: Repository storeEvent never called
            coVerify(exactly = 0) { repository.storeEvent(any()) }
        }

    @Test
    fun `repository failure - EventSourcingRepositoryException propagated`() =
        runTest {
            // Given: Empty aggregate
            coEvery { repository.loadLatestSnapshot("order-1") } returns null
            coEvery { repository.loadEvents("order-1", 1) } returns flowOf()

            // And: storeEvent fails with repository exception
            val dbException = EventSourcingRepositoryException(RuntimeException("DB hiccup"))
            coEvery { repository.storeEvent(any()) } throws dbException

            // When/Then: Repository exception propagated (caller handles retries)
            val exception =
                assertThrows<EventSourcingRepositoryException> {
                    manager.acceptCommand("order-1", TestCommand.CreateOrder(100))
                }

            assertSame(dbException, exception)

            // And: Only attempted once (no internal retry)
            coVerify(exactly = 1) { repository.storeEvent(any()) }
        }

    @Test
    fun `repository failure on load - EventSourcingRepositoryException propagated`() =
        runTest {
            // Given: Repository fails to load snapshot
            val dbException = EventSourcingRepositoryException(RuntimeException("DB error"))
            coEvery { repository.loadLatestSnapshot("order-1") } throws dbException

            // When/Then: Repository exception propagated
            val exception =
                assertThrows<EventSourcingRepositoryException> {
                    manager.acceptCommand("order-1", TestCommand.CreateOrder(100))
                }

            assertSame(dbException, exception)
        }

    @Test
    fun `unexpected exception - wrapped as CommandRejectionException`() =
        runTest {
            // Given: Repository throws unexpected exception (bug)
            coEvery { repository.loadLatestSnapshot("order-1") } throws
                NullPointerException("Unexpected bug")

            // When/Then: Wrapped in CommandRejectionException
            val exception =
                assertThrows<CommandRejectionException> {
                    manager.acceptCommand("order-1", TestCommand.CreateOrder(100))
                }

            assertEquals("LOCAL_EXECUTION_ERROR", exception.errorCode)
            assertIs<NullPointerException>(exception.rootCause)
        }

    @Test
    fun `state is correctly rebuilt from snapshot and events`() =
        runTest {
            // Given: Snapshot with amount = 100
            val snapshot =
                AggregateRepository.SnapshotRecord(
                    entityId = "order-1",
                    state = TestState("order-1", amount = 100, created = true),
                    sequenceNumber = 3,
                    timestamp = OffsetDateTime.now(),
                )
            coEvery { repository.loadLatestSnapshot("order-1") } returns snapshot

            // And: Events that increase amount by 50 and 25
            val events: Flow<AggregateRepository.EventRecord<TestEvent>> =
                flowOf(
                    AggregateRepository.EventRecord(
                        entityId = "order-1",
                        event = TestEvent.OrderCreated(150, 100, 50),
                        sequenceNumber = 4,
                        timestamp = OffsetDateTime.now(),
                    ),
                    AggregateRepository.EventRecord(
                        entityId = "order-1",
                        event = TestEvent.OrderCreated(175, 150, 25),
                        sequenceNumber = 5,
                        timestamp = OffsetDateTime.now(),
                    ),
                )
            coEvery { repository.loadEvents("order-1", 4) } returns events

            val storedEvent = slot<AggregateRepository.EventRecord<TestEvent>>()
            coEvery { repository.storeEvent(capture(storedEvent)) } just Runs

            // When: Execute command (state should be rebuilt: 100 (snapshot) -> 150 -> 175)
            manager.acceptCommand("order-1", TestCommand.CreateOrder(200))

            // Then: Verify final event stored
            assertEquals("order-1", storedEvent.captured.entityId)
            assertEquals(6L, storedEvent.captured.sequenceNumber)
            assertEquals(TestEvent.OrderCreated(375, 175, 200), storedEvent.captured.event)
        }
}
