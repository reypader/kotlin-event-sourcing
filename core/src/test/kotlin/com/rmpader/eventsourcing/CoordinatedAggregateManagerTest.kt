package com.rmpader.eventsourcing

import com.rmpader.eventsourcing.coordination.AggregateCoordinator
import com.rmpader.eventsourcing.coordination.AggregateCoordinator.AggregateLocation
import com.rmpader.eventsourcing.coordination.CommandTransport
import com.rmpader.eventsourcing.repository.EventSourcingRepositoryException
import io.mockk.clearAllMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertIs

class CoordinatedAggregateManagerTest : AggregateManagerBaseClass() {
    private val coordinator = mockk<AggregateCoordinator>()
    private val transport = mockk<CommandTransport<TestCommand, TestState>>()
    private val localDelegate = mockk<AggregateManager<TestCommand, TestEvent, TestState>>()

    private val manager =
        object : CoordinatedAggregateManager<TestCommand, TestEvent, TestState>(
            coordinator = coordinator,
            commandTransport = transport,
            localDelegate = localDelegate,
        ) {
            override fun initializeAggregate(entityId: String) = TestState(entityId)
        }

    @AfterEach
    fun cleanup() {
        clearAllMocks()
    }

    @Test
    fun `local aggregate - executes locally`() =
        runTest {
            // Given: Coordinator says aggregate is local
            val testCommand = TestCommand.CreateOrder(100)
            every { coordinator.locateAggregate("order-1") } returns AggregateLocation.Local
            coEvery { localDelegate.acceptCommand("order-1", "cmd-1", testCommand) } returns TestState("order-1")

            // When: Accept command
            val result = manager.acceptCommand("order-1", "cmd-1", testCommand)

            // Then: Local delegate called
            coVerify(exactly = 1) {
                localDelegate.acceptCommand("order-1", "cmd-1", TestCommand.CreateOrder(100))
            }

            // And: Transport not called
            coVerify(exactly = 0) { transport.sendToNode(any(), any(), any(), any()) }
            assertEquals(TestState("order-1"), result)
        }

    @Test
    fun `remote aggregate - delegates to transport`() =
        runTest {
            // Given: Coordinator says aggregate is on remote node
            val testCommand = TestCommand.CreateOrder(100)

            every { coordinator.locateAggregate("order-1") } returns
                AggregateLocation.Remote("node-2")
            coEvery { transport.sendToNode("node-2", "order-1", "cmd-1", testCommand) } returns TestState("order-1")

            // When: Accept command
            val result = manager.acceptCommand("order-1", "cmd-1", TestCommand.CreateOrder(100))

            // Then: Transport called with correct parameters
            coVerify(exactly = 1) {
                transport.sendToNode("node-2", "order-1", "cmd-1", TestCommand.CreateOrder(100))
            }

            // And: Local delegate not called
            coVerify(exactly = 0) { localDelegate.acceptCommand(any(), any(), any()) }
            assertEquals(TestState("order-1"), result)
        }

    @Test
    fun `local execution - EventSourcingRepositoryException propagated`() =
        runTest {
            // Given: Local delegate throws repository exception
            val repoException = EventSourcingRepositoryException(RuntimeException("DB error"))
            coEvery { localDelegate.acceptCommand(any(), any(), any()) } throws repoException

            // When/Then: Exception propagated without wrapping
            val exception =
                assertThrows<EventSourcingRepositoryException> {
                    manager.executeLocally("order-1", "cmd-1", TestCommand.CreateOrder(100))
                }

            assertEquals(repoException, exception)
        }

    @Test
    fun `local execution - CommandRejectionException propagated`() =
        runTest {
            // Given: Local delegate throws domain rejection
            val domainException =
                CommandRejectionException(
                    reason = "Order already exists",
                    errorCode = "DUPLICATE_ORDER",
                )
            coEvery { localDelegate.acceptCommand(any(), any(), any()) } throws domainException

            // When/Then: Exception propagated without wrapping
            val exception =
                assertThrows<CommandRejectionException> {
                    manager.executeLocally("order-1", "cmd-1", TestCommand.CreateOrder(100))
                }

            assertEquals("DUPLICATE_ORDER", exception.errorCode)
        }

    @Test
    fun `local execution - unexpected exception wrapped`() =
        runTest {
            // Given: Local delegate throws unexpected exception
            coEvery { localDelegate.acceptCommand(any(), any(), any()) } throws
                NullPointerException("Unexpected bug")

            // When/Then: Wrapped in CommandRejectionException
            val exception =
                assertThrows<CommandRejectionException> {
                    manager.executeLocally("order-1", "cmd-1", TestCommand.CreateOrder(100))
                }

            assertEquals("LOCAL_EXECUTION_ERROR", exception.errorCode)
            assertIs<NullPointerException>(exception.rootCause)
        }

    @Test
    fun `acceptCommand - local - repository exception propagated`() =
        runTest {
            // Given: Local aggregate with repository failure
            every { coordinator.locateAggregate("order-1") } returns AggregateLocation.Local
            val repoException = EventSourcingRepositoryException(RuntimeException("DB error"))
            coEvery { localDelegate.acceptCommand(any(), any(), any()) } throws repoException

            // When/Then: Repository exception propagates (for retry logic in DefaultAggregateManager)
            val exception =
                assertThrows<EventSourcingRepositoryException> {
                    manager.acceptCommand("order-1", "cmd-1", TestCommand.CreateOrder(100))
                }

            assertEquals(repoException, exception)
        }

    @Test
    fun `acceptCommand - remote - transport exception wrapped`() =
        runTest {
            // Given: Remote aggregate with transport failure
            every { coordinator.locateAggregate("order-1") } returns
                AggregateLocation.Remote("node-2")
            coEvery { transport.sendToNode(any(), any(), any(), any()) } throws
                RuntimeException("Network timeout")

            // When/Then: Wrapped in CommandRejectionException
            val exception =
                assertThrows<CommandRejectionException> {
                    manager.acceptCommand("order-1", "cmd-1", TestCommand.CreateOrder(100))
                }

            assertEquals("REMOTE_EXECUTION_ERROR", exception.errorCode)
            assertIs<RuntimeException>(exception.rootCause)
        }

    @Test
    fun `acceptCommand - remote - CommandRejectionException from transport propagated`() =
        runTest {
            // Given: Remote node rejects command
            every { coordinator.locateAggregate("order-1") } returns
                AggregateLocation.Remote("node-2")
            val remoteRejection =
                CommandRejectionException(
                    reason = "Insufficient stock",
                    errorCode = "OUT_OF_STOCK",
                )
            coEvery { transport.sendToNode(any(), any(), any(), any()) } throws remoteRejection

            // When/Then: Remote rejection propagated as-is
            val exception =
                assertThrows<CommandRejectionException> {
                    manager.acceptCommand("order-1", "cmd-1", TestCommand.CreateOrder(100))
                }

            assertEquals("OUT_OF_STOCK", exception.errorCode)
            assertEquals(remoteRejection, exception)
        }

    @Test
    fun `acceptCommand - remote - transport exception fallback to local`() =
        runTest {
            // setup
            val manager =
                object : CoordinatedAggregateManager<TestCommand, TestEvent, TestState>(
                    coordinator = coordinator,
                    commandTransport = transport,
                    localDelegate = localDelegate,
                    localFallbackCondition = { true },
                ) {
                    override fun initializeAggregate(entityId: String) = TestState(entityId)
                }
            val testCommand = TestCommand.CreateOrder(100)
            // Given: Remote aggregate with transport failure
            every { coordinator.locateAggregate("order-1") } returns
                AggregateLocation.Remote("node-2")
            coEvery { transport.sendToNode(any(), any(), any(), any()) } throws
                RuntimeException("Network timeout")
            coEvery { localDelegate.acceptCommand("order-1", "cmd-1", testCommand) } returns TestState("order-1")

            // When: Accept command
            val result = manager.acceptCommand("order-1", "cmd-1", testCommand)

            // Then: Wrapped in CommandRejectionException
            coVerify(exactly = 1) {
                localDelegate.acceptCommand("order-1", "cmd-1", testCommand)
            }
            assertEquals(TestState("order-1"), result)
        }

    @Test
    fun `acceptCommand - remote - fallback exception propagated`() =
        runTest {
            // setup
            val manager =
                object : CoordinatedAggregateManager<TestCommand, TestEvent, TestState>(
                    coordinator = coordinator,
                    commandTransport = transport,
                    localDelegate = localDelegate,
                    localFallbackCondition = { true },
                ) {
                    override fun initializeAggregate(entityId: String) = TestState(entityId)
                }
            val testCommand = TestCommand.CreateOrder(100)

            // Given: Remote node rejects command
            every { coordinator.locateAggregate("order-1") } returns
                AggregateLocation.Remote("node-2")
            val remoteRejection =
                CommandRejectionException(
                    reason = "Insufficient stock",
                    errorCode = "OUT_OF_STOCK",
                )
            coEvery { transport.sendToNode(any(), any(), any(), any()) } throws remoteRejection
            coEvery {
                localDelegate.acceptCommand(
                    "order-1",
                    "cmd-1",
                    testCommand,
                )
            } throws RuntimeException("Network timeout")

            // When/Then: Remote rejection propagated as-is
            val exception =
                assertThrows<CommandRejectionException> {
                    manager.acceptCommand("order-1", "cmd-1", TestCommand.CreateOrder(100))
                }

            coVerify(exactly = 1) {
                localDelegate.acceptCommand("order-1", "cmd-1", testCommand)
            }

            assertEquals("LOCAL_EXECUTION_ERROR", exception.errorCode)
        }

    @Test
    fun `coordinator exception wrapped`() =
        runTest {
            // Given: Coordinator throws unexpected exception
            every { coordinator.locateAggregate("order-1") } throws
                IllegalStateException("Coordinator not initialized")

            // When/Then: Wrapped in CommandRejectionException
            val exception =
                assertThrows<CommandRejectionException> {
                    manager.acceptCommand("order-1", "cmd-1", TestCommand.CreateOrder(100))
                }

            assertEquals("REMOTE_EXECUTION_ERROR", exception.errorCode)
            assertIs<IllegalStateException>(exception.rootCause)
        }
}
