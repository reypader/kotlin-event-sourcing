package com.rmpader.eventsourcing.coordination.transport.ktor

import com.rmpader.eventsourcing.AggregateKey
import com.rmpader.eventsourcing.AggregateManager
import com.rmpader.eventsourcing.AggregateState
import com.rmpader.eventsourcing.Serializer
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * Integration tests for multi-node command transport
 */
class KtorCommandTransportIntegrationTest {
    data class OrderCommand(
        val amount: Int,
    )

    data class OrderEvent(
        val amount: Int,
    )

    data class OrderState(
        override val key: AggregateKey,
        val total: Int = 0,
    ) : AggregateState<OrderCommand, OrderEvent, OrderState> {
        override fun handleCommand(command: OrderCommand): OrderEvent = OrderEvent(command.amount)

        override fun applyEvent(event: OrderEvent): OrderState = copy(total = total + event.amount)
    }

    class OrderCommandSerializer : Serializer<OrderCommand> {
        override fun serialize(data: OrderCommand) = "ORDER:${data.amount}".toByteArray()

        override fun deserialize(data: ByteArray): OrderCommand {
            val parts = String(data).split(":")
            return OrderCommand(parts[1].toInt())
        }
    }

    class OrderStateSerializer : Serializer<OrderState> {
        override fun serialize(data: OrderState) = "STATE:${data.total}".toByteArray()

        override fun deserialize(data: ByteArray): OrderState = throw UnsupportedOperationException()
    }

    class OrderManager : AggregateManager<OrderCommand, OrderEvent, OrderState> {
        override suspend fun acceptCommand(
            aggregateKey: AggregateKey,
            commandId: String,
            command: OrderCommand,
        ): OrderState {
            val state = OrderState(aggregateKey)
            val event = state.handleCommand(command)
            return state.applyEvent(event)
        }
    }

    private lateinit var node1: KtorCommandTransport
    private lateinit var node2: KtorCommandTransport

    @BeforeEach
    fun setup() {
        node1 = KtorCommandTransport()
        node2 = KtorCommandTransport()

        // Register aggregates on both nodes
        listOf(node1, node2).forEach { transport ->
            transport.registerAggregate(
                alias = "order",
                commandSerializer = OrderCommandSerializer(),
                stateSerializer = OrderStateSerializer(),
                aggregateManager = OrderManager(),
            )
        }
    }

    @AfterEach
    fun teardown() {
        listOf(node1, node2).forEach { transport ->
            try {
                transport.stopListening()
            } catch (e: Exception) {
                // Ignore
            }
        }
    }

    @Test
    fun `two nodes - can communicate bidirectionally`() =
        runTest {
            // Given: Two nodes running on different ports
            val port1 = 9001
            val port2 = 9002

            node1.listenForCommands(port1)
            node2.listenForCommands(port2)
            delay(1000) // Wait for both servers to start

            try {
                // When: Node 1 sends command to Node 2
                val result1 =
                    node1.sendToNode(
                        nodeId = "localhost:$port2",
                        aggregateKey = AggregateKey.from("order:order-1"),
                        commandId = "cmd-1",
                        commandData = OrderCommandSerializer().serialize(OrderCommand(100)),
                    )

                // Then: Node 2 processes command and returns state
                assertEquals("STATE:100", String(result1))

                // When: Node 2 sends command to Node 1
                val result2 =
                    node2.sendToNode(
                        nodeId = "localhost:$port1",
                        aggregateKey = AggregateKey.from("order:order-2"),
                        commandId = "cmd-2",
                        commandData = OrderCommandSerializer().serialize(OrderCommand(200)),
                    )

                // Then: Node 1 processes command and returns state
                assertEquals("STATE:200", String(result2))
            } finally {
                node1.stopListening()
                node2.stopListening()
            }
        }

    @Test
    fun `concurrent commands - all processed successfully`() =
        runTest {
            // Given: Server running
            val port = 9003
            node1.listenForCommands(port)
            delay(500)

            try {
                // When: Sending multiple concurrent commands
                val results =
                    List(10) { i ->
                        launch {
                            node2.sendToNode(
                                nodeId = "localhost:$port",
                                aggregateKey = AggregateKey.from("order:order-$i"),
                                commandId = "cmd-$i",
                                commandData = OrderCommandSerializer().serialize(OrderCommand(i * 10)),
                            )
                        }
                    }

                // Then: All complete successfully
                results.forEach { it.join() }
            } finally {
                node1.stopListening()
            }
        }
}
