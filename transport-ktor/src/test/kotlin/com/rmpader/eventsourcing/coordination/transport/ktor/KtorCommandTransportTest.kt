package com.rmpader.eventsourcing.coordination.transport.ktor

import com.rmpader.eventsourcing.AggregateKey
import com.rmpader.eventsourcing.AggregateManager
import com.rmpader.eventsourcing.AggregateState
import com.rmpader.eventsourcing.CommandRejectionException
import com.rmpader.eventsourcing.Serializer
import com.rmpader.eventsourcing.coordination.transport.ktor.protobuf.CommandEnvelope
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.protobuf.protobuf
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.ExperimentalSerializationApi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation as clientContentNegotiation

@OptIn(ExperimentalSerializationApi::class)
class KtorCommandTransportTest {
    // Test domain model
    data class TestCommand(
        val value: Int,
    )

    data class TestEvent(
        val value: Int,
    )

    data class TestState(
        override val key: AggregateKey,
        val total: Int = 0,
    ) : AggregateState<TestCommand, TestEvent, TestState> {
        override fun handleCommand(command: TestCommand): TestEvent {
            if (command.value < 0) {
                throw CommandRejectionException(
                    reason = "Negative values not allowed",
                    errorCode = "NEGATIVE_VALUE",
                )
            }
            return TestEvent(command.value)
        }

        override fun applyEvent(event: TestEvent): TestState = copy(total = total + event.value)
    }

    // Test serializers
    class TestCommandSerializer : Serializer<TestCommand> {
        override fun serialize(data: TestCommand): ByteArray = data.value.toString().toByteArray()

        override fun deserialize(data: ByteArray): TestCommand = TestCommand(String(data).toInt())
    }

    class TestStateSerializer : Serializer<TestState> {
        override fun serialize(data: TestState): ByteArray = data.total.toString().toByteArray()

        override fun deserialize(data: ByteArray): TestState = throw UnsupportedOperationException("Not needed for tests")
    }

    // Test aggregate manager
    class TestAggregateManager : AggregateManager<TestCommand, TestEvent, TestState> {
        val commandsProcessed = AtomicInteger(0)

        override suspend fun acceptCommand(
            key: AggregateKey,
            commandId: String,
            command: TestCommand,
        ): TestState {
            commandsProcessed.incrementAndGet()
            val state = TestState(key)
            val event = state.handleCommand(command)
            return state.applyEvent(event)
        }
    }

    private lateinit var transport: KtorCommandTransport
    private lateinit var testManager: TestAggregateManager

    @BeforeEach
    fun setup() {
        transport = KtorCommandTransport()
        testManager = TestAggregateManager()

        transport.registerAggregate(
            alias = "test",
            commandSerializer = TestCommandSerializer(),
            stateSerializer = TestStateSerializer(),
            aggregateManager = testManager,
        )
    }

    @AfterEach
    fun teardown() {
        if (::transport.isInitialized) {
            try {
                transport.stopListening()
            } catch (e: Exception) {
                // Server might not be started
            }
        }
    }

    @Test
    fun `registerAggregate - should register aggregate successfully`() {
        // Given: A new transport
        val newTransport = KtorCommandTransport()

        // When: Registering an aggregate
        newTransport.registerAggregate(
            alias = "order",
            commandSerializer = TestCommandSerializer(),
            stateSerializer = TestStateSerializer(),
            aggregateManager = testManager,
        )

        // Then: No exception thrown (registration successful)
        // Verification is implicit - if registration fails, it would throw
    }

    @Test
    fun `handleCommandRequest - successful command execution`() =
        testApplication {
            application {
                transport.run { configureRouting() }
            }
            client =
                createClient {
                    install(clientContentNegotiation) {
                        protobuf()
                    }
                }

            // Given: A valid command envelope
            val envelope =
                CommandEnvelope(
                    aggregateAlias = "test",
                    aggregateKey = "test|entity-1",
                    commandId = "cmd-123",
                    commandData = "42".toByteArray(),
                )

            // When: Posting command to endpoint
            val response =
                client.post("/command") {
                    contentType(ContentType.Application.ProtoBuf)
                    setBody(envelope)
                }

            // Then: Response is OK
            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals(1, testManager.commandsProcessed.get())
        }

    @Test
    fun `handleCommandRequest - command rejection`() =
        testApplication {
            application {
                transport.run { configureRouting() }
            }
            client =
                createClient {
                    install(clientContentNegotiation) {
                        protobuf()
                    }
                }
            // Given: Command that will be rejected (negative value)
            val envelope =
                CommandEnvelope(
                    aggregateAlias = "test",
                    aggregateKey = "test|entity-1",
                    commandId = "cmd-123",
                    commandData = "-10".toByteArray(),
                )

            // When: Posting command
            val response =
                client.post("/command") {
                    contentType(ContentType.Application.ProtoBuf)
                    setBody(envelope)
                }

            // Then: Response is BadRequest
            assertEquals(HttpStatusCode.BadRequest, response.status)
        }

    @Test
    fun `handleCommandRequest - unknown aggregate type`() =
        testApplication {
            application {
                transport.run { configureRouting() }
            }
            client =
                createClient {
                    install(clientContentNegotiation) {
                        protobuf()
                    }
                }
            // Given: Command for unregistered aggregate
            val envelope =
                CommandEnvelope(
                    aggregateAlias = "unknown",
                    aggregateKey = "unknown|entity-1",
                    commandId = "cmd-123",
                    commandData = "42".toByteArray(),
                )

            // When: Posting command
            // When: Posting command
            val response =
                client.post("/command") {
                    contentType(ContentType.Application.ProtoBuf)
                    setBody(envelope)
                }

            // Then: Response is UnprocessableEntity
            assertEquals(HttpStatusCode.UnprocessableEntity, response.status)
        }

    @Test
    fun `sendToNode - successful remote command execution`() =
        runTest {
            // Given: Transport with server listening
            val port = 8888
            transport.listenForCommands(port)
            delay(500) // Wait for server to start

            try {
                // When: Sending command to local server
                val key = AggregateKey.from("test|entity-1")
                val commandData = "42".toByteArray()

                val resultData =
                    transport.sendToNode(
                        nodeId = "localhost:$port",
                        aggregateKey = key,
                        commandId = "cmd-123",
                        commandData = commandData,
                    )

                // Then: Should receive state data back
                assertEquals("42", String(resultData))
                assertEquals(1, testManager.commandsProcessed.get())
            } finally {
                transport.stopListening()
            }
        }

    @Test
    fun `sendToNode - handles command rejection from remote`() =
        runTest {
            // Given: Transport with server listening
            val port = 8889
            transport.listenForCommands(port)
            delay(500)

            try {
                // When: Sending command that will be rejected
                val key = AggregateKey.from("test|entity-1")
                val commandData = "-10".toByteArray()

                // Then: Should throw CommandRejectionException
                val exception =
                    assertThrows<CommandRejectionException> {
                        transport.sendToNode(
                            nodeId = "localhost:$port",
                            aggregateKey = key,
                            commandId = "cmd-123",
                            commandData = commandData,
                        )
                    }

                assertEquals("NEGATIVE_VALUE", exception.errorCode)
                assertTrue(exception.reason.contains("Negative values"))
            } finally {
                transport.stopListening()
            }
        }

    @Test
    fun `sendToNode - handles network errors`() =
        runTest {
            // Given: No server running
            val key = AggregateKey.from("test|entity-1")
            val commandData = "42".toByteArray()

            // When: Attempting to send to non-existent node
            // Then: Should throw CommandRejectionException with TRANSPORT_ERROR
            val exception =
                assertThrows<CommandRejectionException> {
                    transport.sendToNode(
                        nodeId = "localhost:9999", // Non-existent server
                        aggregateKey = key,
                        commandId = "cmd-123",
                        commandData = commandData,
                    )
                }

            assertEquals("TRANSPORT_ERROR", exception.errorCode)
            assertTrue(exception.reason.contains("Failed to send command"))
        }

    @Test
    fun `server lifecycle - start and stop`() =
        runTest {
            // Given: Transport not started
            val port = 8890

            // When: Starting server
            transport.listenForCommands(port)
            delay(500) // Wait for startup

            // Then: Server should be accessible
            val key = AggregateKey.from("test|entity-1")
            val result =
                transport.sendToNode(
                    nodeId = "localhost:$port",
                    aggregateKey = key,
                    commandId = "cmd-1",
                    commandData = "10".toByteArray(),
                )
            assertContentEquals("10".toByteArray(), result)

            // When: Stopping server
            transport.stopListening()
            delay(500) // Wait for shutdown

            // Then: Server should not be accessible
            assertThrows<CommandRejectionException> {
                transport.sendToNode(
                    nodeId = "localhost:$port",
                    aggregateKey = key,
                    commandId = "cmd-2",
                    commandData = "20".toByteArray(),
                )
            }
        }

    @Test
    fun `multiple commands - processes sequentially`() =
        runTest {
            // Given: Server running
            val port = 8891
            transport.listenForCommands(port)
            delay(500)

            try {
                val key = AggregateKey.from("test|entity-1")

                // When: Sending multiple commands
                repeat(5) { i ->
                    val result =
                        transport.sendToNode(
                            nodeId = "localhost:$port",
                            aggregateKey = key,
                            commandId = "cmd-$i",
                            commandData = "${i * 10}".toByteArray(),
                        )
                    assertEquals("${i * 10}", String(result))
                }

                // Then: All commands processed
                assertEquals(5, testManager.commandsProcessed.get())
            } finally {
                transport.stopListening()
            }
        }

    @Test
    fun `protobuf serialization - handles binary data correctly`() =
        testApplication {
            application {
                transport.run { configureRouting() }
            }
            client =
                createClient {
                    install(clientContentNegotiation) {
                        protobuf()
                    }
                }
            // Given: Command with specific binary representation
            val envelope =
                CommandEnvelope(
                    aggregateAlias = "test",
                    aggregateKey = "test|entity-1",
                    commandId = "cmd-binary",
                    commandData = "999".toByteArray(),
                )

            // When: Sending via protobuf
            val response =
                client.post("/command") {
                    contentType(ContentType.Application.ProtoBuf)
                    setBody(envelope)
                }

            // Then: Response contains correct data
            assertEquals(HttpStatusCode.OK, response.status)
            val responseBody = response.bodyAsText()
            assertTrue(responseBody.isNotEmpty())
        }
}
