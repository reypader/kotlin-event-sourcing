package com.rmpader.eventsourcing.coordination.transport.ktor

import com.rmpader.eventsourcing.AggregateKey
import com.rmpader.eventsourcing.CommandRejectionException
import com.rmpader.eventsourcing.coordination.CommandTransport
import com.rmpader.eventsourcing.coordination.transport.ktor.protobuf.CommandEnvelope
import com.rmpader.eventsourcing.coordination.transport.ktor.protobuf.CommandResponse
import com.rmpader.eventsourcing.coordination.transport.ktor.protobuf.commandEnvelope
import com.rmpader.eventsourcing.coordination.transport.ktor.protobuf.commandResponse
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.protobuf.protobuf
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.install
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.routing
import kotlinx.serialization.ExperimentalSerializationApi
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation as clientContentNegotiation
import io.ktor.client.request.post as outboundPost
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation as serverContentNegotiation
import io.ktor.server.routing.post as inboundPost

@OptIn(ExperimentalSerializationApi::class)
class KtorCommandTransport : CommandTransport {
    private lateinit var server: EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration>
    private val registry = AggregateRegistry()

    fun <C, E, S : com.rmpader.eventsourcing.AggregateState<C, E, S>> registerAggregate(
        alias: String,
        commandSerializer: com.rmpader.eventsourcing.Serializer<C>,
        stateSerializer: com.rmpader.eventsourcing.Serializer<S>,
        aggregateManager: com.rmpader.eventsourcing.AggregateManager<C, E, S>,
    ) {
        registry.register(alias, commandSerializer, stateSerializer, aggregateManager)
        logger.info("Registered aggregate type: $alias")
    }

    private val client =
        HttpClient(CIO) {
            install(clientContentNegotiation) {
                protobuf()
            }
        }

    fun listenForCommands(port: Int) {
        server =
            embeddedServer(Netty, port = port) {
                install(serverContentNegotiation) {
                    protobuf()
                }
                routing {
                    inboundPost("/command") {
                        handleCommandRequest(call)
                    }
                }
            }
        server.start(wait = false)
        println("Ktor server started on port 8080")
    }

    fun stopListening() {
        println("Stopping Ktor")
        server.stop(1000, 2000) // Graceful shutdown with 1s grace period and 2s timeout
    }

    override suspend fun sendToNode(
        nodeId: String,
        aggregateKey: AggregateKey,
        commandId: String,
        commandData: ByteArray,
    ): ByteArray {
        // TODO make this a bit more configurable
        val baseUrl = "http://$nodeId"

        val aggregateAlias = aggregateKey.entityAlias

        val envelope =
            commandEnvelope {
                this.aggregateAlias = aggregateAlias
                this.aggregateKey = aggregateKey.toString()
                this.commandId = commandId
                this.commandData =
                    com.google.protobuf.ByteString
                        .copyFrom(commandData)
            }

        try {
            val response =
                client.outboundPost("$baseUrl/command") {
                    contentType(ContentType.Application.ProtoBuf)
                    setBody(envelope)
                }

            if (response.status != HttpStatusCode.OK) {
                throw CommandRejectionException(
                    reason = "Remote node returned ${response.status}",
                    errorCode = "TRANSPORT_ERROR",
                )
            }

            val commandResponse = response.body<CommandResponse>()

            if (!commandResponse.success) {
                throw CommandRejectionException(
                    reason = commandResponse.errorMessage,
                    errorCode = commandResponse.errorCode,
                )
            }

            return commandResponse.stateData.toByteArray()
        } catch (e: CommandRejectionException) {
            throw e
        } catch (e: Exception) {
            logger.error("Error sending command to node $nodeId", e)
            throw CommandRejectionException(
                reason = "Failed to send command: ${e.message}",
                errorCode = "TRANSPORT_ERROR",
                rootCause = e,
            )
        }
    }

    /**
     * Handle incoming command request
     */
    private suspend fun handleCommandRequest(call: ApplicationCall) {
        try {
            val envelope = call.receive<CommandEnvelope>()
            logger.debug("Received command: ${envelope.commandId} for aggregate ${envelope.aggregateAlias}:${envelope.aggregateKey}")

            val registration = registry.get(envelope.aggregateAlias)

            val stateData =
                registration.handleCommand(
                    aggregateKey = AggregateKey.from(envelope.aggregateKey),
                    commandId = envelope.commandId,
                    commandData = envelope.commandData.toByteArray(),
                )

            call.respond(
                HttpStatusCode.OK,
                commandResponse {
                    this.success = true
                    this.stateData =
                        com.google.protobuf.ByteString
                            .copyFrom(stateData)
                },
            )
        } catch (e: CommandRejectionException) {
            logger.warn("Command rejected: ${e.reason}", e)
            call.respond(
                HttpStatusCode.BadRequest,
                commandResponse {
                    this.success = false
                    this.stateData = com.google.protobuf.ByteString.EMPTY
                    this.errorCode = e.errorCode
                    this.errorMessage = e.reason
                },
            )
        } catch (e: Exception) {
            logger.error("Error processing command", e)
            call.respond(
                HttpStatusCode.InternalServerError,
                commandResponse {
                    this.success = false
                    this.stateData = com.google.protobuf.ByteString.EMPTY
                    this.errorCode = "INTERNAL_ERROR"
                    this.errorMessage = e.message ?: "Unknown error"
                },
            )
        }
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(KtorCommandTransport::class.java)
    }
}
