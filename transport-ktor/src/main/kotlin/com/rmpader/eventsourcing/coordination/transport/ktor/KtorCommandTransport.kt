package com.rmpader.eventsourcing.coordination.transport.ktor

import com.rmpader.eventsourcing.AggregateKey
import com.rmpader.eventsourcing.CommandRejectionException
import com.rmpader.eventsourcing.coordination.CommandTransport
import com.rmpader.eventsourcing.coordination.transport.ktor.protobuf.CommandEnvelope
import com.rmpader.eventsourcing.coordination.transport.ktor.protobuf.CommandResponse
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.protobuf.protobuf
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.install
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
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

    fun Application.configureRouting() {
        install(serverContentNegotiation) {
            protobuf()
        }
        routing {
            inboundPost("/command") {
                handleCommandRequest(call)
            }

            get("/health") {
                call.respond(HttpStatusCode.OK, mapOf("status" to "healthy"))
            }
        }
    }

    fun listenForCommands(port: Int) {
        server =
            embeddedServer(Netty, port = port) {
                configureRouting()
            }
        server.start(wait = false)
        logger.info("Ktor server started on port $port")
    }

    fun stopListening() {
        logger.info("Stopping Ktor")
        server.stop(1000, 2000)
    }

    override suspend fun sendToNode(
        nodeId: String,
        aggregateKey: AggregateKey,
        commandId: String,
        commandData: ByteArray,
    ): ByteArray {
        val baseUrl = "http://$nodeId"
        val aggregateAlias = aggregateKey.entityAlias

        val envelope =
            CommandEnvelope(
                aggregateAlias = aggregateAlias,
                aggregateKey = aggregateKey.toString(),
                commandId = commandId,
                commandData = commandData,
            )

        try {
            val response =
                client.outboundPost("$baseUrl/command") {
                    contentType(ContentType.Application.ProtoBuf)
                    setBody(envelope)
                }

            if (response.status.value >= 500) {
                throw CommandRejectionException(
                    reason = "Remote node returned ${response.status}",
                    errorCode = "TRANSPORT_ERROR",
                )
            } else {
                val commandResponse = response.body<CommandResponse>()
                if (!commandResponse.success) {
                    throw CommandRejectionException(
                        reason = commandResponse.errorMessage,
                        errorCode = commandResponse.errorCode,
                    )
                }
                return commandResponse.stateData
            }
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

    internal suspend fun handleCommandRequest(call: ApplicationCall) {
        try {
            val envelope = call.receive<CommandEnvelope>()
            logger.debug("Received command: ${envelope.commandId} for aggregate ${envelope.aggregateAlias}:${envelope.aggregateKey}")

            val registration = registry.get(envelope.aggregateAlias)

            val stateData =
                registration.handleCommand(
                    aggregateKey = AggregateKey.from(envelope.aggregateKey),
                    commandId = envelope.commandId,
                    commandData = envelope.commandData,
                )

            call.respond(
                HttpStatusCode.OK,
                CommandResponse(
                    success = true,
                    stateData = stateData,
                ),
            )
        } catch (e: CommandRejectionException) {
            logger.warn("Command rejected: ${e.reason}", e)
            call.respond(
                HttpStatusCode.BadRequest,
                CommandResponse(
                    success = false,
                    stateData = ByteArray(0),
                    errorCode = e.errorCode,
                    errorMessage = e.reason,
                ),
            )
        } catch (e: Exception) {
            logger.error("Error processing command", e)
            call.respond(
                HttpStatusCode.UnprocessableEntity,
                CommandResponse(
                    success = false,
                    stateData = ByteArray(0),
                    errorCode = "INTERNAL_ERROR",
                    errorMessage = e.message ?: "Unknown error",
                ),
            )
        }
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(KtorCommandTransport::class.java)
    }
}
