package com.rmpader.eventsourcing.coordination.ddb

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.DeleteItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.ScanRequest
import com.rmpader.eventsourcing.coordination.AggregateCoordinator
import com.rmpader.eventsourcing.coordination.AggregateCoordinator.AggregateLocation
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.security.MessageDigest
import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

/**
 * DynamoDB-based membership with TTL heartbeats.
 */
class DynamoDbAggregateCoordinator private constructor(
    private val tableName: String,
    private val nodeId: String,
    private val dynamoDbClient: DynamoDbClient,
    private val heartbeatInterval: Duration,
    private val ttl: Duration,
) : AggregateCoordinator {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var heartbeatJob: Job? = null
    private var scanJob: Job? = null

    private val clusterMembers = MutableStateFlow<List<String>>(emptyList())

    @Volatile
    private var lastHeartbeatTime: OffsetDateTime? = null

    @Volatile
    private var lastScanTime: OffsetDateTime? = null

    override fun start() {
        logger.info("Starting DynamoDbAggregateCoordinator...")
        logger.debug(
            "Configuration: tableName=$tableName, nodeId=$nodeId, " +
                "heartbeatInterval=$heartbeatInterval, ttl=$ttl",
        )

        logger.info("Starting heartbeat job...")
        heartbeatJob =
            scope.launch {
                while (isActive) {
                    try {
                        sendHeartbeat()
                        delay(heartbeatInterval)
                    } catch (e: Exception) {
                        logger.error("Error sending heartbeat: ${e.message}", e)
                        delay(1000)
                    }
                }
            }

        logger.info("Starting membership scan job...")
        scanJob =
            scope.launch {
                while (isActive) {
                    try {
                        updateMembership()
                        delay(heartbeatInterval)
                    } catch (e: Exception) {
                        logger.error("Error updating membership: ${e.message}", e)
                        delay(1000)
                    }
                }
            }

        logger.info("Initial cluster membership update...")
        scope.launch {
            updateMembership()
        }

        logger.info("DynamoDbAggregateCoordinator started successfully")
    }

    override fun stop() {
        logger.info("Stopping DynamoDbAggregateCoordinator...")
        heartbeatJob?.cancel()
        scanJob?.cancel()
        scope.cancel()

        // Remove self from membership
        runBlocking {
            try {
                logger.info("Removing node $nodeId from membership table...")
                dynamoDbClient.deleteItem(
                    DeleteItemRequest {
                        tableName = this@DynamoDbAggregateCoordinator.tableName
                        key = mapOf("nodeId" to AttributeValue.S(nodeId))
                    },
                )
                logger.info("Node removed from membership table")
            } catch (e: Exception) {
                logger.error("Error removing node from membership: ${e.message}", e)
            }
            dynamoDbClient.close()
        }
    }

    override fun locateAggregate(aggregateId: String): AggregateLocation {
        val members = clusterMembers.value
        logger.info("Locating aggregate $aggregateId amongst ${members.size} members...")

        val targetNode =
            members.maxByOrNull { member ->
                hash("$aggregateId:$member")
            }

        return if (targetNode == null || targetNode == nodeId) {
            logger.debug("Aggregate $aggregateId is local")
            AggregateLocation.Local
        } else {
            logger.debug("Aggregate $aggregateId is located remotely: $targetNode")
            AggregateLocation.Remote(targetNode)
        }
    }

    override fun checkHealth(): AggregateCoordinator.HealthStatus {
        val now = OffsetDateTime.now()
        val heartbeatJobActive = heartbeatJob?.isActive ?: false
        val scanJobActive = scanJob?.isActive ?: false

        val heartbeatStale =
            lastHeartbeatTime?.let {
                val timeSinceLastHeartbeat = java.time.Duration.between(it, now)
                timeSinceLastHeartbeat.toKotlinDuration() > (heartbeatInterval * 3)
            } ?: true
        val scanStale =
            lastScanTime?.let {
                val timeSinceLastScan = java.time.Duration.between(it, now)
                timeSinceLastScan.toKotlinDuration() > (heartbeatInterval * 3)
            } ?: true

        val details =
            mutableMapOf(
                "heartbeatJobActive" to heartbeatJobActive.toString(),
                "scanJobActive" to scanJobActive.toString(),
                "clusterSize" to clusterMembers.value.size.toString(),
            )

        return when {
            !heartbeatJobActive ->
                AggregateCoordinator.HealthStatus.unhealthy(
                    "Heartbeat job is not active",
                    details,
                )

            !scanJobActive ->
                AggregateCoordinator.HealthStatus.unhealthy(
                    "Scan job is not active",
                    details,
                )

            heartbeatStale ->
                AggregateCoordinator.HealthStatus.unhealthy(
                    "Heartbeat is stale",
                    details,
                )

            scanStale ->
                AggregateCoordinator.HealthStatus.unhealthy(
                    "Membership scan is stale",
                    details,
                )

            else -> AggregateCoordinator.HealthStatus.healthy(details)
        }
    }

    private suspend fun sendHeartbeat() {
        try {
            val now = OffsetDateTime.now(UTC)
            val expiresAt = now.plus(ttl.toJavaDuration()).toInstant().epochSecond

            logger.debug("Sending heartbeat for node $nodeId (ttl: $expiresAt)")
            dynamoDbClient.putItem(
                PutItemRequest {
                    tableName = this@DynamoDbAggregateCoordinator.tableName
                    item =
                        mapOf(
                            "nodeId" to AttributeValue.S(nodeId),
                            "lastHeartbeat" to AttributeValue.N(now.toString()),
                            "ttl" to AttributeValue.N(expiresAt.toString()),
                        )
                },
            )
            lastHeartbeatTime = OffsetDateTime.now(UTC)
        } catch (e: Exception) {
            throw e
        }
    }

    private suspend fun updateMembership() {
        try {
            logger.info("Fetching cluster membership...")
            val now = OffsetDateTime.now(UTC).toInstant().epochSecond
            logger.debug("Filtering ttl against: $now")
            val response =
                dynamoDbClient.scan(
                    ScanRequest {
                        tableName = this@DynamoDbAggregateCoordinator.tableName
                        filterExpression = "#ttl > :now"
                        expressionAttributeNames = mapOf("#ttl" to "ttl")
                        expressionAttributeValues =
                            mapOf(
                                ":now" to AttributeValue.N(now.toString()),
                            )
                    },
                )

            val activeNodes =
                response.items?.mapNotNull { item ->
                    item["nodeId"]?.asS()?.also { nodeId ->
                        logger.debug("Node $nodeId is active")
                    }
                } ?: emptyList()

            logger.debug("Found ${activeNodes.size} active members: ${activeNodes.joinToString(", ")}")
            clusterMembers.value = activeNodes

            logger.debug("Updated cluster members: $activeNodes")
            lastScanTime = OffsetDateTime.now(UTC)
        } catch (e: Exception) {
            throw e
        }
    }

    private fun hash(combined: String): Long {
        val md = MessageDigest.getInstance("MD5")
        val digest = md.digest(combined.toByteArray())
        return digest.take(8).foldIndexed(0L) { index, acc, byte ->
            acc or ((byte.toLong() and 0xFF) shl (index * 8))
        }
    }

    class Builder(
        private val nodeId: String,
        private val dynamoDbClient: DynamoDbClient,
    ) {
        private var tableName: String = "cluster_membership"
        private var heartbeatInterval: Duration = 5.seconds
        private var ttl: Duration = 6.seconds

        fun tableName(name: String) = apply { this.tableName = name }

        fun heartbeatInterval(interval: Duration) = apply { this.heartbeatInterval = interval }

        fun ttl(ttl: Duration) = apply { this.ttl = ttl }

        fun build() =
            DynamoDbAggregateCoordinator(
                tableName = tableName,
                nodeId = nodeId,
                dynamoDbClient = dynamoDbClient,
                heartbeatInterval = heartbeatInterval,
                ttl = ttl,
            )
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(DynamoDbAggregateCoordinator::class.java)

        fun builder(
            nodeId: String,
            dynamoDbClient: DynamoDbClient,
        ) = Builder(nodeId, dynamoDbClient)
    }
}
