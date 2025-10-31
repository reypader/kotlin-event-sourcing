package com.rmpader.eventsourcing.aws

import aws.sdk.kotlin.services.servicediscovery.ServiceDiscoveryClient
import aws.sdk.kotlin.services.servicediscovery.model.DiscoverInstancesRequest
import aws.sdk.kotlin.services.servicediscovery.model.HealthStatusFilter
import com.rmpader.eventsourcing.AggregateCoordinator
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
import java.security.MessageDigest

/**
 * AWS Cloud Map-based aggregate coordinator
 */
class CloudMapAggregateCoordinator private constructor(
    private val namespaceName: String,
    private val serviceName: String,
    private val nodeId: String,
    private val cloudMapClient: ServiceDiscoveryClient,
    private val pollIntervalMs: Long = 30_000, // Poll less frequently (Cloud Map caches)
) : AggregateCoordinator {
    private val clusterMembers = MutableStateFlow<Set<String>>(emptySet())

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var pollJob: Job? = null

    override fun start() {
        scope.launch {
            updateClusterMembers()
        }

        pollJob =
            scope.launch {
                while (isActive) {
                    delay(pollIntervalMs)
                    updateClusterMembers()
                }
            }
    }

    override fun stop() {
        pollJob?.cancel()
        scope.cancel()
        runBlocking {
            cloudMapClient.close()
        }
    }

    override fun getAggregateOwnner(aggregateId: String): String? {
        val members = clusterMembers.value
        if (members.isEmpty()) return null

        val targetNode =
            members.maxByOrNull { member ->
                hash("$aggregateId:$member")
            }

        return if (targetNode == nodeId) {
            null
        } else {
            targetNode
        }
    }

    private suspend fun updateClusterMembers() {
        try {
            val response =
                cloudMapClient.discoverInstances(
                    DiscoverInstancesRequest.Companion {
                        this.namespaceName = this@CloudMapAggregateCoordinator.namespaceName
                        this.serviceName = this@CloudMapAggregateCoordinator.serviceName
                        this.healthStatus = HealthStatusFilter.Healthy
                        this.maxResults = 100
                    },
                )

            val instances =
                response.instances
                    ?.mapNotNull { instance ->
                        // Use instance ID or IP as node identifier
                        instance.instanceId
                        // Or use custom attribute:
                        // instance.attributes?.get("POD_NAME")
                    }?.toSet() ?: emptySet()

            clusterMembers.value = instances

            println("Updated Cloud Map cluster members: $instances")
        } catch (e: Exception) {
            println("Error updating cluster members: ${e.message}")
        }
    }

    private fun hash(input: String): Long {
        val md = MessageDigest.getInstance("MD5")
        val digest = md.digest(input.toByteArray())
        return digest.take(8).foldIndexed(0L) { index, acc, byte ->
            acc or ((byte.toLong() and 0xFF) shl (index * 8))
        }
    }

    class Builder {
        private var namespaceName: String? = null
        private var serviceName: String? = null
        private var nodeId: String? = null
        private var cloudMapClient: ServiceDiscoveryClient? = null
        private var pollIntervalMs: Long = 30_000

        fun namespaceName(name: String) =
            apply {
                this.namespaceName = name
            }

        fun serviceName(name: String) =
            apply {
                this.serviceName = name
            }

        fun nodeId(id: String) =
            apply {
                this.nodeId = id
            }

        fun pollIntervalMs(interval: Long) =
            apply {
                this.pollIntervalMs = interval
            }

        fun cloudMapClient(client: ServiceDiscoveryClient) =
            apply {
                this.cloudMapClient = client
            }

        suspend fun build(): CloudMapAggregateCoordinator {
            val resolvedNamespace =
                namespaceName
                    ?: throw IllegalStateException("namespaceName is required")

            val resolvedService =
                serviceName
                    ?: throw IllegalStateException("serviceName is required")

            val resolvedNodeId =
                nodeId
                    ?: System.getenv("HOSTNAME")
                    ?: throw IllegalStateException("nodeId must be set")

            val resolvedClient =
                cloudMapClient
                    ?: ServiceDiscoveryClient.Companion.fromEnvironment()

            return CloudMapAggregateCoordinator(
                namespaceName = resolvedNamespace,
                serviceName = resolvedService,
                nodeId = resolvedNodeId,
                cloudMapClient = resolvedClient,
                pollIntervalMs = pollIntervalMs,
            )
        }
    }

    companion object {
        fun builder() = Builder()
    }
}
