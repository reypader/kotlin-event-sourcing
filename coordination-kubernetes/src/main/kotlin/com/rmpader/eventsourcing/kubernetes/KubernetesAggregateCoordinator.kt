package com.rmpader.eventsourcing.kubernetes

import com.rmpader.eventsourcing.AggregateCoordinator
import io.kubernetes.client.informer.ResourceEventHandler
import io.kubernetes.client.informer.SharedIndexInformer
import io.kubernetes.client.informer.SharedInformerFactory
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1PodList
import io.kubernetes.client.util.Config
import kotlinx.coroutines.flow.MutableStateFlow
import java.security.MessageDigest

/**
 * Kubernetes-native aggregate coordinator using Informer pattern for pod discovery
 */
class KubernetesAggregateCoordinator private constructor(
    private val labelSelector: String,
    private val nodeId: String,
    private val namespace: String,
    private val api: CoreV1Api,
    private val informerFactory: SharedInformerFactory,
) : AggregateCoordinator {
    private val clusterMembers = MutableStateFlow<Set<String>>(emptySet())
    private lateinit var podInformer: SharedIndexInformer<V1Pod>

    override fun start() {
        // Create informer for pods with label selector
        podInformer =
            informerFactory.sharedIndexInformerFor(
                { params ->
                    api
                        .listNamespacedPod(namespace)
                        .labelSelector(labelSelector)
                        .resourceVersion(params.resourceVersion)
                        .timeoutSeconds(params.timeoutSeconds)
                        .watch(params.watch)
                        .buildCall(null)
                },
                V1Pod::class.java,
                V1PodList::class.java,
            )
        // Add event handler for pod changes
        podInformer.addEventHandler(
            object : ResourceEventHandler<V1Pod> {
                override fun onAdd(pod: V1Pod) {
                    updateClusterMembers()
                }

                override fun onUpdate(
                    oldPod: V1Pod,
                    newPod: V1Pod,
                ) {
                    updateClusterMembers()
                }

                override fun onDelete(
                    pod: V1Pod,
                    deletedFinalStateUnknown: Boolean,
                ) {
                    updateClusterMembers()
                }
            },
        )

        // Start all informers
        informerFactory.startAllRegisteredInformers()

        // Initial update
        updateClusterMembers()
    }

    override fun stop() {
        informerFactory.stopAllRegisteredInformers()
    }

    override fun getAggregateOwnner(aggregateId: String): String? {
        val members = clusterMembers.value
        if (members.isEmpty()) return null

        val targetNode =
            members.maxByOrNull { member ->
                hash("$aggregateId:$member")
            }

        return if (targetNode == nodeId) {
            null // This node handles it
        } else {
            targetNode // Forward to this node
        }
    }

    private fun updateClusterMembers() {
        try {
            // Use informer's indexer (local cache) instead of API call
            val pods = podInformer.indexer.list()

            val readyPods =
                pods
                    .filter { pod ->
                        pod.status?.phase == "Running" &&
                            pod.status?.conditions?.any {
                                it.type == "Ready" && it.status == "True"
                            } == true
                    }.mapNotNull { it.metadata?.name }
                    .toSet()

            clusterMembers.value = readyPods

            println("Updated cluster members: $readyPods")
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

    /**
     * Builder for KubernetesAggregateCoordinator
     */
    class Builder {
        private var labelSelector: String? = null
        private var nodeId: String? = null
        private var namespace: String? = null
        private var client: ApiClient? = null

        /**
         * Set the Kubernetes label selector for discovering pods (required)
         * Example: "app=event-sourcing"
         */
        fun labelSelector(selector: String) =
            apply {
                this.labelSelector = selector
            }

        /**
         * Set this node's ID. Defaults to POD_NAME environment variable
         */
        fun nodeId(id: String) =
            apply {
                this.nodeId = id
            }

        /**
         * Set the Kubernetes namespace. Defaults to NAMESPACE env var or "default"
         */
        fun namespace(ns: String) =
            apply {
                this.namespace = ns
            }

        /**
         * Set a custom ApiClient (useful for testing)
         */
        fun client(apiClient: ApiClient) =
            apply {
                this.client = apiClient
            }

        /**
         * Build the KubernetesAggregateCoordinator
         */
        fun build(): KubernetesAggregateCoordinator {
            val resolvedLabelSelector =
                labelSelector
                    ?: throw IllegalStateException("labelSelector is required")

            val resolvedNodeId =
                nodeId
                    ?: System.getenv("POD_NAME")
                    ?: throw IllegalStateException("nodeId must be set or POD_NAME env var must exist")

            val resolvedNamespace =
                namespace
                    ?: System.getenv("NAMESPACE")
                    ?: "default"

            val resolvedClient = client ?: Config.defaultClient()
            val api = CoreV1Api(resolvedClient)
            val informerFactory = SharedInformerFactory(resolvedClient)

            return KubernetesAggregateCoordinator(
                labelSelector = resolvedLabelSelector,
                nodeId = resolvedNodeId,
                namespace = resolvedNamespace,
                api = api,
                informerFactory = informerFactory,
            )
        }
    }

    companion object {
        /**
         * Create a new builder
         */
        fun builder() = Builder()
    }
}
