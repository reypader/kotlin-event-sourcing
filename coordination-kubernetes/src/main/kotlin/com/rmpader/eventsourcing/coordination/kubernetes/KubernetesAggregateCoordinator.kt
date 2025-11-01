package com.rmpader.eventsourcing.coordination.kubernetes

import com.rmpader.eventsourcing.coordination.AggregateCoordinator
import com.rmpader.eventsourcing.coordination.AggregateCoordinator.AggregateLocation
import io.kubernetes.client.informer.ResourceEventHandler
import io.kubernetes.client.informer.SharedIndexInformer
import io.kubernetes.client.informer.SharedInformerFactory
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1PodList
import io.kubernetes.client.util.Config
import kotlinx.coroutines.flow.MutableStateFlow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
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
        logger.info("Starting KubernetesAggregateCoordinator...")
        podInformer =
            informerFactory.sharedIndexInformerFor(
                { params ->
                    logger.debug(
                        "Creating informer for pods with properties: " +
                            "resourceVersion=${params.resourceVersion}, " +
                            "timeoutSeconds=${params.timeoutSeconds}, " +
                            "watch=${params.watch}, " +
                            "namespace=$namespace, " +
                            "labelSelector=$labelSelector",
                    )
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
        logger.info("Registering Event Handler for pod changes...")
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

        logger.info("Starting Informers...")
        informerFactory.startAllRegisteredInformers()

        logger.info("Initial cluster membership update...")
        updateClusterMembers()
    }

    override fun stop() {
        logger.info("Stopping Informers...")
        informerFactory.stopAllRegisteredInformers()
    }

    override fun locateAggregate(aggregateId: String): AggregateLocation {
        val members = clusterMembers.value
        logger.info("Locating aggregate $aggregateId...")

        val targetNode =
            members.maxByOrNull { member ->
                hash("$aggregateId:$member")
            }

        return if (targetNode == null || targetNode == nodeId) {
            logger.info("Aggregate is local")
            AggregateLocation.Local
        } else {
            logger.info("Aggregate is located remotely: $targetNode")
            AggregateLocation.Remote(targetNode)
        }
    }

    private fun updateClusterMembers() {
        try {
            logger.info("Fetching cluster membership...")
            val pods = podInformer.indexer.list()
            logger.debug("Found ${pods.size} members: ${pods.joinToString(", ") { it.metadata?.name ?: "" }}")

            val readyPods =
                pods
                    .filter { pod ->
                        pod.status?.phase == "Running" &&
                            pod.status?.conditions?.any {
                                it.type == "Ready" && it.status == "True"
                            } == true
                    }.mapNotNull {
                        logger.debug("Pod ${it.metadata?.name} is ready")
                        it.metadata?.name
                    }.toSet()

            clusterMembers.value = readyPods

            logger.info("Updated cluster members: $readyPods")
        } catch (e: Exception) {
            logger.error("Error updating cluster members: ${e.message}", e)
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
        private val logger: Logger = LoggerFactory.getLogger(KubernetesAggregateCoordinator::class.java)

        /**
         * Create a new builder
         */
        fun builder() = Builder()
    }
}
