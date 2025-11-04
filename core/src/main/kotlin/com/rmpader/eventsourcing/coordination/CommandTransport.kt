package com.rmpader.eventsourcing.coordination

import com.rmpader.eventsourcing.AggregateKey

interface CommandTransport {
    suspend fun sendToNode(
        nodeId: String,
        aggregateKey: AggregateKey,
        commandId: String,
        commandData: ByteArray,
    ): ByteArray
}
