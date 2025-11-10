package com.rmpader.eventsourcing.coordination.transport.ktor.protobuf

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.protobuf.ProtoNumber

/**
 * Command envelope for transporting commands between nodes.
 * Uses kotlinx.serialization with protobuf format.
 *
 * Had to create this manually instead because .proto generated files do not have @Seralizable
 */
@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class CommandEnvelope(
    @ProtoNumber(1)
    val aggregateAlias: String,
    @ProtoNumber(2)
    val aggregateKey: String,
    @ProtoNumber(3)
    val commandId: String,
    @ProtoNumber(4)
    val commandData: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CommandEnvelope

        if (aggregateAlias != other.aggregateAlias) return false
        if (aggregateKey != other.aggregateKey) return false
        if (commandId != other.commandId) return false
        if (!commandData.contentEquals(other.commandData)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = aggregateAlias.hashCode()
        result = 31 * result + aggregateKey.hashCode()
        result = 31 * result + commandId.hashCode()
        result = 31 * result + commandData.contentHashCode()
        return result
    }
}
