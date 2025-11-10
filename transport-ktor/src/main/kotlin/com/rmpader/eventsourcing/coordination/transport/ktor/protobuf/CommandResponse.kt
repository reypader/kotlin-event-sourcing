package com.rmpader.eventsourcing.coordination.transport.ktor.protobuf

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.protobuf.ProtoNumber

/**
 * Response from command execution.
 * Uses kotlinx.serialization with protobuf format.
 *
 * Had to create this manually instead because .proto generated files do not have @Seralizable
 */
@OptIn(ExperimentalSerializationApi::class)
@Serializable
data class CommandResponse(
    @ProtoNumber(1)
    val success: Boolean,
    @ProtoNumber(2)
    val stateData: ByteArray,
    @ProtoNumber(3)
    val errorCode: String = "",
    @ProtoNumber(4)
    val errorMessage: String = "",
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CommandResponse

        if (success != other.success) return false
        if (!stateData.contentEquals(other.stateData)) return false
        if (errorCode != other.errorCode) return false
        if (errorMessage != other.errorMessage) return false

        return true
    }

    override fun hashCode(): Int {
        var result = success.hashCode()
        result = 31 * result + stateData.contentHashCode()
        result = 31 * result + errorCode.hashCode()
        result = 31 * result + errorMessage.hashCode()
        return result
    }
}
