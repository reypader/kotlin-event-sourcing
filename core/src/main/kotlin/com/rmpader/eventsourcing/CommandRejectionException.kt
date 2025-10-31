package com.rmpader.eventsourcing

/**
 * Thrown when a command is rejected during processing.
 * Can represent local failures or remote failures propagated back.
 *
 * @param reason Human-readable error reason
 * @param errorCode Optional error code for programmatic handling
 * @param details Optional additional context (serializable data only)
 */
class CommandRejectionException(
    val reason: String,
    val errorCode: String? = null,
    val details: Map<String, String>? = null,
    val rootCause: Throwable? = null,
) : Exception(reason, rootCause) {
    /**
     * Serialize to JSON-compatible format for transport.
     */
    fun toSerializable(): SerializableError =
        SerializableError(
            reason = reason,
            errorCode = errorCode,
            details = details,
        )

    data class SerializableError(
        val reason: String,
        val errorCode: String? = null,
        val details: Map<String, String>? = null,
    ) {
        fun toException(): CommandRejectionException = CommandRejectionException(reason, errorCode, details)
    }
}
