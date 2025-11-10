package com.rmpader.eventsourcing

/**
 * Thrown when a command is rejected during processing.
 * Can represent local failures or remote failures propagated back.
 *
 * @param reason Human-readable error reason
 * @param errorCode Optional error code for programmatic handling
 */
class CommandRejectionException(
    val reason: String,
    val errorCode: String,
    val rootCause: Throwable? = null,
) : Exception(reason, rootCause)
