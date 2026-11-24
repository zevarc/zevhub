package com.zevarc.zevhub.request

/**
 * Asks the remote side to perform an operation and return a business-level
 * result.
 */
data class Request(
    val type: RequestType,
    val data: ByteArray
)
