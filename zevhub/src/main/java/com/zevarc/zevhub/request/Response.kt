package com.zevarc.zevhub.request

/**
 * The business-level result of a [Request]. A failure carries a service
 * error code and never surfaces as a protocol exception; the session stays
 * usable after a failure response.
 */
sealed interface Response {

    data class Success(
        val data: ByteArray
    ) : Response

    data class Failure(
        val errorCode: Int,
        val errorData: ByteArray
    ) : Response
}
