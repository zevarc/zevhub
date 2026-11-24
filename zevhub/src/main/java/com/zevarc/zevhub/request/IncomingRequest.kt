package com.zevarc.zevhub.request

import kotlinx.coroutines.flow.Flow

/**
 * A request made by the peer, delivered through [com.zevarc.zevhub.session.Session.requests].
 * The receiver must answer exactly once with [respond]; failing to respond
 * eventually times the caller out on the peer side.
 */
interface IncomingRequest {

    val type: RequestType

    val data: ByteArray

    /**
     * Answers the request. One-shot: a second call throws
     * [IllegalStateException].
     */
    suspend fun respond(response: Response)
}
