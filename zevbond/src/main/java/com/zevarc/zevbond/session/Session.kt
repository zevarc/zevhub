package com.zevarc.zevbond.session

import com.zevarc.zevbond.device.Device
import com.zevarc.zevbond.message.Message
import com.zevarc.zevbond.request.IncomingRequest
import com.zevarc.zevbond.request.Request
import com.zevarc.zevbond.request.Response
import kotlinx.coroutines.flow.Flow

interface Session {

    val device: Device

    suspend fun send(message: Message)

    fun messages(): Flow<Message>

    /**
     * Performs a request/response exchange with the peer. Concurrent
     * requests are supported; responses arriving out of order resume the
     * correct caller.
     *
     * The timeout is a local operation semantic: expiring or cancelling the
     * call stops only the local wait and never affects the peer. A business
     * failure is returned as [com.zevarc.zevbond.request.Response.Failure],
     * not thrown; transport or protocol death throws.
     */
    suspend fun request(
        request: com.zevarc.zevbond.request.Request,
        timeoutMs: Long = DEFAULT_REQUEST_TIMEOUT_MS
    ): com.zevarc.zevbond.request.Response

    /**
     * Streams requests made by the peer. Answer each one through
     * [com.zevarc.zevbond.request.IncomingRequest.respond]; the flow must
     * be collected for the peer's requests to be processed.
     */
    fun requests(): Flow<com.zevarc.zevbond.request.IncomingRequest>

    suspend fun close()

    companion object {
        const val DEFAULT_REQUEST_TIMEOUT_MS = 30_000L
    }
}
