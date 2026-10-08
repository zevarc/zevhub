package com.zevarc.zevbond.session

import com.zevarc.zevbond.device.Device
import com.zevarc.zevbond.message.Message
import com.zevarc.zevbond.request.IncomingRequest
import com.zevarc.zevbond.request.Request
import com.zevarc.zevbond.request.Response
import com.zevarc.zevbond.stream.IncomingStream
import com.zevarc.zevbond.stream.Stream
import com.zevarc.zevbond.stream.StreamRejectedException
import com.zevarc.zevbond.stream.StreamType
import com.zevarc.zevbond.transfer.IncomingTransfer
import com.zevarc.zevbond.transfer.Transfer
import com.zevarc.zevbond.transfer.TransferSource
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

    /**
     * Starts sending the data of [source] to the peer and returns the
     * handle to observe it. The transfer runs only after the peer
     * accepts the offer surfaced through [transfers].
     */
    fun transfer(source: TransferSource): Transfer

    /**
     * Streams transfers offered by the peer. Each offer is delivered
     * exactly once, to the active collector; uncollected offers wait in
     * a bounded queue, and rejecting or ignoring an offer cancels the
     * peer's transfer. Answer each one through
     * [IncomingTransfer.accept] or [IncomingTransfer.reject].
     */
    fun transfers(): Flow<IncomingTransfer>

    /**
     * Offers a stream of [type] to the peer and suspends until the peer
     * accepts or refuses it. Returns the sending/receiving side on
     * acceptance; throws [StreamRejectedException] when the peer
     * refuses or does not answer in time.
     *
     * The peer sees the offer through [streams] and decides with
     * [IncomingStream.accept] or [IncomingStream.reject].
     */
    suspend fun openStream(type: StreamType): Stream

    /**
     * Streams stream offers made by the peer. Each offer is delivered
     * exactly once, to the active collector; ignoring an offer
     * eventually refuses it on the peer side. Decide each one through
     * [IncomingStream.accept] or [IncomingStream.reject].
     */
    fun streams(): Flow<IncomingStream>

    suspend fun close()

    companion object {
        const val DEFAULT_REQUEST_TIMEOUT_MS = 30_000L
    }
}
