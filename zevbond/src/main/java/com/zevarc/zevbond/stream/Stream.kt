package com.zevarc.zevbond.stream

import kotlinx.coroutines.flow.Flow

/**
 * A continuous ordered sequence of binary frames sharing one
 * communication context. One [send] corresponds to one frame received
 * through [receive] on the peer. Frames are delivered reliably and in
 * order while the session lives, but the receive buffer is bounded: if
 * the receiving application falls too far behind, its oldest undelivered
 * frames are dropped to protect the session.
 */
interface Stream {

    val type: StreamType

    /**
     * Sends one logical frame. Suspends for backpressure while the
     * wire is not ready to carry it. Throws if the stream or the
     * session is closed.
     *
     * Frames carry a per-stream sequence that the receiving side
     * validates: sends from concurrent coroutines on one stream may
     * interleave out of order and are treated as a fatal protocol
     * violation by the peer. Send from a single coroutine, or impose
     * your own ordering.
     */
    suspend fun send(data: ByteArray)

    /**
     * Receives frames from the peer in arrival order. The flow
     * completes when the stream is closed by either side or the
     * session dies.
     */
    fun receive(): Flow<ByteArray>

    /**
     * Closes the stream on both sides. Idempotent: further calls do
     * nothing. Sending after close throws.
     */
    suspend fun close()
}
