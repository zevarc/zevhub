package com.zevarc.zevbond.stream

import com.zevarc.zevbond.protocol.FrameIdGenerator
import com.zevarc.zevbond.protocol.Protocol
import com.zevarc.zevbond.protocol.ProtocolFrame
import com.zevarc.zevbond.protocol.ProtocolKind
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow

/**
 * One open stream over a single context id. Sends go straight to the
 * protocol (backpressure comes from the wire); received frames arrive
 * through the session-routed channel and are surfaced as a flow. Either
 * side closing ends the stream on both sides; late frames for a closed
 * context are dropped by the session pump.
 */
internal class StreamImpl(
    override val type: StreamType,
    val contextId: Long,
    private val protocol: Protocol,
    private val frameIds: FrameIdGenerator,
    private val frames: Channel<ByteArray>,
    private val onClosed: () -> Unit
) : Stream {

    /** Completes when the stream is terminal on either side. */
    private val settled = CompletableDeferred<Unit>()

    /** Only the caller's send coroutines race here; order is theirs. */
    private var nextSequence = 0

    override suspend fun send(data: ByteArray) {
        if (settled.isCompleted) {
            throw IllegalStateException("Stream is closed")
        }
        val sequence = nextSequence++
        protocol.send(
            ProtocolFrame(
                kind = ProtocolKind.STREAM_DATA,
                frameId = frameIds.next(),
                contextId = contextId,
                sequence = sequence,
                payload = data
            )
        )
    }

    override fun receive(): Flow<ByteArray> = frames.receiveAsFlow()

    override suspend fun close() {
        if (!settled.complete(Unit)) {
            return
        }
        runCatching {
            protocol.send(
                ProtocolFrame(
                    kind = ProtocolKind.STREAM_CLOSE,
                    frameId = frameIds.next(),
                    contextId = contextId
                )
            )
        }
        frames.close()
        onClosed()
    }

    /**
     * Ends the stream because the peer closed it or the session died.
     * Called by the session pump on STREAM_CLOSE or teardown; local
     * sends afterwards fail with "Stream is closed".
     */
    fun settleFromPeer() {
        if (!settled.complete(Unit)) {
            return
        }
        frames.close()
        onClosed()
    }
}
