package com.zevarc.zevhub.stream

import com.zevarc.zevhub.protocol.FrameIdGenerator
import com.zevarc.zevhub.protocol.Protocol
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

/**
 * A stream offer from the peer. [accept] answers the peer's negotiation
 * request and wires the already-routed frame channel into a
 * [StreamImpl]; [reject] answers the peer's negotiation request with a
 * failure and releases the context.
 */
internal class IncomingStreamImpl(
    private val scope: CoroutineScope,
    override val type: StreamType,
    private val contextId: Long,
    private val protocol: Protocol,
    private val frameIds: FrameIdGenerator,
    private val frames: Channel<ByteArray>,
    private val respondAccept: suspend () -> Unit,
    private val respondReject: suspend () -> Unit,
    private val onRejected: () -> Unit,
    private val onStreamClosed: () -> Unit
) : IncomingStream {

    private val decided = CompletableDeferred<Unit>()

    override fun accept(): Stream {
        check(decided.complete(Unit)) { "Stream already accepted or rejected" }
        scope.launch { respondAccept() }
        return StreamImpl(
            type = type,
            contextId = contextId,
            protocol = protocol,
            frameIds = frameIds,
            frames = frames,
            onClosed = onStreamClosed
        )
    }

    override suspend fun reject() {
        check(decided.complete(Unit)) { "Stream already accepted or rejected" }
        respondReject()
        onRejected()
    }
}
