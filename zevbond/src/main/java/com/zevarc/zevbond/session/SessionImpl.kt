package com.zevarc.zevbond.session

import android.util.Log
import com.zevarc.zevbond.device.Device
import com.zevarc.zevbond.message.Message
import com.zevarc.zevbond.message.MessageCodec
import com.zevarc.zevbond.protocol.FrameIdGenerator
import com.zevarc.zevbond.protocol.Protocol
import com.zevarc.zevbond.protocol.ProtocolFrame
import com.zevarc.zevbond.protocol.ProtocolKind
import com.zevarc.zevbond.request.IncomingRequest
import com.zevarc.zevbond.request.IncomingRequestImpl
import com.zevarc.zevbond.request.Request
import com.zevarc.zevbond.request.RequestCodec
import com.zevarc.zevbond.request.Response
import com.zevarc.zevbond.request.ResponseCodec
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import java.util.concurrent.ConcurrentHashMap

internal class SessionImpl(
    override val device: Device,
    private val protocol: Protocol,
    private val scope: kotlinx.coroutines.CoroutineScope,
    private val frameIds: FrameIdGenerator = FrameIdGenerator()
) : Session {

    private val incoming = Channel<Message>(capacity = 64)

    private val incomingRequests = Channel<IncomingRequest>(
        capacity = 64
    )

    /** In-flight requests keyed by correlation id. */
    private val pendingRequests =
        ConcurrentHashMap<Long, CompletableDeferred<Response>>()

    private val pumpJob = scope.launch {
        // The id of the frame being decoded, for the ERROR payload when a
        // payload codec rejects a frame that was valid on the wire.
        var decodingFrameId = 0L
        try {
            protocol.receive().collect { frame ->
                decodingFrameId = frame.frameId
                when (frame.kind) {
                    ProtocolKind.MESSAGE ->
                        incoming.send(MessageCodec.decode(frame.payload))

                    ProtocolKind.REQUEST -> {
                        val decoded = RequestCodec.decode(frame.payload)
                        incomingRequests.send(
                            IncomingRequestImpl(
                                type = decoded.type,
                                data = decoded.data,
                                correlationId = frame.correlationId,
                                frameIds = frameIds,
                                protocol = protocol
                            )
                        )
                    }

                    ProtocolKind.RESPONSE -> {
                        val deferred = pendingRequests.remove(frame.correlationId)
                        if (deferred != null) {
                            deferred.complete(ResponseCodec.decode(frame.payload))
                        } else {
                            // A late response to a timed-out or cancelled
                            // request; drop it, the session stays usable.
                            Log.w(TAG, "dropping orphan response ${frame.correlationId}")
                        }
                    }

                    else -> Unit
                }
            }
            // The receive flow completing means the transport or protocol is
            // gone; the session is dead either way.
            incoming.close()
            incomingRequests.close()
            failPendingRequests(IllegalStateException("Session closed"))
        } catch (e: Exception) {
            // The frame was valid on the wire but its payload broke the
            // codec: report it to the peer before dying (spec §13).
            if (e is IllegalArgumentException) {
                runCatching {
                    protocol.signalProtocolError(decodingFrameId, e)
                }
            }
            incoming.close(e)
            incomingRequests.close(e)
            failPendingRequests(e)
        }
    }

    override fun messages(): Flow<Message> = incoming.receiveAsFlow()

    override fun requests(): Flow<IncomingRequest> =
        incomingRequests.receiveAsFlow()

    override suspend fun send(message: Message) {
        protocol.send(
            ProtocolFrame(
                kind = ProtocolKind.MESSAGE,
                frameId = frameIds.next(),
                payload = MessageCodec.encode(message)
            )
        )
    }

    override suspend fun request(
        request: Request,
        timeoutMs: Long
    ): Response {
        val encoded = RequestCodec.encode(request)
        // Payload length is a UInt32 and the wire spec caps it at 1 MiB;
        // failing here locally avoids poisoning the peer's session.
        require(encoded.size <= MAX_REQUEST_PAYLOAD) {
            "Request payload exceeds the 1 MiB wire limit"
        }

        val correlationId = frameIds.next()
        val deferred = CompletableDeferred<Response>()
        pendingRequests[correlationId] = deferred
        try {
            return withTimeout(timeoutMs) {
                protocol.send(
                    ProtocolFrame(
                        kind = ProtocolKind.REQUEST,
                        frameId = frameIds.next(),
                        correlationId = correlationId,
                        payload = encoded
                    )
                )
                deferred.await()
            }
        } finally {
            pendingRequests.remove(correlationId)
        }
    }

    override suspend fun close() {
        pumpJob.cancel()
        incoming.close()
        incomingRequests.close()
        protocol.close()
    }

    private fun failPendingRequests(cause: Throwable) {
        pendingRequests.values.forEach { it.completeExceptionally(cause) }
        pendingRequests.clear()
    }

    private companion object {
        const val TAG = "ZevBond"

        const val MAX_REQUEST_PAYLOAD = 1024 * 1024
    }
}
