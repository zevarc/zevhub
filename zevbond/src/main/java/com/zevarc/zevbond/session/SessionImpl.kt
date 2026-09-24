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
import com.zevarc.zevbond.transfer.IncomingTransfer
import com.zevarc.zevbond.transfer.IncomingTransferImpl
import com.zevarc.zevbond.transfer.Transfer
import com.zevarc.zevbond.transfer.TransferCodec
import com.zevarc.zevbond.transfer.TransferDestination
import com.zevarc.zevbond.transfer.TransferFrame
import com.zevarc.zevbond.transfer.TransferImpl
import com.zevarc.zevbond.transfer.TransferSource
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

    private val incomingTransfers = Channel<IncomingTransfer>(
        capacity = 64
    )

    /** Open transfer contexts routed by the pump, keyed by context id. */
    private val transferContexts = ConcurrentHashMap<Long, Channel<TransferFrame>>()

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

                    ProtocolKind.TRANSFER_START -> {
                        val (metadata, totalBytes) = TransferCodec.decodeStart(frame.payload)
                        val frames = Channel<TransferFrame>(capacity = 64)
                        transferContexts[frame.contextId] = frames
                        incomingTransfers.send(
                            IncomingTransferImpl(
                                metadata = metadata,
                                totalBytes = totalBytes,
                                contextId = frame.contextId,
                                scope = scope,
                                protocol = protocol,
                                frameIds = frameIds,
                                frames = frames
                            )
                        )
                    }

                    ProtocolKind.TRANSFER_DATA,
                    ProtocolKind.TRANSFER_COMPLETE,
                    ProtocolKind.TRANSFER_CANCEL -> {
                        val channel = transferContexts[frame.contextId]
                        if (channel != null) {
                            channel.send(TransferFrame(frame.kind, frame.payload))
                            if (frame.kind != ProtocolKind.TRANSFER_DATA) {
                                // The context is settled; stop routing.
                                transferContexts.remove(frame.contextId)
                            }
                        }
                        // Frames for unknown or already-settled contexts
                        // belong to a cancelled transfer; drop them.
                    }

                    else -> Unit
                }
            }
            // The receive flow completing means the transport or protocol is
            // gone; the session is dead either way.
            incoming.close()
            incomingRequests.close()
            incomingTransfers.close(IllegalStateException("Session closed"))
            closeTransferContexts(IllegalStateException("Session closed"))
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
            incomingTransfers.close(e)
            closeTransferContexts(e)
            failPendingRequests(e)
        }
    }

    override fun messages(): Flow<Message> = incoming.receiveAsFlow()

    override fun requests(): Flow<IncomingRequest> =
        incomingRequests.receiveAsFlow()

    override fun transfers(): Flow<IncomingTransfer> =
        incomingTransfers.receiveAsFlow()

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

    override fun transfer(source: TransferSource): Transfer {
        val frames = Channel<com.zevarc.zevbond.transfer.TransferFrame>(capacity = 64)
        val transfer = TransferImpl(
            scope = scope,
            protocol = protocol,
            frameIds = frameIds,
            frames = frames,
            source = source
        )
        transferContexts[transfer.contextId] = frames
        return transfer
    }

    override suspend fun close() {
        pumpJob.cancel()
        incoming.close()
        incomingRequests.close()
        incomingTransfers.close(IllegalStateException("Session closed"))
        closeTransferContexts(IllegalStateException("Session closed"))
        protocol.close()
    }

    private fun closeTransferContexts(cause: Throwable) {
        transferContexts.values.forEach { it.close(cause) }
        transferContexts.clear()
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
