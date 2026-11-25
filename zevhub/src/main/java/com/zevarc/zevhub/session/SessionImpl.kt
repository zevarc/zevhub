package com.zevarc.zevhub.session

import android.util.Log
import com.zevarc.zevhub.device.Device
import com.zevarc.zevhub.message.Message
import com.zevarc.zevhub.message.MessageCodec
import com.zevarc.zevhub.protocol.FrameIdGenerator
import com.zevarc.zevhub.protocol.Protocol
import com.zevarc.zevhub.protocol.ProtocolFrame
import com.zevarc.zevhub.protocol.ProtocolKind
import com.zevarc.zevhub.request.IncomingRequest
import com.zevarc.zevhub.request.IncomingRequestImpl
import com.zevarc.zevhub.request.Request
import com.zevarc.zevhub.request.RequestCodec
import com.zevarc.zevhub.request.RequestType
import com.zevarc.zevhub.request.Response
import com.zevarc.zevhub.request.ResponseCodec
import com.zevarc.zevhub.stream.IncomingStream
import com.zevarc.zevhub.stream.IncomingStreamImpl
import com.zevarc.zevhub.stream.Stream
import com.zevarc.zevhub.stream.StreamImpl
import com.zevarc.zevhub.stream.StreamNegotiationCodec
import com.zevarc.zevhub.stream.StreamRejectedException
import com.zevarc.zevhub.stream.StreamType
import com.zevarc.zevhub.transfer.IncomingTransfer
import com.zevarc.zevhub.transfer.IncomingTransferImpl
import com.zevarc.zevhub.transfer.Transfer
import com.zevarc.zevhub.transfer.TransferCodec
import com.zevarc.zevhub.transfer.TransferDestination
import com.zevarc.zevhub.transfer.TransferFrame
import com.zevarc.zevhub.transfer.TransferImpl
import com.zevarc.zevhub.transfer.TransferSource
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
    private val frameIds: FrameIdGenerator = FrameIdGenerator(),
    /**
     * Fired once when the session reaches a terminal state, whether the peer
     * went away, the protocol failed, or close() was called. The owner uses it
     * to decide whether a reconnect makes sense.
     */
    private val onClosed: (Throwable?) -> Unit = {}
) : Session {

    private val incoming = Channel<Message>(capacity = 64)

    private val incomingRequests = Channel<IncomingRequest>(
        capacity = 64
    )

    private val incomingTransfers = Channel<IncomingTransfer>(
        capacity = 64
    )

    private val incomingStreams = Channel<IncomingStream>(
        capacity = 64
    )

    /** Open transfer contexts routed by the pump, keyed by context id. */
    private val transferContexts = ConcurrentHashMap<Long, Channel<TransferFrame>>()

    /** Open stream contexts routed by the pump, keyed by context id. */
    private val streamContexts = ConcurrentHashMap<Long, Channel<ByteArray>>()

    /**
     * Locally-opened streams keyed by context id, so a peer STREAM_CLOSE
     * can settle them.
     */
    private val peerClosedStreams = ConcurrentHashMap<Long, StreamImpl>()

    /** In-flight requests keyed by correlation id. */
    private val pendingRequests =
        ConcurrentHashMap<Long, CompletableDeferred<Response>>()

    /** Guards [onClosed] so it fires exactly once. */
    private val terminated = java.util.concurrent.atomic.AtomicBoolean(false)

    private val pumpJob = scope.launch {
        // The id of the frame being decoded, for the ERROR payload when a
        // payload codec rejects a frame that was valid on the wire.
        var decodingFrameId = 0L
        try {
            protocol.receive().collect { frame ->
                decodingFrameId = frame.frameId
                when (frame.kind) {
                    ProtocolKind.MESSAGE -> {
                        val decoded = MessageCodec.decode(frame.payload)
                        val result = incoming.trySend(decoded)
                        if (result.isFailure) {
                            // The message consumer is too slow; dropping
                            // keeps the pump free for every other kind.
                            Log.w(TAG, "dropping message ${frame.frameId}: queue full")
                        }
                    }

                    ProtocolKind.REQUEST -> handleRequest(frame)

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
                        // Four frames, not sixty four. Each one carries a whole
                        // chunk, so a per-frame count is a memory size in
                        // disguise: at 512 KiB this queue would otherwise be able
                        // to hold 32 MiB of received data waiting for a consumer
                        // that may be slower than the sender.
                        val frames = Channel<TransferFrame>(capacity = 4)
                        val contextId = frame.contextId
                        transferContexts[contextId] = frames
                        val offer = IncomingTransferImpl(
                            metadata = metadata,
                            totalBytes = totalBytes,
                            contextId = contextId,
                            scope = scope,
                            protocol = protocol,
                            frameIds = frameIds,
                            frames = frames,
                            onSettled = {
                                transferContexts.remove(contextId)
                                protocol.closeContext(contextId)
                            }
                        )
                        offer.startOfferTimeout(OFFER_TIMEOUT_MS)
                        val result = incomingTransfers.trySend(offer)
                        if (result.isFailure) {
                            // Nobody is collecting offers; rejecting frees
                            // the peer instead of stalling it.
                            Log.w(TAG, "dropping transfer offer $contextId: queue full")
                            offer.reject()
                        }
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

                    ProtocolKind.STREAM_DATA -> {
                        val channel = streamContexts[frame.contextId]
                        if (channel != null) {
                            val result = channel.trySend(frame.payload)
                            if (result.isFailure) {
                                // The stream consumer is too slow; the
                                // receive buffer is bounded, so the frame
                                // is dropped to protect the session.
                                Log.w(
                                    TAG,
                                    "dropping stream frame ${frame.sequence}" +
                                        " on context ${frame.contextId}: queue full"
                                )
                            }
                        }
                        // Frames for unknown or already-closed contexts
                        // belong to a closed stream; drop them.
                    }

                    ProtocolKind.STREAM_CLOSE -> {
                        val channel = streamContexts.remove(frame.contextId)
                        if (channel != null) {
                            channel.close()
                            peerClosedStreams.remove(frame.contextId)?.settleFromPeer()
                            protocol.closeContext(frame.contextId)
                        }
                        // A close for an unknown context arrives after a
                        // local close; the context is already settled.
                    }

                    else -> Unit
                }
            }
            // The receive flow completing means the transport or protocol is
            // gone; the session is dead either way.
            incoming.close()
            incomingRequests.close()
            incomingTransfers.close(IllegalStateException("Session closed"))
            incomingStreams.close(IllegalStateException("Session closed"))
            closeTransferContexts(IllegalStateException("Session closed"))
            closeStreamContexts()
            failPendingRequests(IllegalStateException("Session closed"))
            if (terminated.compareAndSet(false, true)) {
                onClosed(null)
            }
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
            incomingStreams.close(e)
            closeTransferContexts(e)
            closeStreamContexts()
            failPendingRequests(e)
            // A cancellation means the pump was cancelled by close(); anything
            // else is the link dying on its own.
            if (terminated.compareAndSet(false, true)) {
                onClosed(e as? kotlinx.coroutines.CancellationException)
            }
        }
    }

    override fun messages(): Flow<Message> = incoming.receiveAsFlow()

    override fun requests(): Flow<IncomingRequest> =
        incomingRequests.receiveAsFlow()

    override fun transfers(): Flow<IncomingTransfer> =
        incomingTransfers.receiveAsFlow()

    override fun streams(): Flow<IncomingStream> =
        incomingStreams.receiveAsFlow()

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
        val frames = Channel<com.zevarc.zevhub.transfer.TransferFrame>(capacity = 4)
        val contextId = frameIds.next()
        val transfer = TransferImpl(
            scope = scope,
            protocol = protocol,
            frameIds = frameIds,
            contextId = contextId,
            frames = frames,
            source = source,
            onSettled = {
                transferContexts.remove(contextId)
                protocol.closeContext(contextId)
            }
        )
        transferContexts[contextId] = frames
        return transfer
    }

    override suspend fun openStream(type: StreamType): Stream {
        val contextId = frameIds.next()
        val frames = Channel<ByteArray>(capacity = FRAME_BUFFER)
        val stream = StreamImpl(
            type = type,
            contextId = contextId,
            protocol = protocol,
            frameIds = frameIds,
            frames = frames,
            onClosed = {
                streamContexts.remove(contextId)
                peerClosedStreams.remove(contextId)
                protocol.closeContext(contextId)
            }
        )
        // Registered before the offer so DATA frames the peer sends
        // right after accepting cannot race the resume of this call.
        streamContexts[contextId] = frames
        peerClosedStreams[contextId] = stream
        val response = request(
            Request(
                type = RequestType(STREAM_OPEN_TYPE),
                data = StreamNegotiationCodec.encode(contextId, type)
            )
        )
        if (response is Response.Failure) {
            streamContexts.remove(contextId)
            peerClosedStreams.remove(contextId)
            protocol.closeContext(contextId)
            throw StreamRejectedException(
                "Stream offer for ${type.value} refused: code=${response.errorCode}"
            )
        }
        return stream
    }

    override suspend fun close() {
        // Cancelling the pump ends it without unwinding through the collector's
        // catch block, so a local close has to report itself explicitly.
        // CancellationException is the marker that distinguishes this from a
        // link that died on its own.
        val alreadyTerminal = terminated.getAndSet(true)
        pumpJob.cancel()
        incoming.close()
        incomingRequests.close()
        incomingTransfers.close(IllegalStateException("Session closed"))
        incomingStreams.close(IllegalStateException("Session closed"))
        closeTransferContexts(IllegalStateException("Session closed"))
        closeStreamContexts()
        failPendingRequests(IllegalStateException("Session closed"))
        protocol.close()
        if (!alreadyTerminal) {
            onClosed(kotlinx.coroutines.CancellationException("Session closed locally"))
        }
    }

    private fun handleRequest(frame: ProtocolFrame) {
        val decoded = RequestCodec.decode(frame.payload)
        if (decoded.type.value == STREAM_OPEN_TYPE) {
            val (contextId, type) = StreamNegotiationCodec.decode(decoded.data)
            val frames = Channel<ByteArray>(capacity = FRAME_BUFFER)
            streamContexts[contextId] = frames
            val offer = IncomingStreamImpl(
                scope = scope,
                type = type,
                contextId = contextId,
                protocol = protocol,
                frameIds = frameIds,
                frames = frames,
                respondAccept = {
                    protocol.send(
                        ProtocolFrame(
                            kind = ProtocolKind.RESPONSE,
                            frameId = frameIds.next(),
                            correlationId = frame.correlationId,
                            payload = ResponseCodec.encode(Response.Success(ByteArray(0)))
                        )
                    )
                },
                respondReject = {
                    protocol.send(
                        ProtocolFrame(
                            kind = ProtocolKind.RESPONSE,
                            frameId = frameIds.next(),
                            correlationId = frame.correlationId,
                            payload = ResponseCodec.encode(
                                Response.Failure(errorCode = 403, errorData = ByteArray(0))
                            )
                        )
                    )
                },
                onRejected = {
                    streamContexts.remove(contextId)
                    protocol.closeContext(contextId)
                },
                onStreamClosed = {
                    streamContexts.remove(contextId)
                    protocol.closeContext(contextId)
                }
            )
            val result = incomingStreams.trySend(offer)
            if (result.isFailure) {
                // Nobody is collecting stream offers; refusing frees the
                // peer instead of stalling it.
                Log.w(TAG, "dropping stream offer $contextId: queue full")
                scope.launch { offer.reject() }
            }
            return
        }
        val result = incomingRequests.trySend(
            IncomingRequestImpl(
                type = decoded.type,
                data = decoded.data,
                correlationId = frame.correlationId,
                frameIds = frameIds,
                protocol = protocol
            )
        )
        if (result.isFailure) {
            Log.w(TAG, "dropping request ${frame.frameId}: queue full")
        }
    }

    private fun closeStreamContexts() {
        streamContexts.values.forEach { it.close() }
        streamContexts.clear()
        peerClosedStreams.values.forEach { it.settleFromPeer() }
        peerClosedStreams.clear()
    }

    private fun closeTransferContexts(cause: Throwable) {
        transferContexts.values.forEach { it.close(cause) }
        transferContexts.clear()
        peerClosedStreams.values.forEach { it.settleFromPeer() }
        peerClosedStreams.clear()
    }

    private fun failPendingRequests(cause: Throwable) {
        pendingRequests.values.forEach { it.completeExceptionally(cause) }
        pendingRequests.clear()
    }

    private companion object {
        const val TAG = "ZevHub"

        const val MAX_REQUEST_PAYLOAD = 1024 * 1024

        /** How long an unclaimed incoming transfer offer stays open. */
        const val OFFER_TIMEOUT_MS = 30_000L

        /** Frames buffered per stream before the pump starts dropping. */
        const val FRAME_BUFFER = 64

        /**
         * Request type reserved by the session layer for stream offers;
         * it never surfaces through [requests].
         */
        const val STREAM_OPEN_TYPE = "__stream.open"
    }
}
