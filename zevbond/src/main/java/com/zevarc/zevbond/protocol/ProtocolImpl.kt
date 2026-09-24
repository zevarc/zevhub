package com.zevarc.zevbond.protocol

import com.zevarc.zevbond.transport.ByteTransport
import com.zevarc.zevbond.wire.WireCodec
import com.zevarc.zevbond.wire.WireConstants
import com.zevarc.zevbond.wire.WireDecoder
import com.zevarc.zevbond.wire.WireException
import com.zevarc.zevbond.wire.WireFrame
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap

/**
 * Frames the transport byte stream and applies the v1 protocol semantics:
 * automatic ACK for received MESSAGE frames, protocol ERROR signalling on
 * fatal decode failures, and per-context sequence validation for
 * TRANSFER_DATA / STREAM_DATA.
 */
internal class ProtocolImpl(
    private val transport: ByteTransport,
    private val scope: CoroutineScope,
    private val frameIds: FrameIdGenerator = FrameIdGenerator()
) : Protocol {

    private val decoder = WireDecoder()

    private val incoming = Channel<ProtocolFrame>(capacity = 64)

    /** Next expected sequence per Transfer/Stream context. */
    private val expectedSequence = ConcurrentHashMap<Long, Int>()

    private val decodeJob = scope.launch {
        try {
            transport.receive().collect { bytes ->
                for (frame in decoder.feed(bytes)) {
                    validate(frame)
                    when (frame.kind) {
                        ProtocolKind.MESSAGE.value -> {
                            sendAck(frame.frameId)
                            incoming.send(toProtocolFrame(frame))
                        }

                        // ACK is a protocol-internal confirmation; the
                        // session layer never sees it.
                        ProtocolKind.ACK.value -> Unit

                        else -> incoming.send(toProtocolFrame(frame))
                    }
                }
            }
            incoming.close()
        } catch (e: Exception) {
            protocolError(0L, e)
        }
    }

    override suspend fun send(frame: ProtocolFrame) {
        transport.send(
            WireCodec.encode(
                WireFrame(
                    version = WireConstants.VERSION,
                    kind = frame.kind.value,
                    frameId = frame.frameId,
                    contextId = frame.contextId,
                    correlationId = frame.correlationId,
                    sequence = frame.sequence,
                    payload = frame.payload
                )
            )
        )
    }

    override suspend fun receiveOne(): ProtocolFrame = incoming.receive()

    override fun receive(): Flow<ProtocolFrame> = incoming.consumeAsFlow()

    override suspend fun close() {
        decodeJob.cancel()
        incoming.close()
        transport.close()
    }

    private fun toProtocolFrame(frame: WireFrame): ProtocolFrame =
        ProtocolFrame(
            kind = ProtocolKind.from(frame.kind),
            frameId = frame.frameId,
            contextId = frame.contextId,
            correlationId = frame.correlationId,
            sequence = frame.sequence,
            payload = frame.payload
        )

    private suspend fun sendAck(acknowledgedFrameId: Long) {
        runCatching {
            send(
                ProtocolFrame(
                    kind = ProtocolKind.ACK,
                    frameId = frameIds.next(),
                    payload = encodeU64(acknowledgedFrameId)
                )
            )
        }
    }

    override suspend fun signalProtocolError(offendingFrameId: Long, cause: Exception) {
        protocolError(offendingFrameId, cause)
    }

    override fun closeContext(contextId: Long) {
        expectedSequence.remove(contextId)
    }

    /**
     * A decode, validation, or peer ERROR frame is fatal: report ERROR to
     * the peer when possible, then tear the protocol down.
     */
    private suspend fun protocolError(offendingFrameId: Long, cause: Exception) {
        // Echoing a peer's ERROR back would be a loop; only announce errors
        // that originated locally.
        val peerSignalled = cause is PeerErrorException
        if (!peerSignalled) {
            val errorCode =
                (cause as? WireException)?.code ?: WireException.CODE_INTERNAL_ERROR
            runCatching {
                send(
                    ProtocolFrame(
                        kind = ProtocolKind.ERROR,
                        frameId = frameIds.next(),
                        payload = errorPayload(errorCode, offendingFrameId)
                    )
                )
            }
        }
        incoming.close(cause)
        runCatching { transport.close() }
    }

    /**
     * Semantic validation a single frame must pass before delivery.
     * Violations raise a fatal protocol error.
     */
    private suspend fun validate(frame: WireFrame) {
        if (frame.kind == ProtocolKind.ERROR.value) {
            throw PeerErrorException()
        }
        if (frame.kind == ProtocolKind.TRANSFER_DATA.value ||
            frame.kind == ProtocolKind.STREAM_DATA.value
        ) {
            // Sequence must advance monotonically within the context.
            val expected = requireNotNull(expectedSequence.merge(frame.contextId, 1, Int::plus)) - 1
            if (frame.sequence != expected) {
                expectedSequence.remove(frame.contextId)
                throw WireException(
                    "Out-of-order sequence ${frame.sequence} on context ${frame.contextId}" +
                        " (expected $expected)",
                    WireException.CODE_PROTOCOL_VIOLATION
                )
            }
        }
    }

    private fun errorPayload(errorCode: Int, offendingFrameId: Long): ByteArray {
        val payload = ByteArray(10)
        for (i in 0 until 8) {
            payload[i] = (offendingFrameId shr ((7 - i) * 8)).toByte()
        }
        payload[8] = (errorCode shr 8).toByte()
        payload[9] = errorCode.toByte()
        return payload
    }

    private fun encodeU64(value: Long): ByteArray {
        val bytes = ByteArray(8)
        for (i in 0 until 8) {
            bytes[i] = (value shr ((7 - i) * 8)).toByte()
        }
        return bytes
    }
}

/** The peer sent an ERROR frame; the protocol dies without echoing it. */
internal class PeerErrorException : Exception("Peer signalled protocol error")
