package com.zevarc.zevhub.protocol

import com.zevarc.zevhub.transport.ByteTransport
import com.zevarc.zevhub.wire.WireCodec
import com.zevarc.zevhub.wire.WireConstants
import com.zevarc.zevhub.wire.WireDecoder
import com.zevarc.zevhub.wire.WireException
import com.zevarc.zevhub.wire.WireFrame
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
    private val frameIds: FrameIdGenerator = FrameIdGenerator(),
    /**
     * Probe spacing. Injectable so tests can drive the watchdog in milliseconds
     * instead of waiting out the production 3s interval.
     */
    private val heartbeatIntervalMs: Long = HEARTBEAT_INTERVAL_MS
) : Protocol {

    private val decoder = WireDecoder()

    private val incoming = Channel<ProtocolFrame>(capacity = 64)

    /** Next expected sequence per Transfer/Stream context. */
    private val expectedSequence = ConcurrentHashMap<Long, Int>()

    /**
     * Consecutive probes sent without hearing anything back. A peer that leaves
     * the P2P group can leave a TCP connection silently open for minutes, during
     * which every send() would report success.
     *
     * Counted in probes rather than wall-clock time so the watchdog is driven
     * purely by coroutine delay and therefore testable with a virtual clock.
     */
    @Volatile
    private var silentProbes: Int = 0

    private val heartbeatJob = scope.launch {
        while (true) {
            kotlinx.coroutines.delay(heartbeatIntervalMs)
            if (silentProbes >= MAX_SILENT_PROBES) {
                protocolError(
                    0L,
                    WireException(
                        "Peer silent for ${MAX_SILENT_PROBES * heartbeatIntervalMs}ms"
                    )
                )
                break
            }
            silentProbes++
            // A probe must never take the slot a real frame is waiting for, so
            // it stays out of the queued send path: a full queue skips this
            // round and the next one tries again.
            runCatching {
                send(
                    ProtocolFrame(
                        kind = ProtocolKind.HEARTBEAT,
                        frameId = frameIds.next()
                    )
                )
            }
        }
    }

    private val decodeJob = scope.launch {
        try {
            transport.receive().collect { bytes ->
                // Any byte proves the peer is alive. Resetting only when a whole
                // frame decoded meant a large frame in flight counted as silence:
                // a megabyte arrives as sixty-four reads, and if they took longer
                // than the watchdog window the peer was declared dead mid-transfer
                // and the connection reset under it.
                silentProbes = 0
                for (frame in decoder.feed(bytes)) {
                    validate(frame)
                    when (frame.kind) {
                        ProtocolKind.MESSAGE.value -> {
                            sendAck(frame.frameId)
                            incoming.send(toProtocolFrame(frame))
                        }

                        // ACK and HEARTBEAT are protocol-internal; the session
                        // layer never sees them.
                        ProtocolKind.ACK.value,
                        ProtocolKind.HEARTBEAT.value -> Unit

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
        heartbeatJob.cancel()
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

    private companion object {
        /** How often a probe is sent when the peer is otherwise silent. */
        const val HEARTBEAT_INTERVAL_MS = 3_000L

        /**
         * Probes unanswered before the peer is declared gone: 20 x 3s = 60s.
         *
         * Generous because silence is now measured in bytes, not frames, but a
         * stalled write on a busy device can still hold the decode loop off for
         * seconds. A watchdog that fires into that window kills working
         * transfers, which is far worse than noticing a dead peer a minute late.
         */
        const val MAX_SILENT_PROBES = 20
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
