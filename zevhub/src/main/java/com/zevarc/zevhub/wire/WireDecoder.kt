package com.zevarc.zevhub.wire

import com.zevarc.zevhub.protocol.ProtocolKind
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * A streaming decoder for the FastAir wire protocol.
 *
 * Feed arbitrary byte chunks as they arrive from the transport; complete
 * frames are returned in order. TCP segmentation and coalescing are both
 * handled. Validation order follows the wire spec: magic, version, kind,
 * payload length, then field constraints.
 */
internal class WireDecoder {

    /**
     * Bytes of the current, incomplete frame. Grows to hold a whole frame and is
     * reused afterwards.
     *
     * The previous version rebuilt this array on every feed: a megabyte frame
     * arrives as sixty four 16 KiB reads, and copying the accumulator each time
     * moved roughly 33 MiB to deliver 1 MiB. That is quadratic in frame size,
     * which is why larger chunks measured no faster on a real device even though
     * they were faster in isolation.
     */
    private var buffer: ByteArray = ByteArray(INITIAL_CAPACITY)

    /** Bytes currently held in [buffer]. */
    private var length: Int = 0

    fun feed(bytes: ByteArray): List<WireFrame> {
        if (bytes.isEmpty()) {
            return emptyList()
        }

        append(bytes)

        val frames = ArrayList<WireFrame>(2)
        while (true) {
            if (length < WireConstants.HEADER_SIZE) {
                return frames
            }

            val header = ByteBuffer.wrap(buffer, 0, WireConstants.HEADER_SIZE)
                .order(ByteOrder.BIG_ENDIAN)

            val magic = header.short
            if (magic != WireConstants.MAGIC) {
                throw WireException("Invalid magic: ${magic.toString(16)}")
            }

            val version = header.get()
            if (version != WireConstants.VERSION) {
                throw WireException("Unsupported version: $version")
            }

            val kind = header.get()
            if (ProtocolKind.entries.none { it.value == kind }) {
                throw WireException("Unsupported kind: ${kind.toString(16)}")
            }

            header.position(PAYLOAD_LENGTH_OFFSET)
            val payloadLength = header.int
            if (payloadLength < 0 || payloadLength > WireConstants.MAX_FRAME_PAYLOAD) {
                throw WireException("Invalid payload length: $payloadLength")
            }

            val totalSize = WireConstants.HEADER_SIZE + payloadLength
            if (length < totalSize) {
                return frames
            }

            // One copy out of the accumulator: the frame's own payload is what
            // the caller keeps, so it has to be a stable array.
            val payload = buffer.copyOfRange(
                WireConstants.HEADER_SIZE,
                WireConstants.HEADER_SIZE + payloadLength
            )

            val ids = ByteBuffer.wrap(buffer, 0, WireConstants.HEADER_SIZE)
                .order(ByteOrder.BIG_ENDIAN)
            ids.position(FRAME_ID_OFFSET)
            val frameId = ids.long
            val contextId = ids.long
            val correlationId = ids.long
            val sequence = ids.int

            validateHeaderUsage(kind, frameId, contextId, correlationId, sequence)

            frames.add(
                WireFrame(
                    version = version,
                    kind = kind,
                    frameId = frameId,
                    contextId = contextId,
                    correlationId = correlationId,
                    sequence = sequence,
                    payload = payload
                )
            )

            // Drop the consumed frame by shifting the tail down, rather than
            // reallocating. The tail is whatever of the next frame already
            // arrived, normally nothing.
            val remaining = length - totalSize
            if (remaining > 0) {
                buffer.copyInto(buffer, 0, totalSize, length)
            }
            length = remaining
        }
    }

    /** Grows the accumulator and appends [bytes] at [length]. */
    private fun append(bytes: ByteArray) {
        val needed = length + bytes.size
        if (needed > buffer.size) {
            // Grow to fit at least this frame, doubling so a stream of frames
            // settles at one allocation instead of one per frame.
            var capacity = buffer.size.coerceAtLeast(INITIAL_CAPACITY)
            while (capacity < needed) {
                capacity *= 2
            }
            buffer = buffer.copyOf(capacity)
        }
        bytes.copyInto(buffer, length)
        length = needed
    }

    /**
     * Header field constraints from the wire spec: correlation ids only on
     * REQUEST/RESPONSE, context ids only on Transfer/Stream frames, non-zero
     * frame ids everywhere, zero sequences everywhere except data frames.
     */
    private fun validateHeaderUsage(
        kind: Byte,
        frameId: Long,
        contextId: Long,
        correlationId: Long,
        sequence: Int
    ) {
        if (frameId == 0L) {
            throw WireException("Zero frame id", WireException.CODE_INVALID_FRAME)
        }
        val withCorrelation = kind == ProtocolKind.REQUEST.value ||
            kind == ProtocolKind.RESPONSE.value
        val withContext = kind == ProtocolKind.TRANSFER_START.value ||
            kind == ProtocolKind.TRANSFER_DATA.value ||
            kind == ProtocolKind.TRANSFER_COMPLETE.value ||
            kind == ProtocolKind.TRANSFER_CANCEL.value ||
            kind == ProtocolKind.STREAM_DATA.value ||
            kind == ProtocolKind.STREAM_CLOSE.value
        val withSequence = kind == ProtocolKind.TRANSFER_DATA.value ||
            kind == ProtocolKind.STREAM_DATA.value
        when {
            withCorrelation && correlationId == 0L ->
                throw WireException(
                    "Missing correlation id on kind $kind",
                    WireException.CODE_INVALID_CORRELATION
                )

            !withCorrelation && correlationId != 0L ->
                throw WireException(
                    "Unexpected correlation id on kind $kind",
                    WireException.CODE_INVALID_CORRELATION
                )

            withContext && contextId == 0L ->
                throw WireException(
                    "Missing context id on kind $kind",
                    WireException.CODE_INVALID_CONTEXT
                )

            !withContext && contextId != 0L ->
                throw WireException(
                    "Unexpected context id on kind $kind",
                    WireException.CODE_INVALID_CONTEXT
                )

            !withSequence && sequence != 0 ->
                throw WireException(
                    "Unexpected sequence on kind $kind",
                    WireException.CODE_INVALID_FRAME
                )
        }
    }

    companion object {
        /** Big enough for an ordinary frame, small enough to be cheap at startup. */
        private const val INITIAL_CAPACITY = 64 * 1024

        private const val FRAME_ID_OFFSET = 4
        private const val PAYLOAD_LENGTH_OFFSET = 32
    }
}
