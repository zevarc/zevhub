package com.zevarc.zevbond.wire

import com.zevarc.zevbond.protocol.ProtocolKind
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
class WireDecoder {

    private var buffer: ByteArray = ByteArray(0)
    private var length: Int = 0

    fun feed(bytes: ByteArray): List<WireFrame> {
        if (bytes.isEmpty()) {
            return emptyList()
        }

        val merged = ByteArray(length + bytes.size)
        buffer.copyInto(merged, 0, 0, length)
        bytes.copyInto(merged, length)
        buffer = merged
        length = merged.size

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

            buffer = buffer.copyOfRange(totalSize, length)
            length -= totalSize
        }
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
        private const val FRAME_ID_OFFSET = 4
        private const val PAYLOAD_LENGTH_OFFSET = 32
    }
}
