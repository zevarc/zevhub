package com.zevarc.zevhub.wire

import com.zevarc.zevhub.protocol.ProtocolKind
import java.nio.ByteBuffer
import java.nio.ByteOrder

internal object WireCodec {

    fun encode(frame: WireFrame): ByteArray {
        val total = WireConstants.HEADER_SIZE + frame.payload.size
        return ByteBuffer.allocate(total)
            .order(ByteOrder.BIG_ENDIAN)
            .putShort(WireConstants.MAGIC)
            .put(WireConstants.VERSION)
            .put(frame.kind)
            .putLong(frame.frameId)
            .putLong(frame.contextId)
            .putLong(frame.correlationId)
            .putInt(frame.sequence)
            .putInt(frame.payload.size)
            .put(frame.payload)
            .array()
    }
}
