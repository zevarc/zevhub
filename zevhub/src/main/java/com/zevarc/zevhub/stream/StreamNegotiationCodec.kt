package com.zevarc.zevhub.stream

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Session-level stream negotiation payload: the context id the opener
 * assigned, then the stream type. Both sides see the same bytes, so the
 * offer and the acceptance name the same context.
 */
internal object StreamNegotiationCodec {

    fun encode(contextId: Long, type: StreamType): ByteArray {
        val typeBytes = type.value.toByteArray(Charsets.UTF_8)
        require(typeBytes.isNotEmpty()) { "Stream type must not be empty" }
        require(typeBytes.size <= 256) { "Stream type exceeds 256 bytes" }
        return ByteBuffer.allocate(8 + 2 + typeBytes.size).apply {
            putLong(contextId)
            putShort(typeBytes.size.toShort())
            put(typeBytes)
        }.array()
    }

    fun decode(payload: ByteArray): Pair<Long, StreamType> {
        require(payload.size >= 10) { "Truncated stream offer" }
        val buffer = ByteBuffer.wrap(payload)
        val contextId = buffer.long
        val typeLength = buffer.short.toInt() and 0xffff
        require(typeLength in 1..256) { "Invalid stream type length: $typeLength" }
        require(buffer.remaining() >= typeLength) { "Truncated stream type" }
        val type = ByteArray(typeLength)
        buffer.get(type)
        return contextId to StreamType(type.toString(Charsets.UTF_8))
    }
}
