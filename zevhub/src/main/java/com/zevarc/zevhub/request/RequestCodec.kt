package com.zevarc.zevhub.request

import java.nio.ByteBuffer
import java.nio.ByteOrder

/** Encodes [Request] payloads per wire spec section 7 (same as Message). */
internal object RequestCodec {

    fun encode(request: Request): ByteArray {
        val type = request.type.value.toByteArray(Charsets.UTF_8)

        require(type.isNotEmpty())
        require(type.size <= 256)

        return ByteBuffer.allocate(
            2 + type.size + request.data.size
        ).apply {
            order(ByteOrder.BIG_ENDIAN)

            putShort(type.size.toShort())
            put(type)
            put(request.data)
        }.array()
    }

    fun decode(payload: ByteArray): Request {
        val buffer = ByteBuffer.wrap(payload)
            .order(ByteOrder.BIG_ENDIAN)

        val typeLength = buffer.short.toInt() and 0xffff

        require(typeLength in 1..256)
        require(buffer.remaining() >= typeLength)

        val type = ByteArray(typeLength)
        buffer.get(type)

        val data = ByteArray(buffer.remaining())
        buffer.get(data)

        return Request(
            type = RequestType(
                type.toString(Charsets.UTF_8)
            ),
            data = data
        )
    }
}
