package com.zevarc.zevhub.message

import java.nio.ByteBuffer
import java.nio.ByteOrder

internal object MessageCodec {

    fun encode(message: Message): ByteArray {
        val type = message.type.value.toByteArray(Charsets.UTF_8)

        require(type.isNotEmpty())
        require(type.size <= 256)

        return ByteBuffer.allocate(
            2 + type.size + message.data.size
        ).apply {
            order(ByteOrder.BIG_ENDIAN)

            putShort(type.size.toShort())
            put(type)
            put(message.data)
        }.array()
    }

    fun decode(payload: ByteArray): Message {
        val buffer = ByteBuffer.wrap(payload)
            .order(ByteOrder.BIG_ENDIAN)

        val typeLength = buffer.short.toInt() and 0xffff

        require(typeLength in 1..256)
        require(buffer.remaining() >= typeLength)

        val type = ByteArray(typeLength)
        buffer.get(type)

        val data = ByteArray(buffer.remaining())
        buffer.get(data)

        return Message(
            type = MessageType(
                type.toString(Charsets.UTF_8)
            ),
            data = data
        )
    }
}