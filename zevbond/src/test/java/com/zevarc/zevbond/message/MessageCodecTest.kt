package com.zevarc.zevbond.message

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class MessageCodecTest {

    @Test
    fun `round trip preserves type and data`() {
        val message = Message(
            type = MessageType("chat"),
            data = byteArrayOf(1, 2, 3)
        )

        val decoded = MessageCodec.decode(MessageCodec.encode(message))

        assertEquals("chat", decoded.type.value)
        assertArrayEquals(byteArrayOf(1, 2, 3), decoded.data)
    }

    @Test
    fun `empty data is allowed`() {
        val message = Message(MessageType("event"), ByteArray(0))

        val decoded = MessageCodec.decode(MessageCodec.encode(message))

        assertEquals("event", decoded.type.value)
        assertEquals(0, decoded.data.size)
    }

    @Test
    fun `type of exactly 256 utf-8 bytes is allowed`() {
        val type = "a".repeat(256)
        val message = Message(MessageType(type), byteArrayOf(7))

        val decoded = MessageCodec.decode(MessageCodec.encode(message))

        assertEquals(256, decoded.type.value.length)
        assertArrayEquals(byteArrayOf(7), decoded.data)
    }

    @Test
    fun `encode rejects empty type`() {
        assertThrows(IllegalArgumentException::class.java) {
            MessageCodec.encode(Message(MessageType(""), ByteArray(0)))
        }
    }

    @Test
    fun `encode rejects type above 256 utf-8 bytes`() {
        assertThrows(IllegalArgumentException::class.java) {
            MessageCodec.encode(Message(MessageType("a".repeat(257)), ByteArray(0)))
        }
    }

    @Test
    fun `decode rejects payload shorter than the type length`() {
        // Type length claims 5 bytes but only 3 follow.
        val payload = byteArrayOf(0, 5, 'a'.code.toByte(), 'b'.code.toByte(), 'c'.code.toByte())

        assertThrows(Exception::class.java) {
            MessageCodec.decode(payload)
        }
    }
}
