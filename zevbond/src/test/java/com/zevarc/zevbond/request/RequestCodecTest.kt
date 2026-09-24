package com.zevarc.zevbond.request

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class RequestCodecTest {

    @Test
    fun `request round trip preserves type and data`() {
        val request = Request(RequestType("device.info"), byteArrayOf(1, 2, 3))

        val decoded = RequestCodec.decode(RequestCodec.encode(request))

        assertEquals("device.info", decoded.type.value)
        assertArrayEquals(byteArrayOf(1, 2, 3), decoded.data)
    }

    @Test
    fun `request with empty data round trips`() {
        val request = Request(RequestType("ping"), ByteArray(0))

        val decoded = RequestCodec.decode(RequestCodec.encode(request))

        assertEquals("ping", decoded.type.value)
        assertArrayEquals(ByteArray(0), decoded.data)
    }

    @Test
    fun `rejects empty type`() {
        assertThrows(IllegalArgumentException::class.java) {
            RequestCodec.encode(Request(RequestType(""), ByteArray(0)))
        }
    }

    @Test
    fun `rejects type longer than 256 bytes`() {
        val request = Request(RequestType("x".repeat(257)), ByteArray(0))

        assertThrows(IllegalArgumentException::class.java) {
            RequestCodec.encode(request)
        }
    }

    @Test
    fun `rejects truncated payload`() {
        val bytes = RequestCodec.encode(Request(RequestType("abc"), byteArrayOf(1)))
        // Type length says 4, only 2 follow.
        val truncated = bytes.copyOfRange(0, 4)

        assertThrows(IllegalArgumentException::class.java) {
            RequestCodec.decode(truncated)
        }
    }
}
