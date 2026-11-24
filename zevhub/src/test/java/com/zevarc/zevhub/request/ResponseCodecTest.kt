package com.zevarc.zevhub.request

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class ResponseCodecTest {

    @Test
    fun `success round trip preserves data`() {
        val response = Response.Success(byteArrayOf(9, 8, 7))

        val decoded = ResponseCodec.decode(ResponseCodec.encode(response))

        assertTrue(decoded is Response.Success)
        assertArrayEquals(byteArrayOf(9, 8, 7), (decoded as Response.Success).data)
    }

    @Test
    fun `success with empty data round trips`() {
        val response = Response.Success(ByteArray(0))

        val decoded = ResponseCodec.decode(ResponseCodec.encode(response))

        assertTrue(decoded is Response.Success)
    }

    @Test
    fun `failure round trip preserves error code and data`() {
        val response = Response.Failure(errorCode = 0x1234, errorData = byteArrayOf(5))

        val decoded = ResponseCodec.decode(ResponseCodec.encode(response))

        assertTrue(decoded is Response.Failure)
        decoded as Response.Failure
        assertEquals(0x1234, decoded.errorCode)
        assertArrayEquals(byteArrayOf(5), decoded.errorData)
    }

    @Test
    fun `rejects empty payload`() {
        assertThrows(IllegalArgumentException::class.java) {
            ResponseCodec.decode(ByteArray(0))
        }
    }

    @Test
    fun `rejects invalid status`() {
        assertThrows(IllegalArgumentException::class.java) {
            ResponseCodec.decode(byteArrayOf(0x02))
        }
    }

    @Test
    fun `rejects failure without error code`() {
        val bytes = ResponseCodec.encode(Response.Success(ByteArray(0)))
        bytes[0] = ResponseCodec.STATUS_FAILURE.toByte()

        assertThrows(IllegalArgumentException::class.java) {
            ResponseCodec.decode(bytes)
        }
    }
}
