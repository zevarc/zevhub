package com.zevarc.zevbond.wire

import com.zevarc.zevbond.protocol.ProtocolKind
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Test

class WireDecoderTest {

    private fun frame(payload: ByteArray = ByteArray(0)) = WireFrame(
        version = WireConstants.VERSION,
        kind = ProtocolKind.MESSAGE.value,
        frameId = 1,
        contextId = 0,
        correlationId = 0,
        sequence = 0,
        payload = payload
    )

    @Test
    fun `reassembles a frame split across chunks`() {
        val bytes = WireCodec.encode(frame(byteArrayOf(1, 2, 3, 4)))

        val decoder = WireDecoder()
        val first = decoder.feed(bytes.copyOfRange(0, 10))
        val second = decoder.feed(bytes.copyOfRange(10, 20))
        val third = decoder.feed(bytes.copyOfRange(20, bytes.size))

        assertEquals(emptyList<WireFrame>(), first)
        assertEquals(emptyList<WireFrame>(), second)
        assertEquals(1, third.size)
        assertArrayEquals(byteArrayOf(1, 2, 3, 4), third[0].payload)
    }

    @Test
    fun `decodes multiple coalesced frames in one chunk`() {
        val a = WireCodec.encode(frame(byteArrayOf(1)))
        val b = WireCodec.encode(frame(byteArrayOf(2)))

        val merged = a + b
        val decoded = WireDecoder().feed(merged)

        assertEquals(2, decoded.size)
        assertArrayEquals(byteArrayOf(1), decoded[0].payload)
        assertArrayEquals(byteArrayOf(2), decoded[1].payload)
    }

    @Test
    fun `holds a partial header until more bytes arrive`() {
        val bytes = WireCodec.encode(frame())

        val decoder = WireDecoder()
        val partial = decoder.feed(bytes.copyOfRange(0, 20))

        assertEquals(emptyList<WireFrame>(), partial)

        val rest = decoder.feed(bytes.copyOfRange(20, bytes.size))
        assertEquals(1, rest.size)
    }

    @Test
    fun `holds a partial payload until more bytes arrive`() {
        val bytes = WireCodec.encode(frame(byteArrayOf(9, 8, 7)))

        val decoder = WireDecoder()
        val partial = decoder.feed(bytes.copyOfRange(0, 38))

        assertEquals(emptyList<WireFrame>(), partial)

        val rest = decoder.feed(bytes.copyOfRange(38, bytes.size))
        assertEquals(1, rest.size)
        assertArrayEquals(byteArrayOf(9, 8, 7), rest[0].payload)
    }

    @Test
    fun `empty chunks are ignored`() {
        val decoder = WireDecoder()
        assertEquals(emptyList<WireFrame>(), decoder.feed(ByteArray(0)))
    }
}
