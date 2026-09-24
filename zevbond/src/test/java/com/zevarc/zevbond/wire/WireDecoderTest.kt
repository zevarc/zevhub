package com.zevarc.zevbond.wire

import com.zevarc.zevbond.protocol.ProtocolKind
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
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

    @Test
    fun `rejects request without correlation id`() {
        val bytes = WireCodec.encode(
            frame().copy(kind = ProtocolKind.REQUEST.value, correlationId = 0)
        )

        val e = assertThrows(WireException::class.java) {
            WireDecoder().feed(bytes)
        }
        assertEquals(WireException.CODE_INVALID_CORRELATION, e.code)
    }

    @Test
    fun `rejects response without correlation id`() {
        val bytes = WireCodec.encode(
            frame().copy(kind = ProtocolKind.RESPONSE.value, correlationId = 0)
        )

        val e = assertThrows(WireException::class.java) {
            WireDecoder().feed(bytes)
        }
        assertEquals(WireException.CODE_INVALID_CORRELATION, e.code)
    }

    @Test
    fun `rejects message with a correlation id`() {
        val bytes = WireCodec.encode(frame().copy(correlationId = 7))

        val e = assertThrows(WireException::class.java) {
            WireDecoder().feed(bytes)
        }
        assertEquals(WireException.CODE_INVALID_CORRELATION, e.code)
    }

    @Test
    fun `rejects transfer frame without context id`() {
        val bytes = WireCodec.encode(
            frame().copy(kind = ProtocolKind.TRANSFER_START.value, contextId = 0)
        )

        val e = assertThrows(WireException::class.java) {
            WireDecoder().feed(bytes)
        }
        assertEquals(WireException.CODE_INVALID_CONTEXT, e.code)
    }

    @Test
    fun `rejects message with a context id`() {
        val bytes = WireCodec.encode(frame().copy(contextId = 9))

        val e = assertThrows(WireException::class.java) {
            WireDecoder().feed(bytes)
        }
        assertEquals(WireException.CODE_INVALID_CONTEXT, e.code)
    }

    @Test
    fun `rejects message with a non-zero sequence`() {
        val bytes = WireCodec.encode(frame().copy(sequence = 3))

        val e = assertThrows(WireException::class.java) {
            WireDecoder().feed(bytes)
        }
        assertEquals(WireException.CODE_INVALID_FRAME, e.code)
    }

    @Test
    fun `rejects zero frame id`() {
        val bytes = WireCodec.encode(frame().copy(frameId = 0))

        val e = assertThrows(WireException::class.java) {
            WireDecoder().feed(bytes)
        }
        assertEquals(WireException.CODE_INVALID_FRAME, e.code)
    }

    @Test
    fun `accepts request with correlation id and transfer frames with context id`() {
        val request = WireDecoder().feed(
            WireCodec.encode(
                frame().copy(kind = ProtocolKind.REQUEST.value, correlationId = 42)
            )
        )
        val transferData = WireDecoder().feed(
            WireCodec.encode(
                frame().copy(
                    kind = ProtocolKind.TRANSFER_DATA.value,
                    contextId = 7,
                    sequence = 1
                )
            )
        )

        assertEquals(1, request.size)
        assertEquals(42L, request[0].correlationId)
        assertEquals(1, transferData.size)
        assertEquals(7L, transferData[0].contextId)
    }
}
