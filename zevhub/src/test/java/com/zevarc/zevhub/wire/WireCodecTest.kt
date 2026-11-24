package com.zevarc.zevhub.wire

import com.zevarc.zevhub.protocol.ProtocolKind
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class WireCodecTest {

    private fun frame(payload: ByteArray = ByteArray(0)) = WireFrame(
        version = WireConstants.VERSION,
        kind = ProtocolKind.MESSAGE.value,
        frameId = 0x0102030405060708L,
        contextId = 0L,
        correlationId = 0L,
        sequence = 0,
        payload = payload
    )

    /** TRANSFER_DATA carries context id and sequence per the header rules. */
    private fun contextFrame(payload: ByteArray = ByteArray(0)) = WireFrame(
        version = WireConstants.VERSION,
        kind = ProtocolKind.TRANSFER_DATA.value,
        frameId = 0x0102030405060708L,
        contextId = 0x1112131415161718L,
        correlationId = 0L,
        sequence = 1,
        payload = payload
    )

    @Test
    fun `header layout matches the frozen wire spec`() {
        val bytes = WireCodec.encode(frame())

        assertEquals(36, bytes.size)

        // Big endian, field order: magic, version, kind, frameId, contextId,
        // correlationId, sequence, payloadLength.
        assertEquals(0x5A, bytes[0].toInt() and 0xff)
        assertEquals(0x56, bytes[1].toInt() and 0xff)
        assertEquals(1, bytes[2].toInt() and 0xff)
        assertEquals(0x10, bytes[3].toInt() and 0xff)
        assertEquals(0x01, bytes[4].toInt() and 0xff)
        assertEquals(0x02, bytes[5].toInt() and 0xff)
        assertEquals(0x03, bytes[6].toInt() and 0xff)
        assertEquals(0x04, bytes[7].toInt() and 0xff)
        assertEquals(0x05, bytes[8].toInt() and 0xff)
        assertEquals(0x06, bytes[9].toInt() and 0xff)
        assertEquals(0x07, bytes[10].toInt() and 0xff)
        assertEquals(0x08, bytes[11].toInt() and 0xff)
        assertEquals(0, bytes[32].toInt())
        assertEquals(0, bytes[33].toInt())
        assertEquals(0, bytes[34].toInt())
        assertEquals(0, bytes[35].toInt())
    }

    @Test
    fun `round trip preserves all fields`() {
        val payload = byteArrayOf(1, 2, 3, 4, 5)
        val bytes = WireCodec.encode(contextFrame(payload))

        val decoded = WireDecoder().feed(bytes)

        assertEquals(1, decoded.size)
        val f = decoded[0]
        assertEquals(ProtocolKind.TRANSFER_DATA.value, f.kind)
        assertEquals(0x0102030405060708L, f.frameId)
        assertEquals(0x1112131415161718L, f.contextId)
        assertEquals(1, f.sequence)
        assertArrayEquals(payload, f.payload)
    }

    @Test
    fun `round trip at the one MiB payload boundary`() {
        val payload = ByteArray(WireConstants.MAX_FRAME_PAYLOAD)
        val bytes = WireCodec.encode(frame(payload))
        val decoded = WireDecoder().feed(bytes)

        assertEquals(1, decoded.size)
        assertEquals(WireConstants.MAX_FRAME_PAYLOAD, decoded[0].payload.size)
    }

    @Test
    fun `rejects payload above one MiB`() {
        val header = WireCodec.encode(frame(ByteArray(1))).copyOfRange(0, 36)
        // Overwrite payload length with 1 MiB + 1.
        header[32] = 0x00
        header[33] = 0x10
        header[34] = 0x00
        header[35] = 0x01

        assertThrows(WireException::class.java) {
            WireDecoder().feed(header)
        }
    }

    @Test
    fun `rejects invalid magic`() {
        val bytes = WireCodec.encode(frame())
        bytes[0] = 0x00

        assertThrows(WireException::class.java) {
            WireDecoder().feed(bytes)
        }
    }

    @Test
    fun `rejects unsupported version`() {
        val bytes = WireCodec.encode(frame())
        bytes[2] = 99

        assertThrows(WireException::class.java) {
            WireDecoder().feed(bytes)
        }
    }

    @Test
    fun `rejects unsupported kind`() {
        val bytes = WireCodec.encode(frame())
        bytes[3] = 0x7F

        assertThrows(WireException::class.java) {
            WireDecoder().feed(bytes)
        }
    }
}
