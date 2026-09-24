package com.zevarc.zevbond.transfer

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Test

class TransferCodecTest {

    @Test
    fun `start round trip preserves type metadata and total`() {
        val payload = TransferCodec.encodeStart(
            TransferMetadata("photo", byteArrayOf(1, 2, 3)),
            totalBytes = 4096
        )

        val (metadata, total) = TransferCodec.decodeStart(payload)

        assertEquals("photo", metadata.type)
        assertArrayEquals(byteArrayOf(1, 2, 3), metadata.data)
        assertEquals(4096L, total)
    }

    @Test
    fun `unknown total round trips as null`() {
        val payload = TransferCodec.encodeStart(
            TransferMetadata("stream"), totalBytes = null
        )

        val (metadata, total) = TransferCodec.decodeStart(payload)

        assertEquals("stream", metadata.type)
        assertNull(total)
    }

    @Test
    fun `empty metadata round trips`() {
        val payload = TransferCodec.encodeStart(
            TransferMetadata("ping"), totalBytes = 0L
        )

        val (metadata, total) = TransferCodec.decodeStart(payload)

        assertEquals(0, metadata.data.size)
        assertEquals(0L, total)
    }

    @Test
    fun `rejects truncated payload`() {
        val payload = TransferCodec.encodeStart(
            TransferMetadata("abc", byteArrayOf(1)),
            totalBytes = 1
        )

        assertThrows(IllegalArgumentException::class.java) {
            TransferCodec.decodeStart(payload.copyOfRange(0, 10))
        }
    }

    @Test
    fun `rejects empty type`() {
        assertThrows(IllegalArgumentException::class.java) {
            TransferCodec.encodeStart(TransferMetadata(""), totalBytes = null)
        }
    }
}
