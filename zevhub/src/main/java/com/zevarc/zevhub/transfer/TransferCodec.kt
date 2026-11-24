package com.zevarc.zevhub.transfer

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Encodes TRANSFER_START payloads per wire spec section 9: the transfer
 * type (2-byte length + UTF-8), then the opaque metadata with a UInt32
 * length prefix.
 */
internal object TransferCodec {

    fun encodeStart(metadata: TransferMetadata, totalBytes: Long?): ByteArray {
        val type = metadata.type.toByteArray(Charsets.UTF_8)

        require(type.isNotEmpty())
        require(type.size <= 256)

        // The sender communicates an unknown total with -1 on the wire.
        val declaredTotal = totalBytes ?: -1L

        return ByteBuffer.allocate(2 + type.size + 8 + 4 + metadata.data.size).apply {
            order(ByteOrder.BIG_ENDIAN)
            putShort(type.size.toShort())
            put(type)
            putLong(declaredTotal)
            putInt(metadata.data.size)
            put(metadata.data)
        }.array()
    }

    fun decodeStart(payload: ByteArray): Pair<TransferMetadata, Long?> {
        require(payload.size >= 14)

        val buffer = ByteBuffer.wrap(payload)
            .order(ByteOrder.BIG_ENDIAN)

        val typeLength = buffer.short.toInt() and 0xffff
        require(typeLength in 1..256)
        require(buffer.remaining() >= typeLength + 12)

        val type = ByteArray(typeLength)
        buffer.get(type)

        val declaredTotal = buffer.long
        val metadataLength = buffer.int
        require(metadataLength >= 0)
        require(buffer.remaining() >= metadataLength)

        val metadata = ByteArray(metadataLength)
        buffer.get(metadata)

        return TransferMetadata(
            type = type.toString(Charsets.UTF_8),
            data = metadata
        ) to (declaredTotal.takeIf { it >= 0 })
    }
}
