package com.zevarc.zevhub.handshake

import com.zevarc.zevhub.device.DeviceId
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * HELLO payload: protocol version, device id, public key placeholder,
 * 32-byte nonce. AUTH payload: signature placeholder. Both are the minimal
 * v1 encodings; the crypto contract is intentionally unfilled until the
 * identity stage.
 */
internal object HandshakeCodec {

    fun encodeHello(deviceId: DeviceId, nonce: ByteArray): ByteArray {
        val id = deviceId.value.toByteArray(Charsets.UTF_8)
        val buffer = ByteBuffer.allocate(1 + 2 + id.size + 4 + nonce.size)
            .order(ByteOrder.BIG_ENDIAN)
        buffer.put(PROTOCOL_VERSION.toByte())
        buffer.putShort(id.size.toShort())
        buffer.put(id)
        buffer.putInt(PUBLIC_KEY_PLACEHOLDER.size)
        buffer.put(PUBLIC_KEY_PLACEHOLDER)
        buffer.put(nonce)
        return buffer.array()
    }

    fun decodeHello(payload: ByteArray): HelloData {
        val buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN)
        val version = buffer.get().toInt() and 0xff
        val idLength = buffer.short.toInt() and 0xffff
        val id = ByteArray(idLength)
        buffer.get(id)
        val keyLength = buffer.int
        val key = ByteArray(keyLength)
        buffer.get(key)
        val nonce = ByteArray(buffer.remaining())
        buffer.get(nonce)
        return HelloData(
            version = version,
            deviceId = DeviceId(String(id, Charsets.UTF_8)),
            publicKey = key,
            nonce = nonce
        )
    }

    fun encodeAuth(signature: ByteArray): ByteArray {
        val buffer = ByteBuffer.allocate(4 + signature.size)
            .order(ByteOrder.BIG_ENDIAN)
        buffer.putInt(signature.size)
        buffer.put(signature)
        return buffer.array()
    }

    fun decodeAuth(payload: ByteArray): ByteArray {
        val buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN)
        val length = buffer.int
        val signature = ByteArray(length)
        buffer.get(signature)
        return signature
    }

    data class HelloData(
        val version: Int,
        val deviceId: DeviceId,
        val publicKey: ByteArray,
        val nonce: ByteArray
    )

    const val PROTOCOL_VERSION = 1
    private val PUBLIC_KEY_PLACEHOLDER = ByteArray(0)
    const val NONCE_SIZE = 32
}
