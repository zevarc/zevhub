package com.zevarc.zevhub.request

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Encodes [Response] payloads per wire spec section 8: one status byte,
 * a UInt16 error code only on failure, then the result or error data.
 */
internal object ResponseCodec {

    const val STATUS_SUCCESS = 0x00
    const val STATUS_FAILURE = 0x01

    private const val FAILURE_HEADER_SIZE = 3

    fun encode(response: Response): ByteArray = when (response) {
        is Response.Success -> ByteBuffer.allocate(1 + response.data.size).apply {
            put(STATUS_SUCCESS.toByte())
            put(response.data)
        }.array()

        is Response.Failure -> ByteBuffer.allocate(
            FAILURE_HEADER_SIZE + response.errorData.size
        ).apply {
            put(STATUS_FAILURE.toByte())
            putShort(response.errorCode.toShort())
            put(response.errorData)
        }.array()
    }

    fun decode(payload: ByteArray): Response {
        require(payload.isNotEmpty())

        val status = payload[0].toInt() and 0xff
        val body = ByteBuffer.wrap(payload, 1, payload.size - 1)
            .order(ByteOrder.BIG_ENDIAN)

        return when (status) {
            STATUS_SUCCESS -> Response.Success(
                data = ByteArray(body.remaining()).also { body.get(it) }
            )

            STATUS_FAILURE -> {
                require(body.remaining() >= 2)
                Response.Failure(
                    errorCode = body.short.toInt() and 0xffff,
                    errorData = ByteArray(body.remaining()).also { body.get(it) }
                )
            }

            else -> throw IllegalArgumentException("Invalid response status: $status")
        }
    }
}
