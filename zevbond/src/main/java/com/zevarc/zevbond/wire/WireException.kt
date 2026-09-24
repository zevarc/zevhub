package com.zevarc.zevbond.wire

/**
 * Fatal wire-level decoding failure. The connection must be closed.
 */
class WireException(
    message: String,
    val code: Int = CODE_INVALID_FRAME
) : Exception(message) {
    companion object {
        const val CODE_INVALID_FRAME = 0x0001
        const val CODE_UNSUPPORTED_VERSION = 0x0002
        const val CODE_UNSUPPORTED_KIND = 0x0003
        const val CODE_INVALID_PAYLOAD = 0x0004
        const val CODE_INVALID_CONTEXT = 0x0005
        const val CODE_INVALID_CORRELATION = 0x0006
        const val CODE_PROTOCOL_VIOLATION = 0x0007
        const val CODE_INTERNAL_ERROR = 0x0008
    }
}
