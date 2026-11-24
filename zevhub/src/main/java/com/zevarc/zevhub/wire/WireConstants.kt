package com.zevarc.zevhub.wire


internal object WireConstants {

    const val MAGIC: Short = 0x5A56

    const val VERSION: Byte = 1

    const val HEADER_SIZE = 36

    const val MAX_FRAME_PAYLOAD = 1024 * 1024
}