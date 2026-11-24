package com.zevarc.zevhub.wire

internal data class WireFrame(
    val version: Byte,
    val kind: Byte,
    val frameId: Long,
    val contextId: Long,
    val correlationId: Long,
    val sequence: Int,
    val payload: ByteArray
)