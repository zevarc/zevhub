package com.zevarc.zevhub.protocol

internal data class ProtocolFrame(
    val kind: ProtocolKind,
    val frameId: Long,
    val contextId: Long = 0,
    val correlationId: Long = 0,
    val sequence: Int = 0,
    val payload: ByteArray = ByteArray(0)
)