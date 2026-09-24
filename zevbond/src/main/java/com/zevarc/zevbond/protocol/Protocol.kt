package com.zevarc.zevbond.protocol

import kotlinx.coroutines.flow.Flow

internal interface Protocol {

    suspend fun send(frame: ProtocolFrame)

    /**
     * Suspend until the next frame arrives. Consumers must be strictly
     * sequential: handshake first, then the session's receive loop.
     */
    suspend fun receiveOne(): ProtocolFrame

    fun receive(): Flow<ProtocolFrame>

    suspend fun close()
}
