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

    /**
     * Reports a fatal protocol violation detected above the framing layer.
     * Sends ERROR to the peer when possible, then closes the protocol.
     */
    suspend fun signalProtocolError(offendingFrameId: Long, cause: Exception)

    /**
     * Releases the protocol state kept for one Transfer/Stream context.
     * Called by the session once a context has settled; frames for the
     * context arriving afterwards are protocol violations.
     */
    fun closeContext(contextId: Long)
}
