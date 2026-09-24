package com.zevarc.zevbond.protocol

import com.zevarc.zevbond.transport.ByteTransport
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow

/**
 * An in-memory pipe for tests: send() on one side appears in receive() on
 * the other. Sends never suspend and never drop, so test interleavings are
 * deterministic regardless of collector scheduling.
 */
class InMemoryTransport : ByteTransport {

    private val incoming = Channel<ByteArray>(Channel.UNLIMITED)

    lateinit var peer: InMemoryTransport

    override suspend fun send(data: ByteArray) {
        peer.incoming.send(data)
    }

    override fun receive(): Flow<ByteArray> = incoming.receiveAsFlow()

    override suspend fun close() {
        incoming.close()
    }

    fun pairWith(other: InMemoryTransport) {
        peer = other
        other.peer = this
    }
}
