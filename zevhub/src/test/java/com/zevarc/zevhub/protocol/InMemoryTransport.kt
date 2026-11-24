package com.zevarc.zevhub.protocol

import com.zevarc.zevhub.transport.ByteTransport
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * An in-memory pipe for tests: send() on one side appears in receive() on
 * the other. Sends never suspend and never drop, so test interleavings are
 * deterministic regardless of collector scheduling. [sentChunks] records
 * every outbound byte chunk so tests can assert on frames a Protocol sends
 * on its own (ACK, ERROR) without decoding through another Protocol.
 */
class InMemoryTransport : ByteTransport {

    private val incoming = Channel<ByteArray>(Channel.UNLIMITED)

    /** Every chunk this side has sent, in order. */
    val sentChunks = ConcurrentLinkedQueue<ByteArray>()

    lateinit var peer: InMemoryTransport

    override suspend fun send(data: ByteArray) {
        sentChunks.add(data)
        peer.incoming.send(data)
    }

    override fun receive(): Flow<ByteArray> = incoming.receiveAsFlow()

    override suspend fun close() {
        incoming.close()
        // A local close is an EOF on the peer's read side, like TCP.
        runCatching { peer.incoming.close() }
    }

    fun pairWith(other: InMemoryTransport) {
        peer = other
        other.peer = this
    }

    /** Simulates the remote side dying with a failure. */
    fun failFromRemote(cause: Throwable) {
        incoming.close(cause)
    }
}
