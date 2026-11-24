package com.zevarc.zevhub.session

import com.zevarc.zevhub.device.Device
import com.zevarc.zevhub.device.DeviceId
import com.zevarc.zevhub.message.Message
import com.zevarc.zevhub.message.MessageType
import com.zevarc.zevhub.protocol.InMemoryTransport
import com.zevarc.zevhub.protocol.ProtocolImpl
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class SessionImplTest {

    private fun pair(
        scope: CoroutineScope
    ): Triple<InMemoryTransport, InMemoryTransport, Pair<ProtocolImpl, ProtocolImpl>> {
        val clientTransport = InMemoryTransport()
        val serverTransport = InMemoryTransport()
        clientTransport.pairWith(serverTransport)
        return Triple(
            clientTransport,
            serverTransport,
            ProtocolImpl(clientTransport, scope) to ProtocolImpl(serverTransport, scope)
        )
    }

    private fun sessionFor(
        protocol: ProtocolImpl,
        scope: CoroutineScope
    ) = SessionImpl(
        device = Device(DeviceId("peer"), null),
        protocol = protocol,
        scope = scope
    )

    @Test
    fun `message sent by one session is delivered to the peer messages flow`() = runTest {
        val (_, _, protocols) = pair(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols

        val serverSession = sessionFor(serverProtocol, backgroundScope)
        val clientSession = sessionFor(clientProtocol, backgroundScope)

        val received = async {
            serverSession.messages().first()
        }
        // Give the async collector a chance to subscribe before sending.
        repeat(5) { yield() }
        clientSession.send(Message(MessageType("chat"), byteArrayOf(1, 2, 3)))

        val message = withTimeout(1_000) { received.await() }
        assertEquals("chat", message.type.value)
        assertArrayEquals(byteArrayOf(1, 2, 3), message.data)
    }

    @Test
    fun `messages flow ends when the transport closes`() = runTest {
        val (clientTransport, _, protocols) = pair(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols

        val serverSession = sessionFor(serverProtocol, backgroundScope)
        val clientSession = sessionFor(clientProtocol, backgroundScope)

        val done = CompletableDeferred<Unit>()
        val collector = launch {
            try {
                withTimeout(1_000) { serverSession.messages().collect { } }
            } finally {
                done.complete(Unit)
            }
        }
        // Let the collector start before killing the transport.
        clientSession.send(Message(MessageType("chat"), byteArrayOf(1)))
        clientTransport.close()

        withTimeout(1_000) { done.await() }
        collector.cancel()
    }

    @Test
    fun `messages flow fails when the transport fails`() = runTest {
        val (clientTransport, _, protocols) = pair(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols

        val serverSession = sessionFor(serverProtocol, backgroundScope)
        val clientSession = sessionFor(clientProtocol, backgroundScope)

        val outcome = CompletableDeferred<Result<Unit>>()
        val collector = launch {
            try {
                withTimeout(1_000) {
                    serverSession.messages().collect { }
                }
                outcome.complete(Result.success(Unit))
            } catch (e: Throwable) {
                outcome.complete(Result.failure(e))
            }
        }
        // Let the collector start, then kill the transport with a failure.
        clientSession.send(Message(MessageType("chat"), byteArrayOf(1)))
        clientTransport.failFromRemote(IllegalStateException("link down"))

        val result = withTimeout(1_000) { outcome.await() }
        assertTrue(result.isFailure)
        collector.cancel()
    }
}
