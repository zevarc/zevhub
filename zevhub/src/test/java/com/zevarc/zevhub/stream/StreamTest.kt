package com.zevarc.zevhub.stream

import com.zevarc.zevhub.device.Device
import com.zevarc.zevhub.device.DeviceId
import com.zevarc.zevhub.protocol.InMemoryTransport
import com.zevarc.zevhub.protocol.ProtocolImpl
import com.zevarc.zevhub.session.SessionImpl
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class StreamTest {

    private fun pairedSessions(
        scope: CoroutineScope
    ): Pair<SessionImpl, SessionImpl> {
        val clientTransport = InMemoryTransport()
        val serverTransport = InMemoryTransport()
        clientTransport.pairWith(serverTransport)
        val client = SessionImpl(
            device = Device(DeviceId("client"), null),
            protocol = ProtocolImpl(clientTransport, scope),
            scope = scope
        )
        val server = SessionImpl(
            device = Device(DeviceId("server"), null),
            protocol = ProtocolImpl(serverTransport, scope),
            scope = scope
        )
        return client to server
    }

    @Test
    fun `open stream negotiates and frames round trip in order`() = runTest {
        val (client, server) = pairedSessions(backgroundScope)

        // The receiver collects offers and accepts the first.
        val acceptor = async {
            server.streams().first().accept()
        }
        repeat(5) { yield() }

        val stream = client.openStream(StreamType("sensor"))
        val peerStream = acceptor.await()

        // One send is one received frame, in order.
        val receiver = async {
            peerStream.receive().take(3).toList()
        }
        repeat(5) { yield() }
        stream.send(byteArrayOf(1))
        stream.send(byteArrayOf(2, 2))
        stream.send(byteArrayOf(3, 3, 3))
        repeat(5) { yield() }

        assertEquals(
            listOf(
                byteArrayOf(1).toList(),
                byteArrayOf(2, 2).toList(),
                byteArrayOf(3, 3, 3).toList()
            ),
            receiver.await().map { it.toList() }
        )
        stream.close()
        peerStream.close()
    }

    @Test
    fun `reject on the receiver fails openStream with StreamRejectedException`() = runTest {
        val (client, server) = pairedSessions(backgroundScope)

        val rejector = async {
            server.streams().first().reject()
        }
        repeat(5) { yield() }

        val failure = runCatching { client.openStream(StreamType("denied")) }
        assertTrue(failure.exceptionOrNull() is StreamRejectedException)
        rejector.await()
    }

    @Test
    fun `openStream fails when nobody collects offers`() = runTest {
        val (client, server) = pairedSessions(backgroundScope)

        repeat(5) { yield() }

        // The peer never collects streams(); the reserved request type
        // falls through to the echo-less responder side and times out
        // locally, which must surface as a failed openStream.
        val failure = runCatching {
            client.openStream(StreamType("ignored"))
        }
        assertTrue(failure.isFailure)
    }

    @Test
    fun `peer close completes the receive flow and blocks further sends`() = runTest {
        val (client, server) = pairedSessions(backgroundScope)

        val acceptor = async {
            server.streams().first().accept()
        }
        repeat(5) { yield() }
        val stream = client.openStream(StreamType("chat"))
        val peerStream = acceptor.await()

        // The frame the peer will still get before the close lands.
        stream.send(byteArrayOf(9))
        repeat(5) { yield() }
        peerStream.close()
        repeat(5) { yield() }

        // The frame sent before the close is still delivered, and the
        // peer's close ends the stream locally too.
        assertEquals(byteArrayOf(9).toList(), peerStream.receive().first().toList())
        val failure = runCatching { stream.send(byteArrayOf(10)) }
        assertTrue(failure.isFailure)
        stream.close()
    }

    @Test
    fun `local close is idempotent and the peer flow completes`() = runTest {
        val (client, server) = pairedSessions(backgroundScope)

        val acceptor = async {
            server.streams().first().accept()
        }
        repeat(5) { yield() }
        val stream = client.openStream(StreamType("dup"))
        val peerStream = acceptor.await()

        stream.close()
        stream.close() // idempotent

        // The peer's receive flow completes after the close.
        withTimeout(1_000) {
            peerStream.receive().collect {}
        }
    }

    @Test
    fun `reserved negotiation type never surfaces through requests`() = runTest {
        val (client, server) = pairedSessions(backgroundScope)

        val acceptor = async {
            server.streams().first().accept()
        }
        repeat(5) { yield() }
        client.openStream(StreamType("hidden"))
        repeat(5) { yield() }

        // The offer was accepted above, so the session is intact and a
        // plain request still reaches the peer's requests() flow.
        val answered = CompletableDeferred<Unit>()
        val responder = launch {
            server.requests().collect { incoming ->
                incoming.respond(
                    com.zevarc.zevhub.request.Response.Success(incoming.data)
                )
                answered.complete(Unit)
            }
        }
        repeat(5) { yield() }
        val response = client.request(
            com.zevarc.zevhub.request.Request(
                com.zevarc.zevhub.request.RequestType("ping"),
                "pong".toByteArray()
            ),
            timeoutMs = 1_000
        )
        answered.await()
        assertTrue(response is com.zevarc.zevhub.request.Response.Success)
        responder.cancel()
    }

    @Test
    fun `session death fails stream sends`() = runTest {
        val clientTransport = InMemoryTransport()
        val serverTransport = InMemoryTransport()
        clientTransport.pairWith(serverTransport)
        val client = SessionImpl(
            device = Device(DeviceId("client"), null),
            protocol = ProtocolImpl(clientTransport, backgroundScope),
            scope = backgroundScope
        )
        val server = SessionImpl(
            device = Device(DeviceId("server"), null),
            protocol = ProtocolImpl(serverTransport, backgroundScope),
            scope = backgroundScope
        )

        repeat(5) { yield() }
        val acceptor = async { server.streams().first().accept() }
        val stream = client.openStream(StreamType("doomed"))
        val peerStream = acceptor.await()

        clientTransport.failFromRemote(IllegalStateException("link down"))
        repeat(5) { yield() }

        val failure = runCatching { stream.send(byteArrayOf(1)) }
        assertTrue(failure.isFailure)
        stream.close()
        peerStream.close()
    }
}
