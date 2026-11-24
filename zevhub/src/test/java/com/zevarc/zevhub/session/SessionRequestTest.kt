package com.zevarc.zevhub.session

import com.zevarc.zevhub.device.Device
import com.zevarc.zevhub.device.DeviceId
import com.zevarc.zevhub.message.Message
import com.zevarc.zevhub.message.MessageType
import com.zevarc.zevhub.protocol.InMemoryTransport
import com.zevarc.zevhub.protocol.ProtocolImpl
import com.zevarc.zevhub.request.IncomingRequest
import com.zevarc.zevhub.request.Request
import com.zevarc.zevhub.request.RequestType
import com.zevarc.zevhub.request.Response
import com.zevarc.zevhub.wire.WireDecoder
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class SessionRequestTest {

    private fun pair(
        scope: kotlinx.coroutines.CoroutineScope
    ): Pair<ProtocolImpl, ProtocolImpl> {
        val (_, _, protocols) = pairWithTransports(scope)
        return protocols
    }

    private fun pairWithTransports(
        scope: kotlinx.coroutines.CoroutineScope
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
        scope: kotlinx.coroutines.CoroutineScope
    ) = SessionImpl(
        device = Device(DeviceId("peer"), null),
        protocol = protocol,
        scope = scope
    )

    @Test
    fun `request is answered by the peer and response reaches the caller`() = runTest {
        val (clientProtocol, serverProtocol) = pair(backgroundScope)
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        val responder = launchResponder(serverSession, backgroundScope)
        val response = clientSession.request(
            Request(RequestType("device.info"), byteArrayOf(1, 2, 3))
        )

        assertTrue(response is Response.Success)
        assertArrayEquals(byteArrayOf(1, 2, 3), (response as Response.Success).data)
        responder.cancel()
    }

    @Test
    fun `concurrent requests resume their own callers despite out of order responses`() = runTest {
        val (clientProtocol, serverProtocol) = pair(backgroundScope)
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        // The responder answers in reverse order of arrival.
        val answered = kotlinx.coroutines.channels.Channel<IncomingRequest>(
            capacity = 8
        )
        val responder = backgroundScope.launch {
            serverSession.requests().collect { answered.send(it) }
        }
        launch {
            val first = answered.receive()
            val second = answered.receive()
            val third = answered.receive()
            third.respond(Response.Success("third".toByteArray()))
            second.respond(Response.Success("second".toByteArray()))
            first.respond(Response.Success("first".toByteArray()))
        }

        val r1 = async {
            clientSession.request(Request(RequestType("job"), "1".toByteArray()))
        }
        val r2 = async {
            clientSession.request(Request(RequestType("job"), "2".toByteArray()))
        }
        val r3 = async {
            clientSession.request(Request(RequestType("job"), "3".toByteArray()))
        }

        assertEquals("first", String((r1.await() as Response.Success).data))
        assertEquals("second", String((r2.await() as Response.Success).data))
        assertEquals("third", String((r3.await() as Response.Success).data))
        responder.cancel()
    }

    @Test
    fun `request times out locally without signalling the protocol`() = runTest {
        val (clientTransport, serverTransport, protocols) = pairWithTransports(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        // Nobody answers; the request must expire locally.
        val failure = runCatching {
            clientSession.request(
                Request(RequestType("slow"), byteArrayOf(1)),
                timeoutMs = 500
            )
        }.exceptionOrNull()

        assertTrue(failure is kotlinx.coroutines.TimeoutCancellationException)

        // The timeout is a local failure: no ERROR frame may be sent.
        yield()
        assertTrue(serverTransport.sentChunks.isEmpty())
    }

    @Test
    fun `cancelling a request stops the local wait and keeps the session usable`() = runTest {
        val (clientProtocol, serverProtocol) = pair(backgroundScope)
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        // A responder that withholds the answer to the first request but
        // keeps consuming, so later requests are still answered.
        val collector = launch {
            serverSession.requests().collect { incoming ->
                if (incoming.type.value != "hang") {
                    incoming.respond(Response.Success(incoming.data))
                }
            }
        }
        val caller = launch {
            clientSession.request(
                Request(RequestType("hang"), ByteArray(0)),
                timeoutMs = 60_000
            )
        }
        repeat(5) { yield() }

        caller.cancel()
        caller.join()

        // After the cancellation a normal request/response still works.
        val response = clientSession.request(
            Request(RequestType("after"), "ok".toByteArray())
        )
        assertEquals("ok", String((response as Response.Success).data))
        collector.cancel()
    }

    @Test
    fun `orphan response is dropped and the session stays usable`() = runTest {
        val (clientProtocol, serverProtocol) = pair(backgroundScope)
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        // A request that times out, then a peer that answers later.
        val lateResponder = launchResponder(serverSession, backgroundScope)
        runCatching {
            clientSession.request(
                Request(RequestType("late"), ByteArray(0)),
                timeoutMs = 50
            )
        }
        yield()

        // The session still works after the orphaned response arrives.
        val response = clientSession.request(
            Request(RequestType("next"), "ok".toByteArray())
        )
        assertEquals("ok", String((response as Response.Success).data))
        lateResponder.cancel()
    }

    @Test
    fun `failure response is delivered as a value and the session stays usable`() = runTest {
        val (clientProtocol, serverProtocol) = pair(backgroundScope)
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        val responder = launch {
            serverSession.requests().collect { incoming ->
                incoming.respond(
                    Response.Failure(errorCode = 403, errorData = "denied".toByteArray())
                )
            }
        }
        val response = clientSession.request(
            Request(RequestType("remote.lock"), byteArrayOf(1))
        )

        assertTrue(response is Response.Failure)
        response as Response.Failure
        assertEquals(403, response.errorCode)
        assertArrayEquals("denied".toByteArray(), response.errorData)

        // A business failure must not poison the session: chat still flows.
        val received = async { serverSession.messages().first() }
        repeat(5) { yield() }
        clientSession.send(Message(MessageType("chat"), byteArrayOf(1)))
        assertEquals("chat", withTimeout(1_000) { received.await().type.value })
        responder.cancel()
    }

    @Test
    fun `transport death fails pending requests instead of hanging`() = runTest {
        val (clientTransport, _, protocols) = pairWithTransports(backgroundScope)
        val clientProtocol = protocols.first
        val clientSession = sessionFor(clientProtocol, backgroundScope)

        val outcome = CompletableDeferred<Result<Response>>()
        val caller = launch {
            try {
                val response = clientSession.request(
                    Request(RequestType("hang"), ByteArray(0)),
                    timeoutMs = 60_000
                )
                outcome@ outcome.complete(Result.success(response))
            } catch (e: Throwable) {
                outcome.complete(Result.failure(e))
            }
        }
        repeat(5) { yield() }

        clientTransport.failFromRemote(IllegalStateException("link down"))

        val result = withTimeout(1_000) { outcome.await() }
        assertTrue(result.isFailure)
        caller.cancel()
    }

    @Test
    fun `local close fails pending requests instead of waiting for the timeout`() = runTest {
        val (_, _, protocols) = pairWithTransports(backgroundScope)
        val clientSession = sessionFor(protocols.first, backgroundScope)

        val outcome = CompletableDeferred<Result<Response>>()
        val caller = launch {
            try {
                // Nobody answers: only the session closing can end this wait.
                val response = clientSession.request(
                    Request(RequestType("hang"), ByteArray(0)),
                    timeoutMs = 60_000
                )
                outcome.complete(Result.success(response))
            } catch (e: Throwable) {
                outcome.complete(Result.failure(e))
            }
        }
        repeat(5) { yield() }

        clientSession.close()

        val result = withTimeout(1_000) { outcome.await() }
        assertTrue(result.isFailure)
        caller.cancel()
    }

    @Test
    fun `responding twice throws`() = runTest {
        val (clientProtocol, serverProtocol) = pair(backgroundScope)
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        val answered = CompletableDeferred<IncomingRequest>()
        val collector = launch {
            serverSession.requests().collect { answered.complete(it) }
        }
        val caller = async {
            runCatching { clientSession.request(Request(RequestType("x"), ByteArray(0)), timeoutMs = 60_000) }
        }
        val incoming = withTimeout(1_000) { answered.await() }
        incoming.respond(Response.Success(ByteArray(0)))

        val second = runCatching { incoming.respond(Response.Success(ByteArray(0))) }
            .exceptionOrNull()
        assertTrue(second is IllegalStateException)
        caller.cancel()
        collector.cancel()
    }

    @Test
    fun `malformed request payload kills the session with an error frame`() = runTest {
        val (clientTransport, serverTransport, protocols) = pairWithTransports(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        val failure = CompletableDeferred<Throwable>()
        val collector = launch {
            try {
                serverSession.requests().collect { }
            } catch (e: Throwable) {
                failure.complete(e)
            }
        }

        // A REQUEST frame whose payload violates the codec: type length 0.
        val badPayload = byteArrayOf(0, 0)
        clientProtocol.send(
            com.zevarc.zevhub.protocol.ProtocolFrame(
                kind = com.zevarc.zevhub.protocol.ProtocolKind.REQUEST,
                frameId = 1,
                correlationId = 7,
                payload = badPayload
            )
        )

        val cause = withTimeout(1_000) { failure.await() }
        assertTrue(cause is IllegalArgumentException)
    }

    @Test
    fun `oversized request fails locally without touching the wire`() = runTest {
        val (clientTransport, _, protocols) = pairWithTransports(backgroundScope)
        val clientProtocol = protocols.first
        val clientSession = sessionFor(clientProtocol, backgroundScope)

        val failure = runCatching {
            clientSession.request(
                Request(RequestType("big"), ByteArray(1024 * 1024)),
                timeoutMs = 5_000
            )
        }.exceptionOrNull()

        assertTrue(failure is IllegalArgumentException)
        // Nothing went on the wire.
        assertTrue(clientTransport.sentChunks.isEmpty())
    }

    /**
     * Answers every incoming request by echoing the request data. Runs on
     * the given scope so the collector subscribes before requests arrive.
     */
    private fun launchResponder(
        session: SessionImpl,
        scope: kotlinx.coroutines.CoroutineScope
    ) = scope.launch {
        session.requests().collect { incoming ->
            incoming.respond(Response.Success(incoming.data))
        }
    }

    @Test
    fun `session closed reports a link failure to its owner`() = runTest {
        val (clientTransport, _, protocols) = pairWithTransports(backgroundScope)
        val reported = CompletableDeferred<Throwable?>()
        val clientSession = SessionImpl(
            device = Device(DeviceId("peer"), null),
            protocol = protocols.first,
            scope = backgroundScope,
            onClosed = { reported.complete(it) }
        )

        clientTransport.failFromRemote(IllegalStateException("link down"))

        // A link that dies on its own is NOT reported as a cancellation: that
        // marker is reserved for close(), which is what the owner keys the
        // reconnect decision on.
        val cause = reported.await()
        assertTrue(
            "a dropped link must not look like a local close, got $cause",
            cause !is kotlinx.coroutines.CancellationException
        )
        clientSession.close()
    }

    @Test
    fun `closing a session locally does not look like a link failure`() = runTest {
        val (_, _, protocols) = pairWithTransports(backgroundScope)
        val reported = CompletableDeferred<Throwable?>()
        val clientSession = SessionImpl(
            device = Device(DeviceId("peer"), null),
            protocol = protocols.first,
            scope = backgroundScope,
            onClosed = { reported.complete(it) }
        )

        clientSession.close()

        val cause = withTimeout(1_000) { reported.await() }
        assertTrue(
            "a local close must not be mistaken for a dropped link, got $cause",
            cause == null || cause is kotlinx.coroutines.CancellationException
        )
    }

}
