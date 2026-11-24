package com.zevarc.zevhub.protocol

import com.zevarc.zevhub.device.DeviceId
import com.zevarc.zevhub.handshake.Handshake
import com.zevarc.zevhub.wire.WireDecoder
import com.zevarc.zevhub.wire.WireException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ProtocolImplTest {

    @Test
    fun `handshake completes on both sides of an in-memory pipe`() = runTest {
        val (client, server) = pairedProtocols(backgroundScope)

        val serverSide = async {
            Handshake(DeviceId("BB"), FrameIdGenerator())
                .run(server, initiator = false)
        }
        val peerOfA = Handshake(DeviceId("AA"), FrameIdGenerator())
            .run(client, initiator = true)

        assertEquals("BB", peerOfA.value)
        assertEquals("AA", serverSide.await().value)
    }

    /** A paired client/server protocol over an in-memory byte pipe. */
    private fun pairedProtocols(
        scope: kotlinx.coroutines.CoroutineScope
    ): Pair<ProtocolImpl, ProtocolImpl> {
        val clientTransport = InMemoryTransport()
        val serverTransport = InMemoryTransport()
        clientTransport.pairWith(serverTransport)
        return ProtocolImpl(clientTransport, scope) to
            ProtocolImpl(serverTransport, scope)
    }

    @Test
    fun `receiving a message produces an ack frame naming its frame id`() = runTest {
        val (clientTransport, serverTransport) = InMemoryTransport().let {
            it to InMemoryTransport().also(it::pairWith)
        }
        val server = ProtocolImpl(serverTransport, backgroundScope)

        clientTransport.send(
            com.zevarc.zevhub.wire.WireCodec.encode(
                com.zevarc.zevhub.wire.WireFrame(
                    version = com.zevarc.zevhub.wire.WireConstants.VERSION,
                    kind = 0x10, // MESSAGE
                    frameId = 100,
                    contextId = 0,
                    correlationId = 0,
                    sequence = 0,
                    payload = ByteArray(0)
                )
            )
        )

        assertEquals(ProtocolKind.MESSAGE, withTimeout(1_000) { server.receiveOne().kind })

        // The ACK is visible in the raw sent chunks of the server side.
        val ack = withTimeout(1_000) {
            while (serverTransport.sentChunks.isEmpty()) {
                delay(10)
            }
            WireDecoder().feed(serverTransport.sentChunks.first())
        }
        assertEquals(1, ack.size)
        assertEquals(ProtocolKind.ACK.value, ack[0].kind)
        assertTrue(ack[0].frameId > 0)
        assertArrayEquals(byteArrayOf(0, 0, 0, 0, 0, 0, 0, 100), ack[0].payload)
    }

    @Test
    fun `ack frames never reach the session layer`() = runTest {
        val (client, server) = pairedProtocols(backgroundScope)

        client.send(ProtocolFrame(ProtocolKind.MESSAGE, frameId = 5))

        val received = withTimeout(1_000) { server.receiveOne() }
        assertEquals(ProtocolKind.MESSAGE, received.kind)
    }

    @Test
    fun `protocol error frames from the peer are fatal and not echoed`() = runTest {
        val clientTransport = InMemoryTransport()
        val serverTransport = InMemoryTransport()
        clientTransport.pairWith(serverTransport)
        val server = ProtocolImpl(serverTransport, backgroundScope)

        clientTransport.send(
            com.zevarc.zevhub.wire.WireCodec.encode(
                com.zevarc.zevhub.wire.WireFrame(
                    version = com.zevarc.zevhub.wire.WireConstants.VERSION,
                    kind = 0x71, // ERROR
                    frameId = 9,
                    contextId = 0,
                    correlationId = 0,
                    sequence = 0,
                    payload = ByteArray(10)
                )
            )
        )

        val failure = withTimeout(1_000) {
            try {
                server.receiveOne()
                null
            } catch (e: Exception) {
                e
            }
        }
        assertTrue(failure is PeerErrorException)

        // No ERROR is echoed back to the peer.
        delay(100)
        assertTrue(serverTransport.sentChunks.isEmpty())
    }

    @Test
    fun `out of order transfer data fails the protocol with a violation code`() = runTest {
        val clientTransport = InMemoryTransport()
        val serverTransport = InMemoryTransport()
        clientTransport.pairWith(serverTransport)
        val server = ProtocolImpl(serverTransport, backgroundScope)

        fun transferData(frameId: Long, sequence: Int) =
            com.zevarc.zevhub.wire.WireCodec.encode(
                com.zevarc.zevhub.wire.WireFrame(
                    version = com.zevarc.zevhub.wire.WireConstants.VERSION,
                    kind = 0x31, // TRANSFER_DATA
                    frameId = frameId,
                    contextId = 5,
                    correlationId = 0,
                    sequence = sequence,
                    payload = byteArrayOf(1)
                )
            )

        clientTransport.send(transferData(1, 0))
        assertEquals(0, withTimeout(1_000) { server.receiveOne().sequence })

        clientTransport.send(transferData(2, 2)) // gap: 1 was expected

        val failure = withTimeout(1_000) {
            try {
                server.receiveOne()
                null
            } catch (e: Exception) {
                e
            }
        }
        assertTrue(failure is WireException)
        assertEquals(
            WireException.CODE_PROTOCOL_VIOLATION,
            (failure as WireException).code
        )
    }

    @Test
    fun `heartbeats are consumed by the protocol and never reach the session`() = runTest {
        val (client, server) = pairedProtocols(backgroundScope)

        client.send(ProtocolFrame(ProtocolKind.HEARTBEAT, frameId = 7))
        // A real frame right behind the probe proves the probe was swallowed
        // rather than queued: the session sees the message, not the heartbeat.
        client.send(ProtocolFrame(ProtocolKind.MESSAGE, frameId = 8))

        val received = withTimeout(1_000) { server.receiveOne() }
        assertEquals(ProtocolKind.MESSAGE, received.kind)
    }

    @Test
    fun `a silent peer is declared dead once the heartbeat window expires`() = runTest {
        // A 10ms probe interval shrinks the production 12s watchdog to ~60ms, so
        // the interval is injectable. Real time is used on purpose: the watchdog
        // runs in a background scope the test scheduler does not drive.
        val clientTransport = InMemoryTransport()
        val serverTransport = InMemoryTransport()
        clientTransport.pairWith(serverTransport)
        val server = ProtocolImpl(serverTransport, backgroundScope, heartbeatIntervalMs = 10L)

        // Nothing is ever written to the server's transport, so the only thing
        // that can end its receive flow is the watchdog.
        val failure = runCatching {
            withContext(Dispatchers.Default) {
                withTimeout(5_000) { server.receiveOne() }
            }
        }.exceptionOrNull()

        assertTrue("expected the watchdog to give up, got $failure", failure != null)
        assertTrue(
            "expected a watchdog exception, got $failure",
            failure !is TimeoutCancellationException
        )
    }
}
