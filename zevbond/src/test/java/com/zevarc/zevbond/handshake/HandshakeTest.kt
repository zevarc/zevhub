package com.zevarc.zevbond.handshake

import com.zevarc.zevbond.device.DeviceId
import com.zevarc.zevbond.protocol.FrameIdGenerator
import com.zevarc.zevbond.protocol.InMemoryTransport
import com.zevarc.zevbond.protocol.ProtocolImpl
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Test

class HandshakeTest {

    @Test
    fun `cross-check passes when the claimed id matches the advertised uuid`() = runTest {
        val clientTransport = InMemoryTransport()
        val serverTransport = InMemoryTransport()
        clientTransport.pairWith(serverTransport)

        val clientProtocol = ProtocolImpl(clientTransport, backgroundScope)
        val serverProtocol = ProtocolImpl(serverTransport, backgroundScope)

        val serverSide = async {
            Handshake(DeviceId("BB"), FrameIdGenerator()).run(serverProtocol, initiator = false)
        }
        val peerId = Handshake(
            DeviceId("AA"),
            FrameIdGenerator(),
            expectedPeerDeviceId = DeviceId("BB")
        ).run(clientProtocol, initiator = true)

        assertEquals("BB", peerId.value)
        assertEquals("AA", serverSide.await().value)
    }

    @Test
    fun `cross-check failure surfaces to the caller`() = runTest {
        val clientTransport = InMemoryTransport()
        val serverTransport = InMemoryTransport()
        clientTransport.pairWith(serverTransport)

        val clientProtocol = ProtocolImpl(clientTransport, backgroundScope)
        val serverProtocol = ProtocolImpl(serverTransport, backgroundScope)

        val serverSide = async {
            // Server claims an id the client never saw in discovery.
            Handshake(DeviceId("EVIL"), FrameIdGenerator())
                .run(serverProtocol, initiator = false)
        }

        val clientSide = async {
            runCatching {
                Handshake(
                    DeviceId("AA"),
                    FrameIdGenerator(),
                    expectedPeerDeviceId = DeviceId("BB")
                ).run(clientProtocol, initiator = true)
            }
        }

        val result = clientSide.await()

        org.junit.Assert.assertTrue(result.isFailure)
        org.junit.Assert.assertTrue(
            result.exceptionOrNull()!!.message!!.contains("mismatch")
        )
        serverSide.cancel()
    }
}
