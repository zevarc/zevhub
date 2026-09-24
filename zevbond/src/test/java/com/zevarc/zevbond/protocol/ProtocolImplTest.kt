package com.zevarc.zevbond.protocol

import com.zevarc.zevbond.device.DeviceId
import com.zevarc.zevbond.handshake.Handshake
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Test

class ProtocolImplTest {

    @Test
    fun `handshake completes on both sides of an in-memory pipe`() = runTest {
        val clientTransport = InMemoryTransport()
        val serverTransport = InMemoryTransport()
        clientTransport.pairWith(serverTransport)

        val clientProtocol = ProtocolImpl(clientTransport, backgroundScope)
        val serverProtocol = ProtocolImpl(serverTransport, backgroundScope)

        val serverSide = async {
            Handshake(DeviceId("BB"), FrameIdGenerator())
                .run(serverProtocol, initiator = false)
        }
        val peerOfA = Handshake(DeviceId("AA"), FrameIdGenerator())
            .run(clientProtocol, initiator = true)

        assertEquals("BB", peerOfA.value)
        assertEquals("AA", serverSide.await().value)
    }
}
