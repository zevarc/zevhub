package com.zevarc.zevbond.session

import com.zevarc.zevbond.device.Device
import com.zevarc.zevbond.device.DeviceId
import com.zevarc.zevbond.handshake.Handshake
import com.zevarc.zevbond.message.Message
import com.zevarc.zevbond.message.MessageType
import com.zevarc.zevbond.protocol.FrameIdGenerator
import com.zevarc.zevbond.protocol.InMemoryTransport
import com.zevarc.zevbond.protocol.ProtocolImpl
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Test

class SessionImplTest {

    @Test
    fun `message sent by one session is delivered to the peer messages flow`() = runTest {
        val clientTransport = InMemoryTransport()
        val serverTransport = InMemoryTransport()
        clientTransport.pairWith(serverTransport)

        val clientProtocol = ProtocolImpl(clientTransport, backgroundScope)
        val serverProtocol = ProtocolImpl(serverTransport, backgroundScope)

        val handshakeClient = Handshake(DeviceId("AA"), FrameIdGenerator())
        val handshakeServer = Handshake(DeviceId("BB"), FrameIdGenerator())

        launch { handshakeClient.run(clientProtocol, initiator = true) }
        val peerId = handshakeServer.run(serverProtocol, initiator = false)

        val serverSession = SessionImpl(
            device = Device(peerId, null),
            protocol = serverProtocol,
            scope = backgroundScope
        )

        val clientSession = SessionImpl(
            device = Device(DeviceId("BB"), null),
            protocol = clientProtocol,
            scope = backgroundScope
        )

        val collector = launch {
            serverSession.messages().collect { }
        }

        clientSession.send(Message(MessageType("chat"), byteArrayOf(1, 2, 3)))

        // The message flows through protocol decode into the shared flow.
        val received = serverSession.messages()
        val first = kotlinx.coroutines.withTimeout(1_000) { received.first() }
        assertEquals("chat", first.type.value)
        assertArrayEquals(byteArrayOf(1, 2, 3), first.data)
        collector.cancel()
    }
}
