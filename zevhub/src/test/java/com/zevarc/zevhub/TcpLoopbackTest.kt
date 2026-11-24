package com.zevarc.zevhub

import com.zevarc.zevhub.device.Device
import com.zevarc.zevhub.device.DeviceId
import com.zevarc.zevhub.handshake.Handshake
import com.zevarc.zevhub.message.Message
import com.zevarc.zevhub.message.MessageType
import com.zevarc.zevhub.protocol.FrameIdGenerator
import com.zevarc.zevhub.protocol.ProtocolImpl
import com.zevarc.zevhub.session.SessionImpl
import com.zevarc.zevhub.tcp.TcpLink
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * End-to-end over a real localhost TCP socket: everything the P2P base
 * carries except the Wi-Fi radio itself.
 */
class TcpLoopbackTest {

    @Test
    fun `handshake and message round trip over a real tcp connection`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val tcp = TcpLink(scope)
        val port = 19527

        try {
            val serverSide = scope.async {
                val transport = tcp.listen(port).first()
                val protocol = ProtocolImpl(transport, scope)
                val peerId = Handshake(DeviceId("SERVER"), FrameIdGenerator())
                    .run(protocol, initiator = false)
                SessionImpl(Device(peerId, null), protocol, scope)
            }

            val clientTransport = withTimeout(10_000) {
                // Listen must bind before connect dials, and the connect
                // retry handles the window before the bind lands.
                tcp.connect("127.0.0.1", port)
            }

            val clientProtocol = ProtocolImpl(clientTransport, scope)
            val peerId = Handshake(DeviceId("CLIENT"), FrameIdGenerator())
                .run(clientProtocol, initiator = true)
            val clientSession = SessionImpl(Device(peerId, null), clientProtocol, scope)

            val serverSession = withTimeout(10_000) { serverSide.await() }

            val received = withTimeout(10_000) {
                val waiter = scope.async { serverSession.messages().first() }
                clientSession.send(Message(MessageType("chat"), byteArrayOf(1, 2, 3)))
                waiter.await()
            }

            assertEquals("chat", received.type.value)
            assertArrayEquals(byteArrayOf(1, 2, 3), received.data)

            clientSession.close()
            serverSession.close()
        } finally {
            scope.cancel()
        }
    }
}
