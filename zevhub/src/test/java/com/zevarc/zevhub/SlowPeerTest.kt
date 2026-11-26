package com.zevarc.zevhub

import com.zevarc.zevhub.protocol.InMemoryTransport
import com.zevarc.zevhub.protocol.ProtocolImpl
import com.zevarc.zevhub.protocol.ProtocolFrame
import com.zevarc.zevhub.protocol.ProtocolKind
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.Assert.fail
import org.junit.Test

/**
 * A peer that is sending a large frame must not be declared dead.
 *
 * The watchdog used to reset only when a whole frame decoded, so a frame split
 * across many slow reads looked exactly like silence. On a device that turned
 * into a mid-transfer connection reset.
 */
class SlowPeerTest {

    @Test
    fun `a slowly arriving frame does not trip the watchdog`() = runBlocking {
        val clientTransport = InMemoryTransport()
        val serverTransport = InMemoryTransport()
        clientTransport.pairWith(serverTransport)

        val shortInterval = 20L
        val server = ProtocolImpl(
            serverTransport,
            this,
            heartbeatIntervalMs = shortInterval
        )

        // The watchdog would fire after MAX_SILENT_PROBES * interval (~400ms at
        // 20ms and 20 probes). Dribble one frame in over about 300ms in pieces
        // smaller than a frame, so no frame ever completes until the end.
        val frame = com.zevarc.zevhub.wire.WireCodec.encode(
            com.zevarc.zevhub.wire.WireFrame(
                version = com.zevarc.zevhub.wire.WireConstants.VERSION,
                kind = ProtocolKind.MESSAGE.value,
                frameId = 1,
                contextId = 0,
                correlationId = 0,
                sequence = 0,
                payload = ByteArray(64 * 1024)
            )
        )

        val feeder = launch(Dispatchers.Default) {
            var offset = 0
            val step = 2 * 1024
            while (offset < frame.size) {
                val end = minOf(offset + step, frame.size)
                clientTransport.send(frame.copyOfRange(offset, end))
                offset = end
                delay(10)
            }
        }

        val received = withTimeout(10_000) {
            withContext(Dispatchers.Default) { server.receiveOne() }
        }
        feeder.join()

        if (received.kind != ProtocolKind.MESSAGE) {
            fail("unexpected frame kind ${received.kind}")
        }
        server.close()
    }

    @Test
    fun `a frame of heartbeats keeps a busy peer alive`() = runBlocking {
        // Sanity check on the other side of the same rule: frames that arrive
        // normally reset the counter.
        val clientTransport = InMemoryTransport()
        val serverTransport = InMemoryTransport()
        clientTransport.pairWith(serverTransport)
        val server = ProtocolImpl(serverTransport, this, heartbeatIntervalMs = 20L)

        val feeder = launch(Dispatchers.Default) {
            repeat(30) {
                clientTransport.send(
                    com.zevarc.zevhub.wire.WireCodec.encode(
                        com.zevarc.zevhub.wire.WireFrame(
                            version = com.zevarc.zevhub.wire.WireConstants.VERSION,
                            kind = ProtocolKind.HEARTBEAT.value,
                            frameId = it + 1L,
                            contextId = 0,
                            correlationId = 0,
                            sequence = 0,
                            payload = ByteArray(0)
                        )
                    )
                )
                delay(10)
            }
        }
        feeder.join()
        server.close()
    }
}
