package com.zevarc.zevhub

import com.zevarc.zevhub.device.Device
import com.zevarc.zevhub.device.DeviceId
import com.zevarc.zevhub.handshake.Handshake
import com.zevarc.zevhub.protocol.FrameIdGenerator
import com.zevarc.zevhub.protocol.ProtocolImpl
import com.zevarc.zevhub.session.Session
import com.zevarc.zevhub.session.SessionImpl
import com.zevarc.zevhub.tcp.TcpLink
import com.zevarc.zevhub.transfer.TransferDestination
import com.zevarc.zevhub.transfer.TransferMetadata
import com.zevarc.zevhub.transfer.TransferResult
import com.zevarc.zevhub.transfer.TransferSource
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
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * The real transport path — TCP sockets, framing, session pump, transfer — with
 * nothing faked except the radio.
 *
 * This exists because the Wi-Fi P2P link cannot be exercised off-device, so the
 * layer above it kept shipping untested and broke in ways only a device showed.
 * A loopback socket carries the same bytes through the same code.
 */
class TcpTransferTest {

    private val payloadSize = 8 * 1024 * 1024
    private val port = 19528

    private fun pattern(size: Int) = ByteArray(size) { (it % 251).toByte() }

    /** A source that hands out [data] in [chunk] sized reads. */
    private class BytesSource(
        override val metadata: TransferMetadata,
        override val totalBytes: Long?,
        private val data: ByteArray,
        private val chunk: Int
    ) : TransferSource {
        private var offset = 0
        var closed: TransferResult? = null

        override suspend fun read(maxBytes: Int): ByteArray? {
            if (offset >= data.size) return null
            val end = minOf(offset + minOf(chunk, maxBytes), data.size)
            val out = data.copyOfRange(offset, end)
            offset = end
            return out
        }

        override suspend fun close(result: TransferResult) {
            closed = result
        }
    }

    private class CollectingDestination : TransferDestination {
        private val chunks = mutableListOf<ByteArray>()
        var closed = false

        override suspend fun write(data: ByteArray) {
            chunks.add(data.copyOf())
        }

        override suspend fun close() {
            closed = true
        }

        fun bytes(): ByteArray {
            val total = chunks.sumOf { it.size }
            val out = ByteArray(total)
            var at = 0
            chunks.forEach { it.copyInto(out, at); at += it.size }
            return out
        }
    }

    private fun <T> withSessions(
        block: suspend (scope: CoroutineScope, client: Session, server: Session) -> T
    ): T = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val tcp = TcpLink(scope)
        try {
            val serverSide = scope.async {
                val transport = tcp.listen(port).first()
                val protocol = ProtocolImpl(transport, scope)
                val peer = Handshake(DeviceId("SERVER"), FrameIdGenerator())
                    .run(protocol, initiator = false)
                SessionImpl(Device(peer, null), protocol, scope)
            }

            val clientTransport = withTimeout(10_000) { tcp.connect("127.0.0.1", port) }
            val clientProtocol = ProtocolImpl(clientTransport, scope)
            val peer = Handshake(DeviceId("CLIENT"), FrameIdGenerator())
                .run(clientProtocol, initiator = true)
            val client = SessionImpl(Device(peer, null), clientProtocol, scope)

            val server = withTimeout(10_000) { serverSide.await() }
            withTimeout(120_000) { block(this, client, server) }
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun `an eight megabyte transfer arrives intact over a real socket`() =
        withSessions { scope, client, server ->
            val data = pattern(payloadSize)
            val destination = CollectingDestination()

            val receiver = scope.async {
                val offer = server.transfers().first()
                assertEquals(payloadSize.toLong(), offer.totalBytes)
                offer.accept(destination)
            }

            val sender = client.transfer(
                BytesSource(TransferMetadata("big", "test".toByteArray()), payloadSize.toLong(), data, 256 * 1024)
            )
            val received = receiver.await()

            assertEquals(TransferResult.Success(payloadSize.toLong()), sender.await())
            assertEquals(TransferResult.Success(payloadSize.toLong()), received.await())
            assertTrue(destination.closed)
            assertArrayEquals(data, destination.bytes())
            Unit
        }

    @Test
    fun `a second transfer on the same session also completes`() =
        withSessions { scope, client, server ->
            val data = pattern(1024 * 1024)

            repeat(2) { round ->
                val destination = CollectingDestination()
                val receiver = scope.async {
                    val offer = server.transfers().first()
                    offer.accept(destination)
                }
                val sender = client.transfer(
                    BytesSource(
                        TransferMetadata("round$round", byteArrayOf(round.toByte())),
                        data.size.toLong(),
                        data,
                        256 * 1024
                    )
                )
                val received = receiver.await()

                assertEquals(
                    "round $round sender",
                    TransferResult.Success(data.size.toLong()),
                    sender.await()
                )
                assertEquals(
                    "round $round receiver",
                    TransferResult.Success(data.size.toLong()),
                    received.await()
                )
                assertArrayEquals("round $round payload", data, destination.bytes())
            }
            Unit
        }

    @Test
    fun `transfer sustains a usable throughput`() =
        withSessions { scope, client, server ->
            val data = pattern(payloadSize)
            val destination = CollectingDestination()
            val receiver = scope.async {
                server.transfers().first().accept(destination)
            }

            val started = System.nanoTime()
            val sender = client.transfer(
                BytesSource(TransferMetadata("speed"), payloadSize.toLong(), data, 256 * 1024)
            )
            val received = receiver.await()
            sender.await()
            received.await()
            val elapsedMs = (System.nanoTime() - started) / 1_000_000

            val mbPerSec = payloadSize.toDouble() / 1024 / 1024 / (elapsedMs / 1000.0)
            println("BENCH tcp transfer ${payloadSize / 1024 / 1024} MiB in ${elapsedMs}ms = ${"%.1f".format(mbPerSec)} MB/s")

            // Loopback is far faster than any radio; this only catches a
            // pathological regression such as a per-frame syscall on Main.
            assertTrue(
                "loopback throughput collapsed: ${"%.1f".format(mbPerSec)} MB/s",
                mbPerSec > 20.0
            )
            Unit
        }
}
