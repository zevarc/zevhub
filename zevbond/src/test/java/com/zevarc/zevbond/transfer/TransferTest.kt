package com.zevarc.zevbond.transfer

import com.zevarc.zevbond.device.Device
import com.zevarc.zevbond.device.DeviceId
import com.zevarc.zevbond.protocol.InMemoryTransport
import com.zevarc.zevbond.protocol.ProtocolImpl
import com.zevarc.zevbond.session.SessionImpl
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@kotlinx.coroutines.ExperimentalCoroutinesApi
class TransferTest {
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

    /** A source delivering [data] in fixed-size chunks. */
    private class ByteSource(
        override val metadata: TransferMetadata,
        override val totalBytes: Long?,
        private val data: ByteArray,
        private val chunkSize: Int
    ) : TransferSource {
        var offset = 0
        override suspend fun read(maxBytes: Int): ByteArray? {
            if (offset >= data.size) return null
            val end = minOf(offset + chunkSize, data.size)
            val chunk = data.copyOfRange(offset, end)
            offset = end
            return chunk
        }
    }

    /** A destination collecting every chunk in memory. */
    private class ListDestination : TransferDestination {
        val chunks = mutableListOf<ByteArray>()
        var closed = false
        override suspend fun write(data: ByteArray) {
            chunks.add(data)
        }
        override suspend fun close() {
            closed = true
        }
        fun allBytes(): ByteArray = chunks.reduce { a, b -> a + b }
    }

    @Test
    fun `small transfer round trips with correct result`() = runTest {
        val (_, _, protocols) = pairWithTransports(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        repeat(5) { yield() }

        val data = "hello transfer".toByteArray()
        val transfer = clientSession.transfer(
            ByteSource(TransferMetadata("note", byteArrayOf(7)), data.size.toLong(), data, 4)
        )
        val destination = ListDestination()
        val incoming = serverSession.transfers().first()
        val received = incoming.accept(destination)

        assertEquals(TransferResult.Success(data.size.toLong()), transfer.await())
        assertEquals(TransferResult.Success(data.size.toLong()), received.await())
        assertArrayEquals(data, destination.allBytes())
        assertTrue(destination.closed)
    }

    @Test
    fun `large transfer survives chunking across many data frames`() = runTest {
        val (_, _, protocols) = pairWithTransports(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        repeat(5) { yield() }

        val data = ByteArray(3 * 1024 * 1024) { (it % 251).toByte() }
        val transfer = clientSession.transfer(
            ByteSource(TransferMetadata("blob"), data.size.toLong(), data, 256 * 1024)
        )
        val destination = ListDestination()
        val received = serverSession.transfers().first()
        val accepted = received.accept(destination)

        assertEquals(TransferResult.Success(data.size.toLong()), transfer.await())
        assertEquals(TransferResult.Success(data.size.toLong()), accepted.await())
        assertArrayEquals(data, destination.allBytes())
    }

    @Test
    fun `progress reports monotonic bytes and total`() = runTest {
        val (_, _, protocols) = pairWithTransports(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        repeat(5) { yield() }

        val data = ByteArray(1000) { it.toByte() }
        val transfer = clientSession.transfer(
            ByteSource(TransferMetadata("p"), data.size.toLong(), data, 300)
        )
        val destination = ListDestination()
        serverSession.transfers().first().accept(destination)

        // Subscribe first so the live snapshots are observed; progress
        // only ever advances, ending at the full size.
        var last = TransferProgress(0, null)
        val progressJob = launch {
            transfer.progress.collect {
                assertTrue(it.bytesTransferred >= last.bytesTransferred)
                last = it
            }
        }
        repeat(5) { yield() }
        val result = transfer.await()
        assertEquals(TransferResult.Success(1000L), result)
        repeat(5) { yield() }
        progressJob.cancel()

        assertEquals(1000L, last.bytesTransferred)
        assertEquals(1000L, last.totalBytes)
    }

    @Test
    fun `reject on the receiver cancels the sender`() = runTest {
        val (_, _, protocols) = pairWithTransports(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        repeat(5) { yield() }

        // A source that waits for the reject to land before producing
        // one more chunk, so the transfer is genuinely in flight.
        val rejected = CompletableDeferred<Unit>()
        val source = object : TransferSource {
            override val metadata = TransferMetadata("denied")
            override val totalBytes = null
            private var calls = 0
            override suspend fun read(maxBytes: Int): ByteArray? {
                calls++
                if (calls == 1) {
                    return ByteArray(1024)
                }
                rejected.await()
                return null
            }
        }
        val transfer = clientSession.transfer(source)
        val incoming = serverSession.transfers().first()
        incoming.reject()
        // Let the CANCEL frame cross the wire before the source resumes.
        repeat(5) { yield() }
        rejected.complete(Unit)

        assertEquals(TransferResult.Cancelled, transfer.await())
    }

    @Test
    fun `local cancel stops the transfer and notifies the peer`() = runTest {
        val (_, _, protocols) = pairWithTransports(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        repeat(5) { yield() }

        // A source that blocks forever after the first chunk, so the
        // transfer only ends through cancellation.
        val stalled = CompletableDeferred<Unit>()
        val source = object : TransferSource {
            override val metadata = TransferMetadata("stall")
            override val totalBytes = null
            private var first = true
            override suspend fun read(maxBytes: Int): ByteArray? {
                if (first) {
                    first = false
                    return ByteArray(1024)
                }
                stalled.await()
                return null
            }
        }
        val transfer = clientSession.transfer(source)
        val destination = ListDestination()
        val received = serverSession.transfers().first().accept(destination)

        transfer.cancel()
        assertEquals(TransferResult.Cancelled, transfer.await())
        assertEquals(TransferResult.Cancelled, received.await())
    }

    @Test
    fun `session death fails an in-flight transfer`() = runTest {
        val (clientTransport, _, protocols) = pairWithTransports(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        repeat(5) { yield() }

        // A source that stalls after the first chunk so the death lands
        // mid-transfer.
        val transportDown = CompletableDeferred<Unit>()
        val source = object : TransferSource {
            override val metadata = TransferMetadata("doomed")
            override val totalBytes = null
            private var first = true
            override suspend fun read(maxBytes: Int): ByteArray? {
                if (first) {
                    first = false
                    return ByteArray(1024)
                }
                transportDown.await()
                return null
            }
        }
        val transfer = clientSession.transfer(source)
        val destination = ListDestination()
        val accepted = serverSession.transfers().first().accept(destination)

        clientTransport.failFromRemote(IllegalStateException("link down"))

        val sendOutcome = runCatching { transfer.await() }
        val recvOutcome = runCatching { accepted.await() }
        assertTrue(sendOutcome.isFailure)
        assertTrue(recvOutcome.isFailure)
        transportDown.complete(Unit)
    }

    @Test
    fun `concurrent transfers do not interfere`() = runTest {
        val (_, _, protocols) = pairWithTransports(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        repeat(5) { yield() }

        val first = ByteArray(100_000) { 1 }
        val second = ByteArray(100_000) { 2 }
        val t1 = clientSession.transfer(
            ByteSource(TransferMetadata("one", byteArrayOf(1)), first.size.toLong(), first, 997)
        )
        val t2 = clientSession.transfer(
            ByteSource(TransferMetadata("two", byteArrayOf(2)), second.size.toLong(), second, 333)
        )

        // Each offer is delivered exactly once; accept both and pair
        // them by the metadata marker so the routing check is exact.
        // Awaiting happens outside the collector, otherwise the
        // suspended collect cannot see the second offer.
        val destinationFor = mutableMapOf<Int, ListDestination>()
        val acceptedJobs = mutableListOf<Transfer>()
        val collectorDone = CompletableDeferred<Unit>()
        val collectJob = launch {
            withTimeout(5_000) {
                serverSession.transfers().collect { offer ->
                    val tag = offer.metadata.data[0].toInt()
                    val destination = ListDestination()
                    destinationFor[tag] = destination
                    acceptedJobs.add(offer.accept(destination))
                    if (acceptedJobs.size == 2) {
                        collectorDone.complete(Unit)
                    }
                }
            }
        }
        collectorDone.await()
        val results = acceptedJobs.map { it.await() }
        assertEquals(2, results.count { it is TransferResult.Success })
        assertEquals(
            setOf(first.size.toLong(), second.size.toLong()),
            results.map { (it as TransferResult.Success).bytesDelivered }.toSet()
        )
        assertArrayEquals(first, destinationFor.getValue(1).allBytes())
        assertArrayEquals(second, destinationFor.getValue(2).allBytes())
        collectJob.cancel()
    }

    @Test
    fun `transfer leaves message and request paths usable`() = runTest {
        val (_, _, protocols) = pairWithTransports(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        repeat(5) { yield() }

        val transfer = clientSession.transfer(
            ByteSource(TransferMetadata("warm"), null, ByteArray(10_000), 1000)
        )
        val destination = ListDestination()
        serverSession.transfers().first().accept(destination)
        assertEquals(TransferResult.Success(10_000L), transfer.await())

        // The session still moves messages and requests afterwards.
        val responder = launch {
            serverSession.requests().collect { incoming ->
                incoming.respond(
                    com.zevarc.zevbond.request.Response.Success(incoming.data)
                )
            }
        }
        val received = async {
            serverSession.messages().first()
        }
        repeat(5) { yield() }
        clientSession.send(com.zevarc.zevbond.message.Message(
            com.zevarc.zevbond.message.MessageType("chat"), byteArrayOf(1)
        ))
        val response = clientSession.request(
            com.zevarc.zevbond.request.Request(
                com.zevarc.zevbond.request.RequestType("ping"), "pong".toByteArray()
            ),
            timeoutMs = 1_000
        )
        assertEquals("pong", String((response as com.zevarc.zevbond.request.Response.Success).data))
        responder.cancel()
        received.cancel()
    }

    @Test
    fun `each offer is delivered exactly once`() = runTest {
        val (_, _, protocols) = pairWithTransports(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        repeat(5) { yield() }

        // A collector that accepts every offer; the sender fires several
        // transfers back to back so the offers queue up while the
        // collector is busy in accept().
        val offersSeen = mutableListOf<IncomingTransfer>()
        val acceptedJobs = mutableListOf<Transfer>()
        val collectJob = launch {
            withTimeout(5_000) {
                serverSession.transfers().collect { offer ->
                    offersSeen.add(offer)
                    acceptedJobs.add(offer.accept(ListDestination()))
                }
            }
        }
        repeat(3) { index ->
            clientSession.transfer(
                ByteSource(
                    TransferMetadata("burst", byteArrayOf(index.toByte())),
                    null,
                    ByteArray(1000),
                    500
                )
            )
        }

        // Let the collector drain the queued offers before asserting.
        repeat(5) { yield() }
        val results = acceptedJobs.map { it.await() }
        assertEquals(3, offersSeen.size)
        assertEquals(3, results.count { it is TransferResult.Success })
        collectJob.cancel()
    }

    @Test
    fun `ignored offer times out and cancels the sender`() = runTest {
        val (_, _, protocols) = pairWithTransports(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        repeat(5) { yield() }

        // A source that blocks after the first chunk, so the transfer is
        // still in flight when the receiver's offer timeout fires.
        val stalled = CompletableDeferred<Unit>()
        val source = object : TransferSource {
            override val metadata = TransferMetadata("forgotten")
            override val totalBytes = null
            private var first = true
            override suspend fun read(maxBytes: Int): ByteArray? {
                if (first) {
                    first = false
                    return ByteArray(1024)
                }
                stalled.await()
                return null
            }
        }
        val transfer = clientSession.transfer(source)

        // Nobody collects the receiver's offers; the timeout must reject
        // the offer so the sender is not left streaming into a void.
        advanceTimeBy(30_001)
        repeat(5) { yield() }

        assertEquals(TransferResult.Cancelled, transfer.await())
        stalled.complete(Unit)
    }

    @Test
    fun `accepted offer keeps working after the timeout window passes`() = runTest {
        val (_, _, protocols) = pairWithTransports(backgroundScope)
        val (clientProtocol, serverProtocol) = protocols
        val clientSession = sessionFor(clientProtocol, backgroundScope)
        val serverSession = sessionFor(serverProtocol, backgroundScope)

        repeat(5) { yield() }

        val data = ByteArray(10_000) { (it % 251).toByte() }
        // A source that waits for acceptance before producing, so the
        // timeout window fully elapses while the offer sits undecided.
        val accepted = CompletableDeferred<Unit>()
        val source = object : TransferSource {
            override val metadata = TransferMetadata("slowstart")
            override val totalBytes = data.size.toLong()
            private var first = true
            override suspend fun read(maxBytes: Int): ByteArray? {
                if (first) {
                    first = false
                    accepted.await()
                    return data
                }
                return null
            }
        }
        val transfer = clientSession.transfer(source)
        val destination = ListDestination()
        val acceptJob = async {
            val offer = serverSession.transfers().first()
            val received = offer.accept(destination)
            accepted.complete(Unit)
            received.await()
        }

        advanceTimeBy(30_001)
        repeat(5) { yield() }
        assertEquals(TransferResult.Success(data.size.toLong()), transfer.await())
        assertEquals(TransferResult.Success(data.size.toLong()), acceptJob.await())
        assertArrayEquals(data, destination.allBytes())
    }
}
