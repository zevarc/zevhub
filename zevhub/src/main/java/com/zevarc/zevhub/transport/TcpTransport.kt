package com.zevarc.zevhub.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.InputStream
import java.io.OutputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * [ByteTransport] over a blocking TCP socket. Read and write loops run as
 * coroutines on Dispatchers.IO. send() serializes through a channel so one
 * write never interleaves with another. receive() must have a single
 * collector; the flow completes on EOF and fails on transport error.
 */
internal class TcpTransport(
    private val socket: Socket,
    scope: CoroutineScope
) : ByteTransport {

    private val closed = AtomicBoolean(false)

    /**
     * Outbound queue.
     *
     * Kept shallow on purpose. Each element is a whole encoded frame, so a deep
     * queue holds megabytes of live arrays and hands the collector steady work for
     * no throughput gain: the socket is the slow end, and backpressure reaching the
     * producer is what keeps memory flat. Two is enough to keep the writer busy
     * across a scheduling hop.
     */
    private val writeChannel = Channel<ByteArray>(capacity = 2)

    private val incoming = Channel<ByteArray>(capacity = 64)

    private val readJob: Job
    private val writeJob: Job

    private val failure = AtomicReference<Throwable?>(null)

    init {
        readJob = scope.launch(Dispatchers.IO) { readLoop(socket.getInputStream()) }
        writeJob = scope.launch(Dispatchers.IO) { writeLoop(socket.getOutputStream()) }
    }

    override suspend fun send(data: ByteArray) {
        failure.get()?.let { throw it }
        writeChannel.send(data)
    }

    override fun receive(): Flow<ByteArray> = incoming.receiveAsFlow()

    override suspend fun close() {
        if (!closed.compareAndSet(false, true)) {
            return
        }
        writeChannel.close()
        incoming.cancel()
        withContext(Dispatchers.IO) {
            runCatching { socket.close() }
        }
        readJob.join()
        writeJob.join()
    }

    private suspend fun readLoop(input: InputStream) {
        val chunk = ByteArray(READ_CHUNK)
        try {
            while (true) {
                val n = input.read(chunk)
                if (n < 0) {
                    break
                }
                incoming.send(chunk.copyOf(n))
            }
            incoming.close()
        } catch (e: Exception) {
            fail(e)
        }
    }

    private suspend fun writeLoop(output: OutputStream) {
        try {
            for (data in writeChannel) {
                // A peer that closed gracefully keeps accepting into its socket
                // buffer, so a blocking write only notices the teardown when
                // cancellation is checked explicitly at a frame boundary.
                currentCoroutineContext().ensureActive()
                output.write(data)
                // Flush only when nothing else is queued. Flushing per frame
                // forces one syscall per frame and defeats the socket's own
                // batching, which measured as the dominant cost of a transfer.
                if (writeChannel.isEmpty) {
                    output.flush()
                }
            }
            output.flush()
        } catch (e: Exception) {
            if (!closed.get()) {
                // A live write failure kills the whole path: fail the read
                // side so the upper layers observe the transport going down.
                socket.close()
                fail(e)
            }
        }
    }

    private fun fail(e: Throwable) {
        if (closed.get()) {
            incoming.cancel()
        } else {
            failure.compareAndSet(null, e)
            incoming.close(e)
        }
    }

    companion object {
        private const val READ_CHUNK = 16 * 1024
    }
}
