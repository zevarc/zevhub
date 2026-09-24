package com.zevarc.zevbond.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.InputStream
import java.io.OutputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

/**
 * [ByteTransport] over a blocking TCP socket. Read and write loops run as
 * coroutines on Dispatchers.IO. send() serializes through a channel so one
 * write never interleaves with another. receive() must have a single
 * collector; the flow completes on EOF and fails on transport error.
 */
class TcpTransport(
    private val socket: Socket,
    scope: CoroutineScope
) : ByteTransport {

    private val closed = AtomicBoolean(false)

    private val writeChannel = Channel<ByteArray>(capacity = 16)

    private val incoming = Channel<ByteArray>(capacity = 64)

    private val readJob: Job
    private val writeJob: Job

    init {
        readJob = scope.launch(Dispatchers.IO) { readLoop(socket.getInputStream()) }
        writeJob = scope.launch(Dispatchers.IO) { writeLoop(socket.getOutputStream()) }
    }

    override suspend fun send(data: ByteArray) {
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
            if (closed.get()) {
                incoming.cancel()
            } else {
                incoming.close(e)
            }
        }
    }

    private suspend fun writeLoop(output: OutputStream) {
        try {
            for (data in writeChannel) {
                output.write(data)
                output.flush()
            }
        } catch (e: Exception) {
            // Local close breaks the blocking write; the read loop surfaces
            // the failure through the incoming flow.
        }
    }

    companion object {
        private const val READ_CHUNK = 16 * 1024
    }
}
