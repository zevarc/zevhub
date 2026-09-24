package com.zevarc.zevbond.tcp

import com.zevarc.zevbond.transport.ByteTransport
import com.zevarc.zevbond.transport.TcpTransport
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withContext
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket

/**
 * TCP connection establishment for the P2P group topology: the group owner
 * listens on [port]; a member connects to the owner address with bounded
 * retry while the freshly formed group interface comes up.
 */
class TcpLink(
    private val scope: CoroutineScope
) {

    fun listen(port: Int): Flow<ByteTransport> = flow {
        val server = ServerSocket()
        try {
            server.reuseAddress = true
            server.bind(InetSocketAddress(port))
            android.util.Log.i(TAG, "listening on port $port")
            while (currentCoroutineContext().isActive) {
                // Cancellation must reach the blocked accept(): runInterruptible
                // interrupts the IO thread so the finally below releases the port.
                val socket = runInterruptible { server.accept() }
                socket.tcpNoDelay = true
                emit(TcpTransport(socket, scope))
            }
        } finally {
            runCatching { server.close() }
        }
    }.flowOn(Dispatchers.IO)

    suspend fun connect(host: String, port: Int): ByteTransport = withContext(Dispatchers.IO) {
        var attempt = 0
        while (true) {
            try {
                val socket = Socket()
                socket.reuseAddress = true
                socket.tcpNoDelay = true
                socket.connect(InetSocketAddress(host, port), CONNECT_TIMEOUT_MS)
                return@withContext TcpTransport(socket, scope)
            } catch (e: Exception) {
                if (attempt >= MAX_ATTEMPTS) {
                    throw IllegalStateException("TCP connect to $host:$port failed", e)
                }
                attempt++
            }
            delay(backoffDelay(attempt))
        }
        // The loop above never exits normally; Nothing satisfies the return type.
        @Suppress("UNREACHABLE_CODE")
        error("unreachable")
    }

    private fun backoffDelay(attempt: Int): Long {
        val shifted = BASE_DELAY_MS shl (attempt - 1).coerceIn(0, 4)
        return shifted.coerceAtMost(MAX_DELAY_MS)
    }

    companion object {
        private const val TAG = "ZevBond/Tcp"

        const val DEFAULT_PORT = 9527
        const val CONNECT_TIMEOUT_MS = 5_000
        const val MAX_ATTEMPTS = 10
        const val BASE_DELAY_MS = 500L
        const val MAX_DELAY_MS = 10_000L
    }
}
