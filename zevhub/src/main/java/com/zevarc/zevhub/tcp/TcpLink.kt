package com.zevarc.zevhub.tcp

import com.zevarc.zevhub.transport.ByteTransport
import com.zevarc.zevhub.transport.TcpTransport
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
internal class TcpLink(
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
                tune(socket)
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
                tune(socket)
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

    /**
     * Applies the socket options this link needs.
     *
     * Wi-Fi Direct has a high round-trip time — tens of milliseconds — so the
     * bandwidth-delay product is measured in megabytes. The stock buffers are
     * sized for the internet, not for a single hop between two phones, and a
     * transfer ends up waiting on the window instead of the radio. Raising both
     * directions lets the sender keep the pipe full.
     *
     * tcpNoDelay stays on: this link carries framed messages where a small
     * request must not wait behind a batch, and the bulk path is large enough
     * that the per-packet cost is not the issue.
     */
    private fun tune(socket: Socket) {
        socket.tcpNoDelay = true
        runCatching {
            socket.sendBufferSize = SOCKET_BUFFER_BYTES
            socket.receiveBufferSize = SOCKET_BUFFER_BYTES
        }.onFailure {
            // A kernel that refuses the hint still works, just slower.
            android.util.Log.w(TAG, "socket buffer tuning rejected", it)
        }
    }

    private fun backoffDelay(attempt: Int): Long {
        val shifted = BASE_DELAY_MS shl (attempt - 1).coerceIn(0, 4)
        return shifted.coerceAtMost(MAX_DELAY_MS)
    }

    companion object {
        private const val TAG = "ZevHub/Tcp"

        const val DEFAULT_PORT = 9527

        /**
         * Requested SO_SNDBUF / SO_RCVBUF, in bytes.
         *
         * Four megabytes is roughly a Wi-Fi Direct bandwidth-delay product at the
         * rates these radios reach. The kernel may clamp this to its own maximum,
         * which is fine — the point is to stop being limited by a default sized
         * for wide-area links.
         */
        const val SOCKET_BUFFER_BYTES = 4 * 1024 * 1024
        const val CONNECT_TIMEOUT_MS = 5_000
        const val MAX_ATTEMPTS = 10
        const val BASE_DELAY_MS = 500L
        const val MAX_DELAY_MS = 10_000L
    }
}
