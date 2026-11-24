package com.zevarc.zevhub.transfer

/**
 * Provides the data of an outgoing transfer in chunks. Called
 * sequentially by the Core; [read] suspends while the peer is behind,
 * so a slow [java.io.InputStream] or generator works without the whole
 * payload being held in memory.
 */
interface TransferSource {

    val metadata: TransferMetadata

    /**
     * Total bytes to be delivered, or null when unknown up front. The receiver
     * verifies the delivered count against this when it is non-null, so a
     * source that lies about it fails the transfer instead of silently leaving
     * a truncated payload behind.
     */
    val totalBytes: Long?

    /**
     * Returns the next chunk of at most [maxBytes] bytes, or null when
     * the data is exhausted. Returned arrays are handed to the wire and
     * must not be modified afterwards.
     *
     * Must be cancellable. The Core cancels this call when the peer refuses or
     * cancels the transfer, so a read that blocks without honouring
     * cancellation keeps a file handle open until the process dies.
     */
    suspend fun read(maxBytes: Int): ByteArray?

    /**
     * Released exactly once when the transfer reaches a terminal state, so
     * implementations can put `InputStream.close()` and friends here. Must be
     * idempotent.
     *
     * [result] is what actually happened, which a source cannot infer from
     * having been closed: reporting a cancelled transfer as a success is how
     * half-transferred files end up recorded as delivered. A transport or
     * protocol failure is not reported here because it throws from
     * [com.zevarc.zevhub.transfer.Transfer.await] instead.
     */
    suspend fun close(result: TransferResult) {}
}
