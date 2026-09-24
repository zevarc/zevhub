package com.zevarc.zevbond.transfer

/**
 * Provides the data of an outgoing transfer in chunks. Called
 * sequentially by the Core; [read] suspends while the peer is behind,
 * so a slow [java.io.InputStream] or generator works without the whole
 * payload being held in memory.
 */
interface TransferSource {

    val metadata: TransferMetadata

    /**
     * Total bytes to be delivered, or null when unknown up front.
     * Used only for progress reporting; it is not validated against the
     * bytes actually read.
     */
    val totalBytes: Long?

    /**
     * Returns the next chunk of at most [maxBytes] bytes, or null when
     * the data is exhausted. Returned arrays are handed to the wire and
     * must not be modified afterwards.
     */
    suspend fun read(maxBytes: Int): ByteArray?
}
