package com.zevarc.zevhub.transfer

/**
 * Receives the data of an accepted transfer. [write] is called
 * sequentially with chunks in wire order; [close] is called exactly
 * once after the last chunk, whether the transfer completes, is
 * cancelled, or fails — the implementation must be idempotent.
 */
interface TransferDestination {

    suspend fun write(data: ByteArray)

    suspend fun close()
}
