package com.zevarc.zevbond.transfer

import kotlinx.coroutines.flow.Flow

/**
 * A lifecycle-bound, large-data reliable delivery operation.
 *
 * Obtain through [com.zevarc.zevbond.session.Session.transfer]; data
 * starts flowing as the peer accepts, with backpressure: reading from
 * the source suspends while the peer is behind. A transport or protocol
 * failure surfaces from [await]; a peer rejection or a cancellation on
 * either side completes it as [TransferResult.Cancelled].
 */
interface Transfer {

    val progress: Flow<TransferProgress>

    /** Completes when the transfer reaches a terminal state. */
    suspend fun await(): TransferResult

    /**
     * Cancels the transfer on both sides. The peer's transfer ends as
     * [TransferResult.Cancelled]; safe to call more than once.
     */
    fun cancel()
}
