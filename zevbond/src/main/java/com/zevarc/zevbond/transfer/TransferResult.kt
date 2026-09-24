package com.zevarc.zevbond.transfer

/**
 * Terminal state of a completed [Transfer]. Transport or protocol death
 * is not a result; it throws from [Transfer.await] instead.
 */
sealed interface TransferResult {

    /** The peer accepted the transfer and all bytes were delivered. */
    data class Success(val bytesDelivered: Long) : TransferResult

    /** The peer rejected the transfer, or either side cancelled it. */
    data object Cancelled : TransferResult
}
