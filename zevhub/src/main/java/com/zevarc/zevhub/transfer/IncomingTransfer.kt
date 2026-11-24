package com.zevarc.zevhub.transfer

/**
 * A transfer offered by the peer. Decide through [accept] or [reject];
 * the incoming data starts being written only after [accept] returns a
 * [Transfer], and the offer expires with the session.
 */
interface IncomingTransfer {

    val metadata: TransferMetadata

    val totalBytes: Long?

    /** Begins receiving the data into [destination]. */
    fun accept(destination: TransferDestination): Transfer

    /** Refuses the transfer; the sender's [Transfer] ends as cancelled. */
    fun reject()
}
