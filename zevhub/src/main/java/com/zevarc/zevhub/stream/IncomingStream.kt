package com.zevarc.zevhub.stream

/**
 * A stream the peer wants to open, delivered through
 * [com.zevarc.zevhub.session.Session.streams]. The receiver must
 * decide with [accept] or [reject]; ignoring the offer eventually
 * times it out and refuses it on the peer side.
 */
interface IncomingStream {

    val type: StreamType

    /**
     * Accepts the stream and returns the receiving side. One-shot: a
     * second call throws [IllegalStateException].
     */
    fun accept(): Stream

    /**
     * Refuses the stream. One-shot: a second call throws
     * [IllegalStateException].
     */
    suspend fun reject()
}
