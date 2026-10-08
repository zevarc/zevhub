package com.zevarc.zevbond.stream

/**
 * Thrown by [com.zevarc.zevbond.session.Session.openStream] when the
 * peer refuses the stream offer or the negotiation fails.
 */
class StreamRejectedException(message: String) : Exception(message)
