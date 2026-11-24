package com.zevarc.zevhub.stream

/**
 * Thrown by [com.zevarc.zevhub.session.Session.openStream] when the
 * peer refuses the stream offer or the negotiation fails.
 */
class StreamRejectedException(message: String) : Exception(message)
