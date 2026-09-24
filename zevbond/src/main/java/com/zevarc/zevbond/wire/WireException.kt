package com.zevarc.zevbond.wire

/**
 * Fatal wire-level decoding failure. The connection must be closed.
 */
class WireException(message: String) : Exception(message)
