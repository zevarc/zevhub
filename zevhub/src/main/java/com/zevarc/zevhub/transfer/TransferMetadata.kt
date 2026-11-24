package com.zevarc.zevhub.transfer

/**
 * Service-defined description of the data being transferred. The Core
 * gives it meaning only as an opaque byte array; a concrete service may
 * pack filename, MIME type, or anything else into [data].
 */
data class TransferMetadata(
    val type: String,
    val data: ByteArray = ByteArray(0)
)
