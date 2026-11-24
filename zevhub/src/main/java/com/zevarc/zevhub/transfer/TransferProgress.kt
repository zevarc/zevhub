package com.zevarc.zevhub.transfer

/**
 * Snapshots of transfer progress. [totalBytes] is null when the source
 * declared no total size.
 */
data class TransferProgress(
    val bytesTransferred: Long,
    val totalBytes: Long?
)
