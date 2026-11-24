package com.zevarc.zevhub.stream

/**
 * Names one logical stream channel, such as "camera" or "audio". The
 * Core gives it meaning only as a label; the peers agree on its use.
 */
@JvmInline
value class StreamType(
    val value: String
)
