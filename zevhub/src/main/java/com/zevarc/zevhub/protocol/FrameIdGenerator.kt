package com.zevarc.zevhub.protocol

import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.fetchAndIncrement

internal class FrameIdGenerator {

    @OptIn(ExperimentalAtomicApi::class)
    private val next = AtomicLong(1)

    @OptIn(ExperimentalAtomicApi::class)
    fun next(): Long {
        return next.fetchAndIncrement()
    }
}