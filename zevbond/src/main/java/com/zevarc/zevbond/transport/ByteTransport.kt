package com.zevarc.zevbond.transport

import kotlinx.coroutines.flow.Flow

interface ByteTransport {

    suspend fun send(data: ByteArray)

    fun receive(): Flow<ByteArray>

    suspend fun close()
}