package com.zevarc.zevbond.session

import com.zevarc.zevbond.device.Device
import com.zevarc.zevbond.message.Message
import kotlinx.coroutines.flow.Flow

interface Session {

    val device: Device

    suspend fun send(message: Message)

    fun messages(): Flow<Message>

    suspend fun close()
}
