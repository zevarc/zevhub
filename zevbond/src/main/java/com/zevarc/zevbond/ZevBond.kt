package com.zevarc.zevbond

import com.zevarc.zevbond.device.Device
import com.zevarc.zevbond.session.Session
import kotlinx.coroutines.flow.Flow

interface ZevBond : AutoCloseable {

    val device: Device

    suspend fun start()

    fun discover(): Flow<Device>

    suspend fun connect(
        device: Device
    ): Session

    fun incomingSessions(): Flow<Session>

    override fun close()
}