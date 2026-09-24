package com.zevarc.zevbond

import com.zevarc.zevbond.device.Device
import com.zevarc.zevbond.session.Session
import kotlinx.coroutines.flow.Flow

interface ZevBond : AutoCloseable {

    val device: Device

    suspend fun start()

    /**
     * Streams devices found through Wi-Fi P2P service discovery.
     *
     * Discovery only surfaces devices reachable over the air. A peer that
     * has already joined this device's P2P group does not answer discovery
     * probes, so an established peer will not appear while the group is
     * up. Use the session obtained from [connect] or [incomingSessions]
     * instead of re-discovering a connected peer.
     */
    fun discover(): Flow<Device>

    suspend fun connect(
        device: Device
    ): Session

    fun incomingSessions(): Flow<Session>

    override fun close()
}