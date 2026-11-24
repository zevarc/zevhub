package com.zevarc.zevhub.discovery

import com.zevarc.zevhub.device.Device
import com.zevarc.zevhub.device.DeviceId
import java.util.concurrent.ConcurrentHashMap

/**
 * Merges the two discovery channels. The UUID (from the DNS-SD TXT record)
 * is the identity key; the P2P MAC address is only a connect handle. A
 * device is emitted exactly once, at the moment both facts are known;
 * either channel may deliver its half first.
 */
internal class DiscoveryRegistry {

    private val byMac = ConcurrentHashMap<String, Entry>()

    // UUID seen through DNS-SD without a source MAC; parked until the MAC
    // half arrives through either channel.
    private val pendingByUuid = ConcurrentHashMap<String, String>()

    private val uuidToMac = ConcurrentHashMap<String, String>()

    fun onService(uuid: String, name: String?, mac: String?): Device? {
        if (mac == null) {
            if (name != null) {
                pendingByUuid.putIfAbsent(uuid, name)
            }
            return null
        }

        val entry = byMac.getOrPut(mac) { Entry() }
        if (name != null) {
            entry.name = name
        }

        if (entry.uuid == null) {
            entry.uuid = uuid
            uuidToMac[uuid] = mac
            // Backfill the name only when nothing better arrived yet.
            if (entry.name == null) {
                entry.name = pendingByUuid[uuid]
            }
            pendingByUuid.remove(uuid)
        }
        return entry.emitIfComplete()
    }

    fun onPeer(mac: String, name: String?): Device? {
        val entry = byMac.getOrPut(mac) { Entry() }
        if (name != null) {
            entry.name = name
        }
        // Without a UUID a peer is not an identity; it is only remembered
        // so a later DNS-SD service report can complete the pairing.
        return entry.emitIfComplete()
    }

    fun macOf(deviceId: String): String? = uuidToMac[deviceId]

    fun clear() {
        byMac.clear()
        pendingByUuid.clear()
        uuidToMac.clear()
    }

    private fun Entry.emitIfComplete(): Device? {
        val uuid = uuid ?: return null
        if (emitted) {
            return null
        }
        emitted = true
        return Device(id = DeviceId(uuid), name = name)
    }

    private class Entry {
        var uuid: String? = null
        var name: String? = null
        var emitted: Boolean = false
    }
}
