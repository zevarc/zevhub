package com.zevarc.zevbond.discovery

import android.annotation.SuppressLint
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.net.wifi.p2p.WifiP2pConfig
import android.net.wifi.p2p.WifiP2pInfo
import android.net.wifi.p2p.WifiP2pManager
import android.net.wifi.p2p.nsd.WifiP2pDnsSdServiceInfo
import android.net.wifi.p2p.nsd.WifiP2pDnsSdServiceRequest
import android.os.Build
import android.os.Parcelable
import com.zevarc.zevbond.device.Device
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

/**
 * Wi-Fi P2P link establishment over the platform WifiP2pManager.
 *
 * Identity is a UUID advertised through DNS-SD service discovery; the P2P
 * MAC address carried in the same TXT record is only a connect handle.
 * Group formation then provides the topology: the group owner listens on
 * TCP, a member connects to the owner address.
 */
internal class WifiP2pLink(
    private val context: Context
) {

    private val appContext = context.applicationContext

    private val manager = appContext.getSystemService(Context.WIFI_P2P_SERVICE) as WifiP2pManager

    private var channel: WifiP2pManager.Channel? = null

    private var localUuid: String? = null

    private var localName: String? = null

    private fun ensureChannel(): WifiP2pManager.Channel {
        channel?.let { return it }
        return manager.initialize(appContext, appContext.mainLooper) {
            // Channel lost; a later call re-initializes.
            channel = null
        }.also { channel = it }
    }

    /**
     * Advertise the local identity as a P2P DNS-SD service. The TXT record
     * carries the UUID and the device name; both stay within the 255-byte
     * ASCII constraint of a DNS-SD TXT entry.
     */
    @SuppressLint("MissingPermission")
    fun announceIdentity(uuid: String, deviceName: String?, onResult: (Throwable?) -> Unit) {
        val c = ensureChannel()
        localUuid = uuid
        localName = deviceName

        val service = buildServiceInfo(uuid, deviceName)

        manager.addLocalService(c, service, object : WifiP2pManager.ActionListener {
            override fun onSuccess() {
                onResult(null)
            }

            override fun onFailure(reason: Int) {
                onResult(IllegalStateException("announceIdentity failed: $reason"))
            }
        })
    }

    private fun buildServiceInfo(uuid: String, deviceName: String?): WifiP2pDnsSdServiceInfo {
        val txt = mutableMapOf(
            KEY_ID to uuid
        )
        if (!deviceName.isNullOrBlank() && deviceName.length <= MAX_NAME_LENGTH) {
            txt[KEY_NAME] = deviceName
        }
        return WifiP2pDnsSdServiceInfo.newInstance(
            SERVICE_INSTANCE,
            SERVICE_TYPE,
            txt
        )
    }

    @SuppressLint("MissingPermission")
    fun clearAnnouncements() {
        val c = channel ?: return
        manager.clearLocalServices(c, null)
    }

    /**
     * Discover nearby FastAir devices. Both discovery channels run in
     * parallel: DNS-SD delivers the UUID half, discoverPeers the MAC half.
     * A device is emitted when both are known. [registry] accumulates the
     * halves across discovery sessions so connect() can find the MAC.
     *
     * While the returned flow is alive, the identity announcement and the
     * discovery window are refreshed periodically — see [renew] below.
     */
    fun discover(registry: DiscoveryRegistry): Flow<Device> = callbackFlow {
        val c = ensureChannel()

        /**
         * The framework stops service discovery after ~120s and clears local
         * services and service requests whenever its P2P state machine
         * re-enters the enabled state, so while this flow is alive it
         * rebuilds the full request + announce + discovery set every
         * [REDISCOVER_INTERVAL_MS]. A cycle that lands while a group
         * invitation is in flight fails with BUSY and is skipped; the next
         * cycle recovers.
         */
        suspend fun renew(first: Boolean) {
            val uuid = localUuid ?: return
            manager.clearServiceRequests(c, null)
            manager.addServiceRequest(
                c,
                WifiP2pDnsSdServiceRequest.newInstance(),
                object : WifiP2pManager.ActionListener {
                    override fun onSuccess() {}

                    override fun onFailure(reason: Int) {
                        if (first) {
                            close(IllegalStateException("addServiceRequest failed: $reason"))
                        }
                    }
                }
            )
            manager.clearLocalServices(c, null)
            manager.addLocalService(
                c,
                buildServiceInfo(uuid, localName),
                object : WifiP2pManager.ActionListener {
                    override fun onSuccess() {}

                    override fun onFailure(reason: Int) {
                        if (first) {
                            close(IllegalStateException("addLocalService failed: $reason"))
                        }
                    }
                }
            )
            manager.discoverServices(c, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {}

                override fun onFailure(reason: Int) {
                    if (first) {
                        close(IllegalStateException("discoverServices failed: $reason"))
                    }
                }
            })
        }

        val receiver = object : BroadcastReceiver() {
            override fun onReceive(context: Context?, intent: Intent?) {
                when (intent?.action) {
                    WifiP2pManager.WIFI_P2P_PEERS_CHANGED_ACTION ->
                        @SuppressLint("MissingPermission")
                        manager.requestPeers(c) { result ->
                            result.deviceList.forEach { peer ->
                                registry.onPeer(peer.deviceAddress, peer.deviceName)?.let {
                                    trySend(it)
                                }
                            }
                        }

                    WifiP2pManager.WIFI_P2P_DISCOVERY_CHANGED_ACTION -> {
                        // The framework stops discovery on its own; v1 does
                        // not restart it (the next discover() call does).
                    }
                }
            }
        }

        val txtListener =
            WifiP2pManager.DnsSdTxtRecordListener { _, txtRecord, srcDevice ->
                val uuid = txtRecord?.get(KEY_ID)
                if (uuid == null || uuid == localUuid) {
                    return@DnsSdTxtRecordListener
                }
                val mac = srcDevice?.deviceAddress
                val name = txtRecord[KEY_NAME]
                registry.onService(uuid, name, mac)?.let { trySend(it) }
            }

        val serviceListener =
            WifiP2pManager.DnsSdServiceResponseListener { _, _, _ -> }

        try {
            manager.setDnsSdResponseListeners(c, serviceListener, txtListener)
        } catch (e: Exception) {
            close(e)
            return@callbackFlow
        }

        renew(first = true)

        val renewalJob = launch {
            while (isActive) {
                delay(REDISCOVER_INTERVAL_MS)
                renew(first = false)
            }
        }

        val filter = IntentFilter().apply {
            addAction(WifiP2pManager.WIFI_P2P_PEERS_CHANGED_ACTION)
            addAction(WifiP2pManager.WIFI_P2P_DISCOVERY_CHANGED_ACTION)
        }
        appContext.registerReceiver(receiver, filter)

        awaitClose {
            renewalJob.cancel()
            runCatching { appContext.unregisterReceiver(receiver) }
            runCatching { manager.stopPeerDiscovery(c, null) }
            runCatching { manager.clearServiceRequests(c, null) }
        }
    }

    fun connect(target: Device, registry: DiscoveryRegistry, onResult: (Throwable?) -> Unit) {
        val mac = registry.macOf(target.id.value)
        if (mac == null) {
            onResult(
                IllegalStateException("No MAC known for device ${target.id.value}")
            )
            return
        }
        val c = ensureChannel()
        // The framework drops a connect whose peer address is not in its
        // current peer list (its cache expires quickly). Refresh the list
        // and only invite when the peer is still visible to the radio.
        @SuppressLint("MissingPermission")
        manager.requestPeers(c) { peers ->
            if (peers.deviceList.none { it.deviceAddress == mac }) {
                onResult(
                    IllegalStateException(
                        "Peer ${target.id.value} is no longer visible; rediscover first"
                    )
                )
                return@requestPeers
            }
            val config = WifiP2pConfig().apply {
                deviceAddress = mac
            }
            android.util.Log.i(TAG, "inviting ${target.id.value} at $mac")
            // Abandon any in-flight invitation first: the framework keeps
            // retrying it indefinitely and two attempts must never overlap.
            manager.cancelConnect(c, null)
            manager.connect(c, config, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    android.util.Log.i(
                        TAG, "invitation to ${target.id.value} accepted by framework"
                    )
                    onResult(null)
                }

                override fun onFailure(reason: Int) {
                    android.util.Log.w(
                        TAG, "invitation to ${target.id.value} failed: $reason"
                    )
                    onResult(IllegalStateException("Wi-Fi P2P connect failed: $reason"))
                }
            })
        }
    }

    /**
     * Abandon an in-flight invitation so the framework stops retrying it.
     * Harmless once a group has formed.
     */
    fun cancelConnect() {
        val c = channel ?: return
        manager.cancelConnect(c, null)
    }

    fun groupEvents(): Flow<GroupEvent> = callbackFlow {
        val c = ensureChannel()
        val receiver = object : BroadcastReceiver() {
            override fun onReceive(context: Context?, intent: Intent?) {
                when (intent?.action) {
                    WifiP2pManager.WIFI_P2P_CONNECTION_CHANGED_ACTION -> {
                        val info = intent.parcelableExtra<WifiP2pInfo>(
                            WifiP2pManager.EXTRA_WIFI_P2P_INFO
                        )
                        if (info?.groupFormed == true) {
                            trySend(
                                GroupEvent.GroupFormed(
                                    ownerAddress = info.groupOwnerAddress?.hostAddress,
                                    isOwner = info.isGroupOwner
                                )
                            )
                        } else {
                            trySend(GroupEvent.GroupRemoved)
                        }
                    }
                }
            }
        }

        val filter = IntentFilter().apply {
            addAction(WifiP2pManager.WIFI_P2P_CONNECTION_CHANGED_ACTION)
        }
        appContext.registerReceiver(receiver, filter)

        @SuppressLint("MissingPermission")
        manager.requestConnectionInfo(c) { info ->
            if (info.groupFormed) {
                trySend(
                    GroupEvent.GroupFormed(
                        ownerAddress = info.groupOwnerAddress?.hostAddress,
                        isOwner = info.isGroupOwner
                    )
                )
            }
        }

        awaitClose {
            runCatching { appContext.unregisterReceiver(receiver) }
        }
    }

    fun removeGroup(onResult: (Throwable?) -> Unit = {}) {
        val c = ensureChannel()
        @SuppressLint("MissingPermission")
        manager.removeGroup(c, object : WifiP2pManager.ActionListener {
            override fun onSuccess() {
                onResult(null)
            }

            override fun onFailure(reason: Int) {
                onResult(IllegalStateException("Wi-Fi P2P removeGroup failed: $reason"))
            }
        })
    }

    private inline fun <reified T : Parcelable> Intent?.parcelableExtra(name: String): T? {
        if (this == null) {
            return null
        }
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            getParcelableExtra(name, T::class.java)
        } else {
            @Suppress("DEPRECATION")
            getParcelableExtra(name)
        }
    }

    sealed interface GroupEvent {
        data class GroupFormed(
            val ownerAddress: String?,
            val isOwner: Boolean
        ) : GroupEvent

        data object GroupRemoved : GroupEvent
    }

    companion object {
        private const val TAG = "ZevBond/P2p"

        private const val KEY_ID = "id"

        private const val KEY_NAME = "name"

        private const val MAX_NAME_LENGTH = 32

        const val SERVICE_TYPE = "_zevbond._tcp"

        const val SERVICE_INSTANCE = "_zevbond"

        // The framework ends a discovery session after ~120s; renew well
        // before that so the flow never sits in a stopped state.
        private const val REDISCOVER_INTERVAL_MS = 60_000L
    }
}
