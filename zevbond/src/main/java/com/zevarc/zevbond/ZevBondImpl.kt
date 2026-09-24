package com.zevarc.zevbond

import com.zevarc.zevbond.device.Device
import com.zevarc.zevbond.device.DeviceId
import com.zevarc.zevbond.discovery.DiscoveryRegistry
import com.zevarc.zevbond.discovery.WifiP2pLink
import com.zevarc.zevbond.handshake.Handshake
import com.zevarc.zevbond.identity.LocalIdentity
import com.zevarc.zevbond.protocol.FrameIdGenerator
import com.zevarc.zevbond.protocol.ProtocolImpl
import com.zevarc.zevbond.session.Session
import com.zevarc.zevbond.session.SessionImpl
import com.zevarc.zevbond.tcp.TcpLink
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean

internal class ZevBondImpl(
    private val identity: LocalIdentity,
    private val p2p: WifiP2pLink,
    private val tcp: TcpLink,
    private val scope: CoroutineScope
) : ZevBond {

    override val device: Device
        get() = Device(
            id = identity.deviceId,
            name = android.os.Build.MODEL
        )

    private val registry = DiscoveryRegistry()

    private val started = AtomicBoolean(false)
    private val closed = AtomicBoolean(false)

    private val _incomingSessions = MutableSharedFlow<Session>(extraBufferCapacity = 8)

    private val sessions = CopyOnWriteArrayList<Session>()

    /** UUID of the device we invited; used to cross-check the handshake. */
    @Volatile
    private var pendingInvite: DeviceId? = null

    private var groupJob: Job? = null
    private var listenJob: Job? = null
    private var memberJob: Job? = null

    override suspend fun start() {
        check(!started.getAndSet(true)) { "ZevBond already started" }
        check(!closed.get()) { "ZevBond is closed" }

        val announcement = CompletableDeferred<Throwable?>()
        p2p.announceIdentity(
            uuid = identity.deviceId.value,
            deviceName = android.os.Build.MODEL
        ) { announcement.complete(it) }
        announcement.await()?.let {
            android.util.Log.w(TAG, "identity announcement failed", it)
            throw it
        }
        android.util.Log.i(TAG, "started, device id = ${identity.deviceId.value}")

        groupJob = scope.launch {
            p2p.groupEvents().collect { event ->
                android.util.Log.i(TAG, "group event: $event")
                when (event) {
                    is WifiP2pLink.GroupEvent.GroupFormed -> {
                        if (event.isOwner) {
                            startListening()
                        } else {
                            connectToOwner(event.ownerAddress)
                        }
                    }

                    WifiP2pLink.GroupEvent.GroupRemoved -> {
                        stopTcpRole()
                    }
                }
            }
        }
    }

    override fun discover(): Flow<Device> {
        check(started.get()) { "ZevBond not started" }
        return p2p.discover(registry)
    }

    override suspend fun connect(device: Device): Session {
        check(started.get()) { "ZevBond not started" }

        // UNDISPATCHED starts the subscription before the invitation is
        // sent, so a session racing the group formation is never missed.
        val session = CompletableDeferred<Session>()
        val waiter = scope.launch(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) {
            session.complete(_incomingSessions.first())
        }

        pendingInvite = device.id
        try {
            val invitation = CompletableDeferred<Throwable?>()
            p2p.connect(device, registry) { invitation.complete(it) }
            return withTimeout(CONNECT_TIMEOUT_MS) {
                // The callback only means the framework took the request;
                // the session arrives once the peer accepts, the group
                // forms and the TCP link plus handshake complete.
                invitation.await()?.let { error -> throw error }
                session.await()
            }
        } catch (e: TimeoutCancellationException) {
            // The framework retries the invitation forever otherwise.
            p2p.cancelConnect()
            throw IllegalStateException(
                "Peer did not complete the connection within ${CONNECT_TIMEOUT_MS}ms", e
            )
        } finally {
            pendingInvite = null
            waiter.cancel()
        }
    }

    override fun incomingSessions(): Flow<Session> {
        check(started.get()) { "ZevBond not started" }
        return _incomingSessions
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) {
            return
        }
        started.set(false)
        groupJob?.cancel()
        stopTcpRole()
        p2p.clearAnnouncements()
        registry.clear()
        val toClose = sessions.toList()
        sessions.clear()
        scope.launch {
            toClose.forEach { runCatching { it.close() } }
        }
    }

    private fun startListening() {
        if (listenJob?.isActive == true) {
            return
        }
        memberJob?.cancel()
        listenJob = scope.launch {
            try {
                tcp.listen(TcpLink.DEFAULT_PORT).collect { transport ->
                    establishSession(transport, initiator = false, expectedPeer = null)
                }
            } catch (e: Exception) {
                // Bind failure or listener teardown; the group event flow
                // decides whether listening is retried on the next group.
            }
        }
    }

    private fun connectToOwner(ownerAddress: String?) {
        if (ownerAddress == null) {
            return
        }
        if (memberJob?.isActive == true) {
            return
        }
        listenJob?.cancel()
        memberJob = scope.launch {
            try {
                android.util.Log.i(TAG, "connecting to owner at $ownerAddress")
                val transport = tcp.connect(ownerAddress, TcpLink.DEFAULT_PORT)
                // When we invited the peer, the peer's HELLO must carry the
                // UUID we discovered; otherwise the handshake fails.
                establishSession(transport, initiator = true, expectedPeer = pendingInvite)
            } catch (e: Exception) {
                // The group dissolved mid-connect or the owner is gone.
                android.util.Log.w(TAG, "member link to owner failed", e)
            }
        }
    }

    private suspend fun establishSession(
        transport: com.zevarc.zevbond.transport.ByteTransport,
        initiator: Boolean,
        expectedPeer: DeviceId?
    ) {
        val protocol = ProtocolImpl(transport, scope)
        // One frame id generator per connection: handshake and session share
        // the session-local monotonic sequence, per the wire spec.
        val connectionFrameIds = FrameIdGenerator()
        val peerDeviceId = try {
            Handshake(
                localDeviceId = identity.deviceId,
                frameIds = connectionFrameIds,
                expectedPeerDeviceId = expectedPeer
            ).run(protocol, initiator)
        } catch (e: Exception) {
            // Handshake failure must not produce a partially usable session.
            android.util.Log.w(TAG, "handshake failed", e)
            runCatching { protocol.close() }
            return
        }
        android.util.Log.i(TAG, "session established with ${peerDeviceId.value}")
        val session = SessionImpl(
            device = Device(peerDeviceId, name = null),
            protocol = protocol,
            scope = scope,
            frameIds = connectionFrameIds
        )
        sessions.add(session)
        _incomingSessions.emit(session)
    }

    private fun stopTcpRole() {
        listenJob?.cancel()
        listenJob = null
        memberJob?.cancel()
        memberJob = null
    }

    companion object {
        private const val TAG = "ZevBond"

        private const val CONNECT_TIMEOUT_MS = 30_000L
    }
}
