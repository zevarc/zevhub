package com.zevarc.zevbond.app

import android.Manifest
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TextField
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.zevarc.zevbond.ZevBond
import com.zevarc.zevbond.ZevBondFactory
import com.zevarc.zevbond.app.ui.theme.ZevBondTheme
import com.zevarc.zevbond.device.Device
import com.zevarc.zevbond.message.Message
import com.zevarc.zevbond.message.MessageType
import com.zevarc.zevbond.request.Request
import com.zevarc.zevbond.request.RequestType
import com.zevarc.zevbond.request.Response
import com.zevarc.zevbond.session.Session
import com.zevarc.zevbond.transfer.TransferDestination
import com.zevarc.zevbond.transfer.TransferMetadata
import com.zevarc.zevbond.transfer.TransferResult
import com.zevarc.zevbond.transfer.TransferSource
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch

class MainActivity : ComponentActivity() {

    private val log = MutableStateFlow<List<String>>(emptyList())
    private val peers = MutableStateFlow<List<Device>>(emptyList())
    private val connectedSession = MutableStateFlow<Session?>(null)

    private var runtime: ZevBond? = null
    private var scope: CoroutineScope? = null

    private val permissionRequest = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { granted ->
        if (granted.values.all { it }) {
            startRuntime()
        } else {
            appendLog("permissions denied (nearby/location)")
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()

        setContent {
            ZevBondTheme {
                Scaffold(modifier = Modifier.fillMaxSize()) { padding ->
                    val logLines by log.collectAsState()
                    val peerList by peers.collectAsState()
                    val session by connectedSession.collectAsState()

                    DebugScreen(
                        logLines = logLines,
                        peers = peerList,
                        connected = session != null,
                        onDiscover = ::startDiscovery,
                        onConnect = { device ->
                            uiScope().launch {
                                val instance = runtime
                                if (instance == null) {
                                    appendLog("runtime not started")
                                    return@launch
                                }
                                runCatching { instance.connect(device) }
                                    .onSuccess { established ->
                                        connectedSession.value = established
                                        appendLog("connected: ${established.device.id.value}")
                                    }
                                    .onFailure { appendLog("connect failed: ${it.message}") }
                            }
                        },
                        onSend = { text ->
                            uiScope().launch {
                                runCatching {
                                    session?.send(
                                        Message(MessageType("chat"), text.toByteArray())
                                    )
                                }.onFailure { appendLog("send failed: ${it.message}") }
                            }
                        },
                        onPing = ::sendPing,
                        onTransfer = ::sendPayload,
                        onStream = ::sendStreamBurst
                    )
                }
            }
        }
    }

    override fun onStart() {
        super.onStart()
        requestPermissions()
    }

    override fun onDestroy() {
        scope?.cancel()
        runtime?.close()
        super.onDestroy()
    }

    private fun requestPermissions() {
        val wanted = if (Build.VERSION.SDK_INT >= 33) {
            arrayOf(Manifest.permission.NEARBY_WIFI_DEVICES)
        } else {
            arrayOf(Manifest.permission.ACCESS_FINE_LOCATION)
        }
        val missing = wanted.filter {
            checkSelfPermission(it) != PackageManager.PERMISSION_GRANTED
        }
        if (missing.isEmpty()) {
            startRuntime()
        } else {
            appendLog("requesting permissions: ${missing.joinToString()}")
            permissionRequest.launch(wanted)
        }
    }

    private fun startRuntime() {
        val existing = runtime
        if (existing != null) {
            return
        }
        val newScope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
        scope = newScope
        val instance = ZevBondFactory.create(this, newScope)
        runtime = instance

        newScope.launch {
            runCatching { instance.start() }
                .onSuccess { appendLog("started, device = ${instance.device.id.value}") }
                .onFailure { appendLog("start failed: ${it.message}") }
        }
        newScope.launch {
            instance.incomingSessions().collect { session ->
                appendLog("incoming session: ${session.device.id.value}")
                connectedSession.value = session
                watchSession(session)
            }
        }
    }

    private fun watchSession(session: Session) {
        // Any of the three streams ending means the session is gone;
        // drop the CONNECTED state once, from whichever notices first.
        val onSessionDown = {
            if (connectedSession.value === session) {
                connectedSession.value = null
                appendLog("session ended: ${session.device.id.value}")
            }
        }
        uiScope().launch {
            runCatching {
                session.messages().collect { message ->
                    appendLog(
                        "recv [${message.type.value}] " +
                            String(message.data, Charsets.UTF_8)
                    )
                }
            }.onFailure { appendLog("message stream ended: ${it.message}") }
            onSessionDown()
        }
        // Auto-responder: echo a ping request back as a success response.
        uiScope().launch {
            runCatching {
                session.requests().collect { incoming ->
                    if (incoming.type.value == "ping") {
                        incoming.respond(Response.Success(incoming.data))
                    } else {
                        incoming.respond(
                            Response.Failure(errorCode = 404, errorData = "unknown type".toByteArray())
                        )
                    }
                }
            }.onFailure { appendLog("request stream ended: ${it.message}") }
            onSessionDown()
        }
        // Accept every offered transfer into a byte-counting destination.
        uiScope().launch {
            runCatching {
                session.transfers().collect { incoming ->
                    appendLog(
                        "transfer offer: ${incoming.metadata.type}" +
                            " (${incoming.totalBytes ?: "unknown"} bytes)"
                    )
                    val started = System.nanoTime()
                    val received = incoming.accept(CountingDestination { count ->
                        appendLog("transfer progress: $count bytes")
                    })
                    runCatching {
                        when (val result = received.await()) {
                            is TransferResult.Success -> appendLog(
                                "transfer done: ${result.bytesDelivered} bytes" +
                                    " in ${(System.nanoTime() - started) / 1_000_000}ms"
                            )
                            is TransferResult.Cancelled ->
                                appendLog("transfer cancelled")
                        }
                    }.onFailure { appendLog("transfer failed: ${it.message}") }
                }
            }.onFailure { appendLog("transfer stream ended: ${it.message}") }
            onSessionDown()
        }
        // Accept every offered stream and log its frames until close.
        uiScope().launch {
            runCatching {
                session.streams().collect { offer ->
                    appendLog("stream offer: ${offer.type.value}")
                    val stream = offer.accept()
                    launch {
                        runCatching {
                            var frames = 0
                            var bytes = 0L
                            stream.receive().collect { frame ->
                                frames++
                                bytes += frame.size
                            }
                            appendLog("stream done: $frames frames, $bytes bytes")
                        }.onFailure { appendLog("stream failed: ${it.message}") }
                    }
                }
            }.onFailure { appendLog("stream offer flow ended: ${it.message}") }
            onSessionDown()
        }
    }

    /** Opens a stream, sends 100 numbered frames, and closes it. */
    private fun sendStreamBurst() {
        uiScope().launch {
            val session = connectedSession.value
            if (session == null) {
                appendLog("stream failed: not connected")
                return@launch
            }
            runCatching {
                val stream = session.openStream(
                    com.zevarc.zevbond.stream.StreamType("burst")
                )
                appendLog("stream opened: ${stream.type.value}")
                val started = System.nanoTime()
                repeat(100) { index ->
                    stream.send("frame-$index".toByteArray())
                }
                stream.close()
                appendLog(
                    "stream sent: 100 frames in" +
                        " ${(System.nanoTime() - started) / 1_000_000}ms"
                )
            }.onFailure { appendLog("stream failed: ${it.message}") }
        }
    }

    /** Logs the running byte count at a coarse interval. */
    private class CountingDestination(
        private val onCount: (Long) -> Unit
    ) : TransferDestination {
        var count = 0L
        override suspend fun write(data: ByteArray) {
            count += data.size
        }
        override suspend fun close() {
            onCount(count)
        }
    }

    private fun sendPing() {
        uiScope().launch {
            val session = connectedSession.value
            if (session == null) {
                appendLog("ping failed: not connected")
                return@launch
            }
            val nonce = java.util.UUID.randomUUID()
                .toString().substring(0, 8).toByteArray()
            val started = System.nanoTime()
            runCatching {
                session.request(Request(RequestType("ping"), nonce))
            }.onSuccess { response ->
                val elapsed = (System.nanoTime() - started) / 1_000_000
                when (response) {
                    is Response.Success ->
                        if (response.data.contentEquals(nonce)) {
                            appendLog("ping ok in ${elapsed}ms")
                        } else {
                            appendLog("ping mismatch: nonce echoed incorrectly")
                        }
                    is Response.Failure ->
                        appendLog("ping failed: code=${response.errorCode}")
                }
            }.onFailure { appendLog("ping failed: ${it.message}") }
        }
    }

    private fun sendPayload() {
        uiScope().launch {
            val session = connectedSession.value
            if (session == null) {
                appendLog("send failed: not connected")
                return@launch
            }
            appendLog("generating 2 MiB payload...")
            val data = ByteArray(2 * 1024 * 1024) { (it % 251).toByte() }
            val started = System.nanoTime()
            val source = object : TransferSource {
                override val metadata = TransferMetadata("payload", "2mib".toByteArray())
                override val totalBytes = data.size.toLong()
                private var offset = 0
                override suspend fun read(maxBytes: Int): ByteArray? {
                    if (offset >= data.size) return null
                    val end = minOf(offset + maxBytes, data.size)
                    val chunk = data.copyOfRange(offset, end)
                    offset = end
                    return chunk
                }
            }
            val transfer = session.transfer(source)
            runCatching {
                when (val result = transfer.await()) {
                    is TransferResult.Success -> appendLog(
                        "sent ${result.bytesDelivered} bytes" +
                            " in ${(System.nanoTime() - started) / 1_000_000}ms"
                    )
                    is TransferResult.Cancelled -> appendLog("send cancelled")
                }
            }.onFailure { appendLog("send failed: ${it.message}") }
        }
    }

    private fun startDiscovery() {
        val instance = runtime ?: return
        uiScope().launch {
            appendLog("discovering...")
            runCatching {
                instance.discover().collect { device ->
                    appendLog("peer: ${device.name ?: device.id.value}")
                    val current = peers.value
                    if (current.none { it.id == device.id }) {
                        peers.value = current + device
                    }
                }
            }.onFailure { appendLog("discover failed: ${it.message}") }
        }
    }

    private fun appendLog(line: String) {
        log.value = log.value + line
    }

    private fun uiScope(): CoroutineScope =
        scope ?: CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate).also {
            scope = it
        }
}

@Composable
fun DebugScreen(
    logLines: List<String>,
    peers: List<Device>,
    connected: Boolean,
    onDiscover: () -> Unit,
    onConnect: (Device) -> Unit,
    onSend: (String) -> Unit,
    onPing: () -> Unit,
    onTransfer: () -> Unit,
    onStream: () -> Unit
) {
    var input by remember { mutableStateOf("") }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Text(
            text = if (connected) "CONNECTED" else "NOT CONNECTED",
            style = MaterialTheme.typography.titleMedium
        )

        Button(onClick = onDiscover) {
            Text("Discover")
        }

        peers.forEach { peer ->
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                Text(peer.name ?: peer.id.value)
                Button(onClick = { onConnect(peer) }) {
                    Text("Connect")
                }
            }
        }

        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Button(onClick = onPing, enabled = connected) {
                Text("Ping")
            }
            Button(onClick = onTransfer, enabled = connected) {
                Text("2MiB")
            }
            Button(onClick = onStream, enabled = connected) {
                Text("Stream")
            }
            TextField(
                value = input,
                onValueChange = { input = it },
                modifier = Modifier.weight(1f)
            )
            Button(
                onClick = {
                    onSend(input)
                    input = ""
                },
                enabled = connected
            ) {
                Text("Send")
            }
        }

        LazyColumn(modifier = Modifier.weight(1f)) {
            items(logLines.reversed()) { line ->
                Text(line, style = MaterialTheme.typography.bodySmall)
            }
        }
    }
}
