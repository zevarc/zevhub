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
import com.zevarc.zevbond.session.Session
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
                        }
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
                session.messages().collect { message ->
                    appendLog(
                        "recv [${message.type.value}] " +
                            String(message.data, Charsets.UTF_8)
                    )
                }
            }
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
    onSend: (String) -> Unit
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
