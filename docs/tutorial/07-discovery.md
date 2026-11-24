# 07 · Wi-Fi P2P 的坑：双通道发现与组网

> 对应代码：`discovery/WifiP2pLink.kt`、`discovery/DiscoveryRegistry.kt`、`handshake/Handshake.kt`、`handshake/HandshakeCodec.kt`、`identity/LocalIdentity.kt`
> 对应测试：`DiscoveryRegistryTest`（8 个）、`HandshakeTest`（2 个）、`LocalIdentityTest`（6 个）

这一层最不像"写代码"，更像"和平台的怪脾气打交道"。以下全部是被平台逼出来的。

## 坑 1：Wi-Fi P2P 给你的是 MAC，但 MAC 不是身份

平台的现实：

- `discoverPeers` / `requestPeers` 给你 `deviceAddress`（MAC）+ `deviceName`
- DNS-SD（`WifiP2pDnsSdServiceInfo`）给你一个自定义 TXT 记录 + **是哪个 MAC 发的**
- MAC 会变（随机化、换设备、DHCP 重分配后你对它的信任没有意义）
- 而"我要连的是谁"必须是**稳定身份**

所以本项目广告的是 UUID：

```kotlin
// WifiP2pLink.kt
private const val KEY_ID = "id"
private const val KEY_NAME = "name"
const val SERVICE_TYPE = "_zevhub._tcp"
const val SERVICE_INSTANCE = "_zevhub"
```

`Id` 来自 `LocalIdentity`，是首次启动生成、存在 `SharedPreferences` 里的 32 位小写十六进制：

```kotlin
// identity/LocalIdentity.kt
private val DEVICE_ID_PATTERN = Regex("^[0-9a-f]{32}$")
fun generateDeviceId(): String = UUID.randomUUID().toString().replace("-", "")
```

**为什么不是 `ANDROID_ID`？** 因为 `ANDROID_ID` 在恢复出厂/换用户后会变，而且它是"装在某台机器上"的标识，不是"这个 App 的设备身份"。自己生成至少语义清楚，而且将来能跟密钥对绑定（`HandshakeCodec` 里已经留了 `PUBLIC_KEY_PLACEHOLDER`）。

**为什么校验格式？** 因为它是从 `SharedPreferences` 读的，可能被别的版本、被人手改、被迁移流程写坏：

```kotlin
fun create(stored: String?, persist: (String) -> Unit): LocalIdentity {
    val id = stored?.takeIf { isValidDeviceIdFormat(it) }
        ?: generateDeviceId().also(persist)
    return LocalIdentity(DeviceId(id))
}
```

**格式不对就重新生成**，而不是抛异常。理由：身份失效的后果（用户重新被发现一次）远小于启动失败的后果。对应测试 `regenerates when the stored id is malformed` / `wrong length`。

## 坑 2：两条发现通道，各自只给一半

| 通道 | 给什么 | 什么时候来 |
|---|---|---|
| DNS-SD TXT listener | UUID + name + **peer MAC** | 广播到达 |
| `requestPeers` | MAC + name | `PEERS_CHANGED` 时 |

**两条通道的顺序不确定。** DNS-SD 可能先来（但那次 TXT 记录没有 MAC，因为平台偶尔给 null），也可能 `PEERS_CHANGED` 先来（那时还没有 UUID）。

于是中间有一个**合并器**，`DiscoveryRegistry`：

```kotlin
fun onService(uuid: String, name: String?, mac: String?): Device? {
    if (mac == null) {
        if (name != null) pendingByUuid.putIfAbsent(uuid, name)   // 先停车
        return null
    }
    val entry = byMac.getOrPut(mac) { Entry() }
    if (name != null) entry.name = name
    if (entry.uuid == null) {
        entry.uuid = uuid
        uuidToMac[uuid] = mac
        if (entry.name == null) entry.name = pendingByUuid[uuid]  // 回填空位
        pendingByUuid.remove(uuid)
    }
    return entry.emitIfComplete()
}

fun onPeer(mac: String, name: String?): Device? {
    val entry = byMac.getOrPut(mac) { Entry() }
    if (name != null) entry.name = name
    return entry.emitIfComplete()
}
```

核心规则只有一条：

```kotlin
private fun Entry.emitIfComplete(): Device? {
    val uuid = uuid ?: return null      // ← 没有 UUID 就不是身份，不发
    if (emitted) return null            // ← 同一个设备只发一次
    emitted = true
    return Device(id = DeviceId(uuid), name = name)
}
```

**没有 UUID 的 MAC 一律不发。** 这条规则让"设备发现"的语义干净：`discover()` 流里的每一项都是一个**可连接的、有稳定身份的**设备，不是一个可能随时消失的 MAC。

8 个测试全是这条规则的边界：

```kotlin
`emits a device once uuid and mac are both known - service first`
`emits a device once uuid and mac are both known - peer first`
`does not emit a peer without a uuid`
`does not emit twice for the same device`
`parked name is backfilled when the mac arrives`
`explicit name wins over the parked one`
`macOf resolves the connect handle for a discovered uuid`
`two devices with different uuids on different macs stay separate`
```

这个类**完全不需要 Android 环境**——纯 `ConcurrentHashMap`，纯 JVM 能测。这是"把平台代码挤到最边缘"的收益：本应是 Android 最难测的一层，实际最好测。

## 坑 3：TXT 记录有硬限制

```kotlin
private fun buildServiceInfo(uuid: String, deviceName: String?): WifiP2pDnsSdServiceInfo {
    val txt = mutableMapOf(KEY_ID to uuid)
    if (!deviceName.isNullOrBlank() && deviceName.length <= MAX_NAME_LENGTH) {
        txt[KEY_NAME] = deviceName
    }
    return WifiP2pDnsSdServiceInfo.newInstance(SERVICE_INSTANCE, SERVICE_TYPE, txt)
}
private const val MAX_NAME_LENGTH = 32
```

DNS-SD TXT 是"key=value"字符串，总长有 255 字节的硬限制。超了 `newInstance` 会抛异常或者广告失败。

`deviceName` 来自 `android.os.Build.MODEL`，长度不可控（有些厂商写成 `SM-G9980 Build/XXXX`）。所以**超限就不放名字**，宁可让 `Device.name` 是 null 也不能整个广告失败。`Device.name` 本来就是 `String?`（[01 篇](01-interface.md)），这里正好用上。

## 坑 4：平台会自己停掉发现，而你不会收到提醒

两个事实：

- `discoverServices` 大约 **120 秒**后被框架自行停止
- P2P 状态机重新进入 enabled 状态时，会**清掉**所有 local service 和 service request

后果：如果你只是"调一次 discover 然后收集 Flow"，两分钟后 `discover()` 就再也不产出了，而且**不报错、不结束**。用户看到的是"找不到设备了"。

所以 `discover()` 是一个**会自我续期的 callbackFlow**：

```kotlin
suspend fun renew(first: Boolean) {
    val uuid = localUuid ?: return
    manager.clearServiceRequests(c, null)
    manager.addServiceRequest(c, WifiP2pDnsSdServiceRequest.newInstance(), listener)
    manager.clearLocalServices(c, null)
    manager.addLocalService(c, buildServiceInfo(uuid, localName), listener)
    manager.discoverServices(c, listener)
}

val renewalJob = launch {
    while (isActive) {
        delay(REDISCOVER_INTERVAL_MS)
        renew(first = false)
    }
}
```

```kotlin
// The framework ends a discovery session after ~120s; renew well
// before that so the flow never sits in a stopped state.
private const val REDISCOVER_INTERVAL_MS = 60_000L
```

**60 秒 < 120 秒**，留一半余量。`first = true` 时失败要 `close(异常)`（真的起不来），后续失败只跳过（可能撞上了正在组网，`BUSY`）：

```kotlin
manager.addServiceRequest(c, req, object : WifiP2pManager.ActionListener {
    override fun onSuccess() {}
    override fun onFailure(reason: Int) {
        if (first) close(IllegalStateException("addServiceRequest failed: $reason"))
    }
})
```

**这个"续期"逻辑有个取舍值得点出来**：`clearLocalServices` 后立刻 `addLocalService`，意味着一个正在扫描你的对端可能看到你"消失又出现"。对 P2P 来说可接受（它本来就是周期性发现），但如果你要的是"长连接广告"，就得改成只在必要时重建。

## 坑 5：邀请会被框架无限重试

`WifiP2pManager.connect` 的 `ActionListener.onSuccess` **只表示框架接受了请求**，不表示对方同意了。而且框架会一直重试邀请，直到：

- 对方接受，组形成
- 对方拒绝
- 有人调 `cancelConnect`

所以 `ZevHubImpl.connect()` 失败时必须主动取消：

```kotlin
} catch (e: TimeoutCancellationException) {
    // The framework retries the invitation forever otherwise.
    p2p.cancelConnect()
    throw IllegalStateException("Peer did not complete the connection within ${CONNECT_TIMEOUT_MS}ms", e)
}
```

不取消的话：用户点了"连接"一次，失败了，那个邀请还在后台重试，下一次可能莫名其妙连上了一个**几十秒前**的目标。

## 坑 6：邀请前必须确认对方还在 peer 列表里

```kotlin
fun connect(target: Device, registry: DiscoveryRegistry, onResult: (Throwable?) -> Unit) {
    val mac = registry.macOf(target.id.value)
    if (mac == null) {
        onResult(IllegalStateException("No MAC known for device ${target.id.value}"))
        return
    }
    // The framework drops a connect whose peer address is not in its
    // current peer list (its cache expires quickly). Refresh the list
    // and only invite when the peer is still visible to the radio.
    manager.requestPeers(c) { peers ->
        if (peers.deviceList.none { it.deviceAddress == mac }) {
            onResult(IllegalStateException("Peer ${target.id.value} is no longer visible; rediscover first"))
            return@requestPeers
        }
        manager.cancelConnect(c, null)      // 先放弃在飞的邀请
        manager.connect(c, WifiP2pConfig().apply { deviceAddress = mac }, listener)
    }
}
```

注意 `macOf` 的存在意义：**`Device` 里没有 MAC（[01 篇](01-interface.md)），MAC 只活在 `DiscoveryRegistry` 里**。上层拿着 `Device` 来连，链路自己去查 MAC。这就是"上层不关心怎么连"的具体兑现。

这条检查的代价是一次额外的 `requestPeers`（异步、有延迟）。收益是错误信息可读：用户看到 "rediscover first"，而不是一个 `reason=2`（`BUSY`）或者静默失败。

## 坑 7：组主选出来之后，两端做完全不同的事

```kotlin
groupJob = scope.launch {
    p2p.groupEvents().collect { event ->
        when (event) {
            is WifiP2pLink.GroupEvent.GroupFormed -> {
                if (event.isOwner) startListening()          // 组主：监听
                else connectToOwner(event.ownerAddress)       // 成员：拨号
            }
            WifiP2pLink.GroupEvent.GroupRemoved -> stopTcpRole()
        }
    }
}
```

这层"组主/成员"的分叉**不能上移到 `Session`**，因为它是链路拓扑，跟协议无关。如果将来换成"局域网直连"链路，就没有 owner/member 这个概念，但它对 `Session` 完全透明。

组主监听时顺手取消成员角色：

```kotlin
private fun startListening() {
    if (listenJob?.isActive == true) return
    memberJob?.cancel()
    listenJob = scope.launch { tcp.listen(TcpLink.DEFAULT_PORT).collect { ... } }
}
```

因为同一台设备可能**先当成员（连过一个组），然后自己建组当组主**。旧角色的协程必须清掉，否则两个连接同时存在。

## 坑 8：握手时交叉校验 UUID

组网成功 ≠ 连的是对的人。组里可能有多个设备，或者你被中间设备误导。所以组成员在拨号时带上"我期望的对端 UUID"：

```kotlin
// ZevHubImpl.connect()
pendingInvite = device.id
...
establishSession(transport, initiator = true, expectedPeer = pendingInvite)
```

```kotlin
// Handshake.receiveHello()
expectedPeerDeviceId?.let { expected ->
    require(hello.deviceId == expected) {
        "Peer device id mismatch: expected ${expected.value}, got ${hello.deviceId.value}"
    }
}
```

这条校验对"我是组主、别人连我"的场景是 `null`（我不知道谁会来），对"我主动连"的场景是必须的。

`Handshake.run` 的六步顺序严格交替，两端是镜像的：

```kotlin
if (initiator) {
    sendHello(); val peerHello = receiveHello()
    sendAuth();  receiveAuth()
    sendReady(); receiveReady()
    return peerHello.deviceId
} else {
    val peerHello = receiveHello(); sendHello()
    receiveAuth(); sendAuth()
    receiveReady(); sendReady()
    return peerHello.deviceId
}
```

**为什么不等所有帧都发出再一起等？** 因为 `Protocol.receiveOne()` 是严格一问一答的（[05 篇](05-framing.md)），而 `ProtocolImpl` 的 `incoming` channel 容量有限。交替收发是最不容易死锁的模式。

## 坑 9：握手是空的，这是个已知的窟窿

```kotlin
// Handshake.kt
private suspend fun sendAuth(protocol: Protocol) {
    protocol.send(ProtocolFrame(
        kind = ProtocolKind.AUTH,
        frameId = frameIds.next(),
        payload = HandshakeCodec.encodeAuth(EMPTY_SIGNATURE)   // ← ByteArray(0)
    ))
}

private suspend fun receiveAuth(protocol: Protocol) {
    val frame = protocol.receiveOne()
    require(frame.kind == ProtocolKind.AUTH) { "Expected AUTH, got ${frame.kind}" }
    // Signature verification is deferred to the identity stage.
}
```

现在的 AUTH **只检查帧类型，不检查任何内容**。`HELLO` 里也只有一个空的 `PUBLIC_KEY_PLACEHOLDER`。

也就是说：**任何能进同一个 P2P 组的设备都能跟你会话，且没人能验证载荷没被篡改。**

格式已经留好了位置（`HandshakeCodec` 的 `HELLO` 结构里有 `publicKey`、`nonce`，`AUTH` 里有 `signature`），补的是密码学，不是格式。这是 [ROADMAP](../ROADMAP.md) 阶段 3。

**现在能做的只有一件事：不要把它当成安全的。** 目前的定位是"同一房间内的可信设备互联"，不是"陌生人之间安全通信"。

## 本篇自检

- 你广告的是稳定身份还是网络地址？地址变了怎么办？
- 你的发现流程会被平台超时终止吗？你怎么知道？
- 多条发现通道给了不同的信息，你合并时"什么时候才算一条完整记录"？
- 邀请失败后你取消了吗？不取消会怎样？
- 连接时你验证了对方就是你想要的那个设备吗？
- 你的握手现在真的在验证什么吗？

---

下一篇：[08 · 大块数据](08-transfer.md)
