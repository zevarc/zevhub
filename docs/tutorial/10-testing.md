# 10 · 不用真机测 P2P：103 个测试怎么搭

> 对应代码：`zevhub/src/test/java/com/zevarc/zevhub/`
> 基线：16 个测试类 / 103 个测试，`:zevhub:testDebugUnitTest` 全绿

## 核心武器：`InMemoryTransport`

整套测试体系只有一个关键道具——一个 40 行的假链路：

```kotlin
// src/test/java/com/zevarc/zevhub/protocol/InMemoryTransport.kt
class InMemoryTransport : ByteTransport {

    private val incoming = Channel<ByteArray>(Channel.UNLIMITED)

    /** Every chunk this side has sent, in order. */
    val sentChunks = ConcurrentLinkedQueue<ByteArray>()

    lateinit var peer: InMemoryTransport

    override suspend fun send(data: ByteArray) {
        sentChunks.add(data)
        peer.incoming.send(data)
    }

    override fun receive(): Flow<ByteArray> = incoming.receiveAsFlow()

    override suspend fun close() {
        incoming.close()
        // A local close is an EOF on the peer's read side, like TCP.
        runCatching { peer.incoming.close() }
    }

    fun pairWith(other: InMemoryTransport) {
        peer = other
        other.peer = this
    }

    /** Simulates the remote side dying with a failure. */
    fun failFromRemote(cause: Throwable) {
        incoming.close(cause)
    }
}
```

它能干什么：

| 能力 | 用来测什么 |
|---|---|
| `send` 直接变成对端的 `receive` | 打通整条协议栈，不需要 socket |
| `sentChunks` 记录原始字节 | 断言 ACK / ERROR 这类**协议自发帧**，不用再解码一遍 |
| `close()` 双向关闭 | 模拟"对端优雅断开" |
| `failFromRemote(cause)` | 模拟"链路异常断开" |
| 容量 `UNLIMITED` | 发送不挂起，测试调度确定 |

**为什么是 `UNLIMITED`？** 如果容量有限，`send` 会挂起，测试就依赖调度顺序，变得不可复现。`InMemoryTransport` 的目的是"验证逻辑"，不是"验证背压"——背压由 `TcpTransport` 的 16 容量负责，那里有别的测法。

`sentChunks` 那次用法值得看（`ProtocolImplTest`）：

```kotlin
@Test
fun `receiving a message produces an ack frame naming its frame id`() = runTest {
    ...
    // The ACK is visible in the raw sent chunks of the server side.
    val ack = withTimeout(1_000) {
        while (serverTransport.sentChunks.isEmpty()) { delay(10) }
        WireDecoder().feed(serverTransport.sentChunks.first())
    }
    assertEquals(ProtocolKind.ACK.value, ack[0].kind)
    assertArrayEquals(byteArrayOf(0, 0, 0, 0, 0, 0, 0, 100), ack[0].payload)
}
```

**直接读原始字节**，而不是"期望某个上层回调"。ACK 不会进上层（[05 篇](05-framing.md)），所以只能这么测。

## 测试分四档

| 档 | 类 | 数量 | 打什么 |
|---|---|---:|---|
| 纯逻辑 | `WireCodecTest` `WireDecoderTest` `MessageCodecTest` `RequestCodecTest` `ResponseCodecTest` `TransferCodecTest` `DiscoveryRegistryTest` `LocalIdentityTest` | 56 | 编解码/合并/ID，**零依赖** |
| 协议 | `ProtocolImplTest` `HandshakeTest` | 9 | ACK、ERROR、心跳判死、序号、握手交叉校验 |
| 会话 | `SessionImplTest` `SessionRequestTest` `TransferTest` `StreamTest` | 34 | 端到端语义 |
| 真实 socket | `TcpLoopbackTest` | 1 | 传输层真的能连 |
| 模板 | `ExampleUnitTest` | 1 | Android Studio 模板自带，**建议删** |

### 第一档：纯 JVM，跑得极快

`DiscoveryRegistry` 和 `LocalIdentity` **完全不碰 Android**：

```kotlin
private fun Entry.emitIfComplete(): Device? { ... }    // ConcurrentHashMap
fun create(stored: String?, persist: (String) -> Unit): LocalIdentity { ... }
```

注意 `LocalIdentity.create` 把**持久化抽成了 `(String) -> Unit`**：

```kotlin
fun from(context: Context): LocalIdentity {
    val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
    return create(
        stored = prefs.getString(KEY_DEVICE_ID, null),
        persist = { id -> prefs.edit().putString(KEY_DEVICE_ID, id).apply() }
    )
}
```

于是测试里就是：

```kotlin
@Test
fun `reuses the stored id across restarts`() {
    var stored: String? = null
    val first = LocalIdentity.create(stored) { stored = it }
    val second = LocalIdentity.create(stored) { stored = it }
    assertEquals(first.deviceId, second.deviceId)
}
```

**这是最有价值的一条重构技巧**：只要把"平台调用"变成一个函数参数，这个类就从"只能 instrumented 测"变成"纯 JVM 测"。

项目里 `zevhub/build.gradle.kts` 还有一条配套：

```kotlin
testOptions {
    unitTests {
        isReturnDefaultValues = true
    }
}
```

它让 `android.util.Log.w` 之类的调用在 JVM 上返回默认值而不是抛异常——**这就是为什么 `SessionImpl` 里可以放心写 `Log.w` 还能在单测里跑**。代价：日志在测试里静默，出问题要看断言而不是日志。

### 第二档：协议层，只需内存管道

```kotlin
private fun pairedProtocols(scope: CoroutineScope): Pair<ProtocolImpl, ProtocolImpl> {
    val clientTransport = InMemoryTransport()
    val serverTransport = InMemoryTransport()
    clientTransport.pairWith(serverTransport)
    return ProtocolImpl(clientTransport, scope) to ProtocolImpl(serverTransport, scope)
}
```

"收到乱序序号要关连接"这种测试，可以直接构造**坏帧**发过去：

```kotlin
@Test
fun `out of order transfer data fails the protocol with a violation code`() {
    // 手工 WireCodec.encode 一个 sequence 跳号的 TRANSFER_DATA
    // 断言对端收到 ERROR 帧且 payload 里的 code 是 CODE_PROTOCOL_VIOLATION
}
```

**能手工造坏帧**是这一层的最大价值。真机上你没法让对端"故意发错"，但在测试里可以。

### 第三档：会话层，覆盖所有"难看"的路径

`SessionRequestTest` 13 个测试，全是异常路径：

```kotlin
`request is answered by the peer and response reaches the caller`     ← 唯一正常路径
`concurrent requests resume their own callers despite out of order responses`
`request times out locally without signalling the protocol`
`cancelling a request stops the local wait and keeps the session usable`
`orphan response is dropped and the session stays usable`
`failure response is delivered as a value and the session stays usable`
`transport death fails pending requests instead of hanging`
`responding twice throws`
`malformed request payload kills the session with an error frame`
`oversized request fails locally without touching the wire`
`local close fails pending requests instead of waiting for the timeout`
`session closed reports a link failure to its owner`
`closing a session locally does not look like a link failure`
```

`TransferTest` 11 个：

```kotlin
`small transfer round trips with correct result`
`large transfer survives chunking across many data frames`      ← 3 MiB
`progress reports monotonic bytes and total`
`reject on the receiver cancels the sender`
`local cancel stops the transfer and notifies the peer`
`session death fails an in-flight transfer`
`concurrent transfers do not interfere`
`transfer leaves message and request paths usable`
`each offer is delivered exactly once`
`ignored offer times out and cancels the sender`                ← 虚拟时间
`accepted offer keeps working after the timeout window passes`
```

**"难看"的路径才是 SDK 的核心。** 正常路径一测就过，异常路径不测就是线上事故。

### 第四档：真实 socket，只测一条

```kotlin
/**
 * End-to-end over a real localhost TCP socket: everything the P2P base
 * carries except the Wi-Fi radio itself.
 */
class TcpLoopbackTest {
    @Test
    fun `handshake and message round trip over a real tcp connection`() = runBlocking { ... }
}
```

**只有一条**，而且用的是 `runBlocking` 而不是 `runTest`——因为它真的在等 IO，虚拟时间会瞬间跳过所有超时，测试直接失败。

为什么只留一条？因为反复连 socket 会：

- 端口冲突（并行测试时）
- flaky（CI 上没有回环网络策略的机器不常见但存在）
- 慢

**一条足够了**：它证明 `TcpTransport` + `TcpLink` 的接线没错。协议语义全在第二、三档覆盖。

## 三条测试纪律

### 纪律一：`yield()` 作为"让出执行权"的显式同步

几乎每个测试都有：

```kotlin
val acceptor = async { server.streams().first().accept() }
repeat(5) { yield() }
val stream = client.openStream(StreamType("chat"))
```

**这是必要的**，因为 `Flow` 的收集者是协程，`async` 启动之后不代表它已经订阅上。`yield` 5 次是"足够让调度器把它推进到挂起点"的实用手法。

为什么不 `delay(10)`？因为在 `runTest` 的虚拟时间里 `delay` 不消耗真实时间但**也不真的等调度**，行为取决于实现。`yield` 语义明确。

**这在测试里是可接受的**，但在生产代码里绝不能这么写。区别：测试可以假设"系统空闲时调度均匀"。

### 纪律二：`backgroundScope` 而不是 `runTest` 的 scope

```kotlin
fun `...`() = runTest {
    val (client, server) = pairedSessions(backgroundScope)   // ← 不是 this
    ...
}
```

`runTest` 会在测试体结束时**等待所有子协程**。而 `SessionImpl` 的 `pumpJob` 是**永不到期**的协程（它一直在 `collect`）。用测试 scope 会永久挂起。

`backgroundScope` 里的协程在测试结束时被取消，正好。

### 纪律三：虚拟时间用来测超时

```kotlin
@Test
fun `ignored offer times out and cancels the sender`() = runTest {
    ...
    advanceTimeBy(30_001)
    ...
}
```

`OFFER_TIMEOUT_MS = 30_000`，测试**不需要等 30 秒**。这是 `runTest` + `delay` 组合的最大价值。

但注意 [08 篇](08-transfer.md) 提到的一个陷阱：`InMemoryTransport` 的 `Channel` 是 `UNLIMITED`，所以 `send` 不挂起——**背压相关的行为在这里测不到**，得靠 `TcpLoopbackTest` 那种真实链路，或者另写一个容量有限的假链路。

**虚拟时间有个适用前提：被测协程必须在测试的调度器上。**

补心跳判死时踩到了这条的反面。第一版测试这样写：

```kotlin
// 想用虚拟时间把 12 秒判死压缩成一次推进
val failure = async { server.receiveOne() }
advanceTimeBy(3_000L * 5 + 1)
```

结果 `advanceTimeBy` 只推进了 1 个 tick 就停住，心跳协程事实上没跑完 5 轮。

原因：`ProtocolImpl` 的心跳循环挂在**它自己的 `scope`**（这里是 `backgroundScope`）上，`advanceTimeBy` 推进的是**测试调度器**的时间轴，两者不是一回事。`backgroundScope` 的协程会随测试结束被取消，但**不会**因为测试体在等它而被自动推进。

最后改成"注入极短间隔 + 真时间"：

```kotlin
val server = ProtocolImpl(serverTransport, backgroundScope, heartbeatIntervalMs = 10L)

val failure = runCatching {
    withContext(Dispatchers.Default) {          // ← 明确离开虚拟时间
        withTimeout(5_000) { server.receiveOne() }
    }
}.exceptionOrNull()
```

60 毫秒跑完生产中 12 秒的逻辑。**这类"缩短常量"的做法比硬推虚拟时间可靠得多**——前提是被缩短的值是可以注入的。

这正是把 `heartbeatIntervalMs` 做成构造参数的真正理由：**能不能测是设计约束**。

## 没测到的地方（诚实清单）

| 没测 | 为什么 | 什么时候补 |
|---|---|---|
| `WifiP2pLink` 全部 | 依赖 `WifiP2pManager`，只能 instrumented + 真机 | 需要一套 mock 掉 framework 的骨架 |
| `ZevHubImpl.scheduleReconnect` 的重连退避 | 同上，依赖 P2P 组事件 + 真实 TCP | 抽出退避计算函数单测，或 mock `Link` |
| 心跳在真实网络上的判死时机 | 只在内存管道里验证过 | 真机拔网测试 |
| `TcpTransport` 背压 | 单测里没有"慢消费者"场景 | 写一个容量受限的 `InMemoryTransport` |
| `ZevHubImpl` 的组网流程 | 同上（依赖 P2P） | — |
| 多线程并发 `send` 到同一 Stream | 会死会话（[09 篇](09-stream.md)），但没写"验证它会死"的测试 | 值得写一条，文档化成错误用法 |
| 2 MiB+ 传输的**真实**内存峰值 | 只在 `ListDestination` 里攒内存 | 需要 `-Xmx` 限制下的压力测试 |

`WifiP2pLink` 是目前最大的测试黑洞（378 行，0 测试）。这是 [ROADMAP](../ROADMAP.md) 里最该补的一项。

## 一条可复用的经验

**把"平台调用"挤到最边缘，边缘之外全用参数注入。**

`LocalIdentity` 的 `persist` 参数、`DiscoveryRegistry` 的纯 `ConcurrentHashMap`、`ByteTransport` 的窄接口——三处不同的地方，同一个手法。结果是：

```
3.3k 行 SDK 里，只有 ~380 行（WifiP2pLink）真正需要真机
```

其余 **89% 的代码可以在 JVM 上、毫秒级、并行地测完**。

## 本篇自检

- 你的测试需要真机吗？如果需要，是"平台 API 硬依赖"还是"你没做参数注入"？
- 你的假链路是无限缓冲吗？那背压在哪测？
- 你测了几个异常路径？正常路径一个就够。
- 你测协议自发帧（ACK/ERROR/心跳）了吗？它们不上浮，只能从原始字节看。
- 你的超时测试真的等了吗？用 `runTest` 的话不用等。

---

下一篇：[11 · 发布 SDK](11-ship.md)
