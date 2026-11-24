# 06 · 一个 pump 管所有上下文

> 对应代码：`session/SessionImpl.kt`
> 对应测试：`SessionImplTest`（3 个）、`SessionRequestTest`（10 个）

这是全项目最大的一个类（433 行），但它干的事可以用一句话说完：

> **一个协程读所有帧，按 kind 和 id 分发给几十个并发上下文。**

## 为什么必须是单读协程

`Protocol.receive()` 的约定是单消费者（[05 篇](05-framing.md)）。所以 `SessionImpl` 的所有入站都从这一个 `pumpJob` 出来：

```kotlin
private val pumpJob = scope.launch {
    var decodingFrameId = 0L
    try {
        protocol.receive().collect { frame ->
            decodingFrameId = frame.frameId
            when (frame.kind) {
                ProtocolKind.MESSAGE -> { /* → incoming */ }
                ProtocolKind.REQUEST -> handleRequest(frame)
                ProtocolKind.RESPONSE -> { /* 按 correlationId 唤醒 */ }
                ProtocolKind.TRANSFER_START -> { /* 建上下文 + 抛给上层 */ }
                ProtocolKind.TRANSFER_DATA / COMPLETE / CANCEL -> { /* 按 contextId 路由 */ }
                ProtocolKind.STREAM_DATA -> { /* 按 contextId 路由 */ }
                ProtocolKind.STREAM_CLOSE -> { /* 结算 */ }
                else -> Unit
            }
        }
        // 正常结束 = 对端优雅关闭 = 会话死了
        ...
    } catch (e: Exception) {
        ...
    }
}
```

**这个 pump 永远不能阻塞。** 只要有一处 `send`（挂起版）或者一个慢消费者，整条会话的所有上下文一起停摆。

所以下面所有分发都是 `trySend`（非挂起、满了失败），而不是 `send`。这个规则贯穿全类，注释里反复出现：

```kotlin
val result = incoming.trySend(decoded)
if (result.isFailure) {
    // The message consumer is too slow; dropping
    // keeps the pump free for every other kind.
    Log.w(TAG, "dropping message ${frame.frameId}: queue full")
}
```

## 四种 id，各自的用途

| id | 谁分配 | 用途 | 谁配对 |
|---|---|---|---|
| `frameId` | `FrameIdGenerator`，每帧 | 唯一标识一帧，ACK 和 ERROR 里引用 | 无 |
| `correlationId` | 发起方 | Request ↔ Response 配对 | `pendingRequests` |
| `contextId` | **发起方** | Transfer / Stream 的路由键 | `transferContexts` / `streamContexts` |
| `sequence` | 发送方，per context | 数据帧顺序 | `ProtocolImpl.expectedSequence` |

关键区别：**`correlationId` 和 `contextId` 都是发起方分配的**，接收方只负责回填到响应里。这让"同一个 id 空间被两个方向共用"变得安全——因为发起方就是唯一的分配者。

`contextId` 从 `frameIds.next()` 拿（跟 `frameId` 共用计数器），所以它和 `frameId` 一样**全局唯一、只增不减**。这是 `closeContext` 必须及时调用的原因之一（[05 篇](05-framing.md)）。

## 请求：`CompletableDeferred` 表

```kotlin
/** In-flight requests keyed by correlation id. */
private val pendingRequests = ConcurrentHashMap<Long, CompletableDeferred<Response>>()

override suspend fun request(request: Request, timeoutMs: Long): Response {
    val encoded = RequestCodec.encode(request)
    // Payload length is a UInt32 and the wire spec caps it at 1 MiB;
    // failing here locally avoids poisoning the peer's session.
    require(encoded.size <= MAX_REQUEST_PAYLOAD) { "Request payload exceeds the 1 MiB wire limit" }

    val correlationId = frameIds.next()
    val deferred = CompletableDeferred<Response>()
    pendingRequests[correlationId] = deferred
    try {
        return withTimeout(timeoutMs) {
            protocol.send(ProtocolFrame(
                kind = ProtocolKind.REQUEST,
                frameId = frameIds.next(),
                correlationId = correlationId,
                payload = encoded
            ))
            deferred.await()
        }
    } finally {
        pendingRequests.remove(correlationId)
    }
}
```

三件事值得拆：

### ① 表在发之前就放好了

`pendingRequests[correlationId] = deferred` 在 `protocol.send` **之前**。如果反过来，一个极快的对端可能在你放进表之前就把 RESPONSE 发了，pump 找不到 `deferred`，响应被当成"孤儿"丢掉，然后你的 `await()` 挂到超时。这条顺序在所有异步协议里都一样。

### ② `finally` 里 remove，而不是正常路径

超时、取消、异常——所有路径都会走到 `finally`。写在 `finally` 里才真的不漏。对应测试：

```kotlin
@Test fun `request times out locally without signalling the protocol`()
@Test fun `cancelling a request stops the local wait and keeps the session usable`()
@Test fun `orphan response is dropped and the session stays usable`()
```

最后一个测的是：响应回到一个**已经不在表里**的 `correlationId`。实现里处理得很克制：

```kotlin
ProtocolKind.RESPONSE -> {
    val deferred = pendingRequests.remove(frame.correlationId)
    if (deferred != null) {
        deferred.complete(ResponseCodec.decode(frame.payload))
    } else {
        // A late response to a timed-out or cancelled
        // request; drop it, the session stays usable.
        Log.w(TAG, "dropping orphan response ${frame.correlationId}")
    }
}
```

**丢弃而不是报错。** 因为超时是"本地决定"，对方根本不知道，它迟到的响应完全合法。如果这里报协议错误，那么"请求超时"就会导致会话死亡——那超时这个功能就没法用了。

### ③ 超时不发任何信令

测试名字就在强调这点：`without signalling the protocol`。超时**只停本地等待**，对方那条请求还会继续处理、还会回一个响应（然后被丢弃）。`Session.kt` 的注释也是这个意思。

代价：对方可能白干一场。收益：超时不会让会话进入不一致状态。这是 P2P 场景的正确选择——对方可能走进电梯了，你既不能等它，也不能因为等不到就宣布协议违规。

## 大块数据：三条生命周期钩子

```kotlin
ProtocolKind.TRANSFER_START -> {
    val (metadata, totalBytes) = TransferCodec.decodeStart(frame.payload)
    val frames = Channel<TransferFrame>(capacity = 64)
    val contextId = frame.contextId
    transferContexts[contextId] = frames
    val offer = IncomingTransferImpl(
        metadata = metadata, totalBytes = totalBytes, contextId = contextId,
        scope = scope, protocol = protocol, frameIds = frameIds, frames = frames,
        onSettled = {
            transferContexts.remove(contextId)
            protocol.closeContext(contextId)
        }
    )
    offer.startOfferTimeout(OFFER_TIMEOUT_MS)
    val result = incomingTransfers.trySend(offer)
    if (result.isFailure) {
        // Nobody is collecting offers; rejecting frees
        // the peer instead of stalling it.
        Log.w(TAG, "dropping transfer offer $contextId: queue full")
        offer.reject()
    }
}
```

注意这里**先建上下文，再抛给上层**。顺序反了的话，上层 `accept()` 之后对方立刻发的 DATA 帧会找不到通道（`transferContexts[contextId]` 还没放进去）。

而 `startOfferTimeout` 解决的是"**上层忘了处理这个 offer**"：

```kotlin
// IncomingTransferImpl
fun startOfferTimeout(timeoutMs: Long) {
    timeoutJob = scope.launch {
        delay(timeoutMs)
        if (decided.compareAndSet(false, true)) {
            frames.close()
            onSettled()
            runCatching { protocol.send(/* TRANSFER_CANCEL */) }
        }
    }
}
```

没有这个超时，一个被忽略的 offer 会让对方的 `transfer.await()` **永远挂着**，同时这个 `contextId` 在 `expectedSequence` 里永远不删。对应测试 `ignored offer times out and cancels the sender` 用的就是虚拟时间（`advanceTimeBy`）。

## 流：协商复用请求通道

```kotlin
override suspend fun openStream(type: StreamType): Stream {
    val contextId = frameIds.next()
    val frames = Channel<ByteArray>(capacity = FRAME_BUFFER)
    val stream = StreamImpl(...)
    // Registered before the offer so DATA frames the peer sends
    // right after accepting cannot race the resume of this call.
    streamContexts[contextId] = frames
    peerClosedStreams[contextId] = stream
    val response = request(
        Request(type = RequestType(STREAM_OPEN_TYPE), data = StreamNegotiationCodec.encode(contextId, type))
    )
    if (response is Response.Failure) {
        streamContexts.remove(contextId)
        peerClosedStreams.remove(contextId)
        protocol.closeContext(contextId)
        throw StreamRejectedException(...)
    }
    return stream
}
```

这段注释是整个会话层最重要的注释之一：**注册必须在 offer 之前**。

因为 `request()` 是 `suspend` 的，它返回时对方可能**已经发了 STREAM_DATA**。如果注册在 `request()` 之后，这些帧到达 pump 时 `streamContexts[contextId]` 还是空的，会被当成"未知上下文"丢弃——**静默丢帧，且没有协议错误**（因为 `STREAM_DATA` 的序号校验在 `ProtocolImpl` 里通过了）。

这类 bug 的表现是"头几帧偶发丢失"，取决于调度。测不出来，只能靠注释和代码顺序。

接收侧对应地用了一个**保留的 request 类型**：

```kotlin
private fun handleRequest(frame: ProtocolFrame) {
    val decoded = RequestCodec.decode(frame.payload)
    if (decoded.type.value == STREAM_OPEN_TYPE) {
        // ... 建 IncomingStreamImpl，回 accept 或 reject 的 RESPONSE
        return
    }
    // 否则走普通请求
    val result = incomingRequests.trySend(IncomingRequestImpl(...))
}
```

```kotlin
/**
 * Request type reserved by the session layer for stream offers;
 * it never surfaces through [requests].
 */
const val STREAM_OPEN_TYPE = "__stream.open"
```

为什么用下划线+点号前缀？因为业务方的 request type 大概率是 `ping`、`upload` 这种词，`__` 开头是事实上的保留命名空间。测试 `reserved negotiation type never surfaces through requests` 就是保证它不会泄漏到用户的 `requests()` 流里。

**为什么不加一个 `STREAM_OPEN` kind？** 因为那需要新的握手能力协商（对端可能不支持）。复用 REQUEST/RESPONSE 意味着**流可以加在已有会话上，不需要重新握手**，也不需要新的协议版本。代价是多一层编码（`StreamNegotiationCodec` 里塞了 contextId + type）。

## 四种入站都用同一个模式

```kotlin
private val incoming = Channel<Message>(capacity = 64)
private val incomingRequests = Channel<IncomingRequest>(capacity = 64)
private val incomingTransfers = Channel<IncomingTransfer>(capacity = 64)
private val incomingStreams = Channel<IncomingStream>(capacity = 64)

override fun messages(): Flow<Message> = incoming.receiveAsFlow()
override fun requests(): Flow<IncomingRequest> = incomingRequests.receiveAsFlow()
override fun transfers(): Flow<IncomingTransfer> = incomingTransfers.receiveAsFlow()
override fun streams(): Flow<IncomingStream> = incomingStreams.receiveAsFlow()
```

四条独立的队列，容量都是 64。**为什么不共用一条？** 因为语义不同：一条慢的 `messages` 收集者不该让 transfer offer 排队。分开的代价是内存（4×64 个对象引用），收益是隔离。

为什么是 `receiveAsFlow` 不是 `consumeAsFlow`？因为业务方可能**分多个协程收集**（比如一个处理文字、一个处理图片），`receiveAsFlow` 允许多次收集。

## 死亡路径：三条，一个都不能漏

会话死亡有**三条**路径，做的事必须一样：

```kotlin
// 路径一：pump 的 collect 正常结束（对端优雅关闭）
// 路径二：catch 分支（帧/载荷错误、链路异常）
// 路径三：本地 close()
```

前两条在 pump 里用同一段收尾代码：

```kotlin
incoming.close(cause)
incomingRequests.close(cause)
incomingTransfers.close(cause)
incomingStreams.close(cause)
closeTransferContexts(cause)
closeStreamContexts()
failPendingRequests(cause)
```

七个动作，逐个都有理由：

| 动作 | 不做的话 |
|---|---|
| `incoming.close()` | `messages()` 永远不结束，收集者泄漏 |
| 四个 `close` | 同上，四种都是 |
| `closeTransferContexts` | 上下文的 frame channel 不关，`TransferImpl` 的 watcher 永远不退出 |
| `closeStreamContexts` | 同上 + `peerClosedStreams` 里的 `settleFromPeer` 不调，本地 `send()` 之后还能成功（假象） |
| `failPendingRequests` | `request()` 挂到超时，而不是立刻知道会话死了 |

**`failPendingRequests` 的意义是让"会话死了"立刻可见。** 否则一次 `request()` 要等 30 秒超时才发现对方早没了——而这个 30 秒里用户以为一切正常。

对应测试 `transport death fails pending requests instead of hanging`。

### 第三条路径最容易被忘掉

`close()` 里 `pumpJob.cancel()` 会让协程**直接终止**：既不执行 `collect` 之后的正常收尾，也不进入 `catch` 分支。所以本地关闭这条路径，pump 里那两段代码**一行都不会跑**。

最初的版本就漏了这里，后果是两个：

1. `failPendingRequests` 没调，`request()` 要等满 30 秒超时
2. 上层收不到任何"会话结束了"的通知

现在的写法是显式调用，并用 `terminated` 哨兵保证三条路径**只通知一次**：

```kotlin
override suspend fun close() {
    val alreadyTerminal = terminated.getAndSet(true)
    pumpJob.cancel()
    ... // 与上面两段相同的七个动作
    protocol.close()
    if (!alreadyTerminal) {
        onClosed(CancellationException("Session closed locally"))
    }
}
```

## `onClosed`：让上层能分辨"谁关的"

`SessionImpl` 有一个默认空实现的回调：

```kotlin
private val onClosed: (Throwable?) -> Unit = {}
```

语义是三态，**重连决策全靠它**：

| 值 | 含义 | 该重连吗 |
|---|---|---|
| `null` | 对端优雅关闭 | 视业务而定 |
| `CancellationException` | 本地 `close()` | **不该** |
| 其它异常 | 协议或链路故障 | **该** |

用 `CancellationException` 当"本地关闭"的标记，而不是加一个布尔参数，原因是它本来就是协程里"这是正常取消，不是错误"的既有约定。

### 为什么不用 `ConnectionState` 枚举

FastAir 那套实现有 `IDLE / CONNECTING / CONNECTED / RECONNECTING / CLOSED` 五态，因为它的 UI 要显示"重连中"。

ZevHub 没加，理由：上层已经能推导出来——

- `Session` 的 flow 完成/异常 → 断了
- `incomingSessions()` emit 一个新 `Session` → 重连成功

再加一条状态流，就多了一个**可能和实际情况不一致**的真相源。状态该由"事件"推导，而不是并行维护。

### 一个必须承认的测试缺口

`onClosed` 本身有测试（两条：链路失败、本地关闭），但**重连逻辑没有**。

`ZevHubImpl.scheduleReconnect` 依赖 `WifiP2pLink` 的组事件和真实 TCP，属于"只能真机验证"的部分（[10 篇](10-testing.md) 的诚实清单里有同类问题）。它的退避计算和"组消失则停止"目前是未验证代码。

## 本篇自检

- 你的分发器会不会阻塞？哪一行可能挂起？
- 你把"待响应表"的插入放在发送之前还是之后？
- 你的超时会不会发信令？发了的话，对端迟到的响应怎么处理？
- 上层忘了处理入站 offer 时，对方会怎样？有没有兜底超时？
- 你数过死亡清理要做几个动作吗？漏一个的症状是什么？
- 本地主动 `close()` 和远端死亡走的是同一条代码吗？如果不是，你怎么保证两边行为一致？
- 上层能分辨"我自己关的"和"链路自己断的"吗？不能的话重连会乱。

---

下一篇：[07 · Wi-Fi P2P 的坑](07-discovery.md)
