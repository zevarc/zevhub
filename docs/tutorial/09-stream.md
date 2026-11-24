# 09 · 连续流：序号校验与有界缓冲

> 对应代码：`stream/`（7 个文件）、`session/SessionImpl.kt` 的 `openStream` / `handleRequest`
> 对应测试：`StreamTest`（7 个）

## 流和传输，差在哪

| | Transfer（[08 篇](08-transfer.md)） | Stream（本篇） |
|---|---|---|
| 时长 | 有限，有终点 | 无限（通话、视频、传感器） |
| 总量 | 已知或未知，但会结束 | 永不结束 |
| 语义 | 一个"东西" | 一串"帧"，每帧独立有意义 |
| 结算 | `Success(bytes)` / `Cancelled` | 关闭（双方或一方） |
| 校验 | 序号（同 Stream） | 序号（同 Transfer） |
| 路由 | `contextId` | `contextId` |
| 缓冲 | 阻塞背压（保数据） | 有界 + 丢弃（保实时） |

最后一行是本质区别。文件少一块就是坏文件，必须阻塞；视频少一帧无所谓，卡住整个会话才致命。

## API：一个 send 对应一个 receive

```kotlin
// stream/Stream.kt
interface Stream {

    val type: StreamType

    suspend fun send(data: ByteArray)

    /**
     * 按到达顺序接收。流在任一端关闭或会话死亡时完成。
     */
    fun receive(): Flow<ByteArray>

    /** 两端都关闭。幂等。关闭后 send 抛异常。 */
    suspend fun close()
}
```

没有 `flush`、没有 `ack`、没有 `seek`。就这三件事：发一帧、收一串、关。

`send` 的注释写得很直白（也是本层最重要的坑）：

```kotlin
/**
 * Frames carry a per-stream sequence that the receiving side
 * validates: sends from concurrent coroutines on one stream may
 * interleave out of order and are treated as a fatal protocol
 * violation by the peer. Send from a single coroutine, or impose
 * your own ordering.
 */
```

## 序号：`Int` + 自增计数器，不是 CAS

```kotlin
// stream/StreamImpl.kt
/** Only the caller's send coroutines race here; order is theirs. */
private var nextSequence = 0

override suspend fun send(data: ByteArray) {
    if (settled.isCompleted) throw IllegalStateException("Stream is closed")
    val sequence = nextSequence++
    protocol.send(ProtocolFrame(
        kind = ProtocolKind.STREAM_DATA,
        frameId = frameIds.next(),
        contextId = contextId,
        sequence = sequence,
        payload = data
    ))
}
```

`nextSequence++` 是**非原子**的，注释也承认了："顺序由调用方负责"。

为什么敢这么写？

1. `++` 在拿到值之前就执行了，`sequence` 和 `protocol.send` 之间没有挂起点，所以**在单线程里不会交错**
2. 多协程并发 `send` 时，两个协程可能拿到**相同或乱序**的 sequence
3. 接收方 `ProtocolImpl` 会校验并**抛协议违规 → 整条会话死亡**

**这是一个"故意不做保护"的设计。** 替代方案是加 `Mutex` 或 `AtomicInteger`，但那会：

- 让每次 `send` 多一次挂起/原子操作
- 掩盖"调用方在并发发帧"这个逻辑错误

接收侧的校验（[05 篇](05-framing.md)）是**严格相等**，不是单调递增：

```kotlin
val expected = requireNotNull(expectedSequence.merge(frame.contextId, 1, Int::plus)) - 1
if (frame.sequence != expected) {
    throw WireException("Out-of-order sequence ...", CODE_PROTOCOL_VIOLATION)
}
```

所以"发现乱序就整条会话死"——不是丢那一帧，是死。理由：乱序意味着**要么实现有并发 bug，要么链路上的字节被破坏了**，两种都不能带着继续跑。

> 如果你要做音频/视频这类"允许丢帧"的场景，**不该用 Stream 的原始帧语义**，而应该在 Stream 之上再包一层（带自己的时间戳、丢帧策略）。SDK 不改校验规则。

## 协商：复用了 Request 通道，所以没有 STREAM_OPEN 帧

```kotlin
// session/SessionImpl.kt
override suspend fun openStream(type: StreamType): Stream {
    val contextId = frameIds.next()
    val frames = Channel<ByteArray>(capacity = FRAME_BUFFER)
    val stream = StreamImpl(...)
    // Registered before the offer so DATA frames the peer sends
    // right after accepting cannot race the resume of this call.
    streamContexts[contextId] = frames
    peerClosedStreams[contextId] = stream
    val response = request(
        Request(
            type = RequestType(STREAM_OPEN_TYPE),      // "__stream.open"
            data = StreamNegotiationCodec.encode(contextId, type)
        )
    )
    if (response is Response.Failure) {
        streamContexts.remove(contextId)
        peerClosedStreams.remove(contextId)
        protocol.closeContext(contextId)
        throw StreamRejectedException("Stream offer for ${type.value} refused: code=${response.errorCode}")
    }
    return stream
}
```

协商载荷（`StreamNegotiationCodec.kt`）：

```
[contextId i64][typeLen u16][type utf8]
```

**双方看到同一份字节**，所以发起方分配的 `contextId` 就是接收方用的那个。这个"发起方命名"的规则让实现简单很多——接收方不需要再发一个"我分配的 id"。

### 为什么不用 `STREAM_OPEN` 帧

三个理由：

1. **不用改握手**：新 kind 需要对端也知道，而 REQUEST/RESPONSE 已经在所有版本里了
2. **不用新能力协商**：`version: 1` 里没有能力位，加帧等于隐性破坏兼容
3. **天然的 accept/reject 语义**：REQUEST 已经有 `Response.Success` / `Response.Failure`，正好表达"接受/拒绝"

代价：

- **一次流的建立 = 一次完整的请求往返**（多 1 个帧）
- **`__stream.open` 是保留命名空间**，得靠约定而不是语法

第二点在 `handleRequest` 里处理得很直接：

```kotlin
private fun handleRequest(frame: ProtocolFrame) {
    val decoded = RequestCodec.decode(frame.payload)
    if (decoded.type.value == STREAM_OPEN_TYPE) {
        // ... 建 IncomingStreamImpl
        return    // ← 绝不进 incomingRequests
    }
    // 普通请求
    incomingRequests.trySend(IncomingRequestImpl(...))
}
```

测试 `reserved negotiation type never surfaces through requests` 保证这条不泄漏。

## 有界缓冲：满了丢帧

```kotlin
// session/SessionImpl.kt
/** Frames buffered per stream before the pump starts dropping. */
const val FRAME_BUFFER = 64

private val streamContexts = ConcurrentHashMap<Long, Channel<ByteArray>>()
```

```kotlin
ProtocolKind.STREAM_DATA -> {
    val channel = streamContexts[frame.contextId]
    if (channel != null) {
        val result = channel.trySend(frame.payload)
        if (result.isFailure) {
            // The stream consumer is too slow; the
            // receive buffer is bounded, so the frame
            // is dropped to protect the session.
            Log.w(TAG, "dropping stream frame ${frame.sequence} on context ${frame.contextId}: queue full")
        }
    }
    // Frames for unknown or already-closed contexts belong to a
    // closed stream; drop them.
}
```

三个决定：

**① `trySend` 而不是 `send`。** pump 是全会话单线程（[06 篇](06-session.md)），阻塞 pump = 所有上下文停摆。

**② 丢新帧，不丢老帧。** `trySend` 满了就是丢弃**最新的**。对实时流这是对的：老帧可能是关键帧，新帧丢掉只影响一小段；反之如果丢老帧，缓冲区会一直保留最旧的、延迟越来越大的内容。

**③ 丢帧不报协议错误。** 只是 `Log.w`。原因：这是**应用消费太慢**，不是对端违规。而 `ProtocolImpl` 的序号校验在这一步**已经通过了**——所以丢弃一个已校验的帧，会出现"应用收到的序号有空洞"。

这是个真实的取舍：**校验严格 + 允许丢弃 = 应用看到不连续的序号**。如果上层需要用序号做丢帧检测，`receive()` 现在只给 `ByteArray`，看不到序号。这是将来要补的（要么给 `receive()` 返回带序号的类型，要么在丢弃时显式通知）。

`incomingStreams` 的容量同样是 64，那是**待处理的流 offer 队列**，不是数据帧。

## 关闭：两端对称，但路径不同

### 本地关闭

```kotlin
override suspend fun close() {
    if (!settled.complete(Unit)) return
    runCatching {
        protocol.send(ProtocolFrame(
            kind = ProtocolKind.STREAM_CLOSE,
            frameId = frameIds.next(),
            contextId = contextId
        ))
    }
    frames.close()
    onClosed()
}
```

`settled` 是 `CompletableDeferred<Unit>`，`complete(Unit)` 返回 false 表示已经结算过 → 幂等。

**`runCatching` 包住 `STREAM_CLOSE` 是必要的**：会话可能已经死了，这时 `close()` 不该抛异常（`Stream.close()` 的约定）。

### 对端关闭

```kotlin
// 会话 pump 里
ProtocolKind.STREAM_CLOSE -> {
    val channel = streamContexts.remove(frame.contextId)
    if (channel != null) {
        channel.close()
        peerClosedStreams.remove(frame.contextId)?.settleFromPeer()
        protocol.closeContext(frame.contextId)
    }
    // A close for an unknown context arrives after a local close;
    // the context is already settled.
}
```

```kotlin
// StreamImpl
/**
 * Ends the stream because the peer closed it or the session died.
 * Called by the session pump on STREAM_CLOSE or teardown; local
 * sends afterwards fail with "Stream is closed".
 */
fun settleFromPeer() {
    if (!settled.complete(Unit)) return
    frames.close()
    onClosed()
}
```

### 为什么需要 `peerClosedStreams` 这张表

因为**本地持有的 `StreamImpl` 没人通知**。`channel.close()` 只让 `receive()` 的收集者结束，但 `StreamImpl.send()` 检查的是**自己的** `settled`。不调 `settleFromPeer()` 的话：

```kotlin
stream.send(x)   // 对端已经关了，本地还成功"发出"，然后帧被 pump 丢弃
```

也就是**假的成功**。测试专门覆盖：

```kotlin
@Test
fun `peer close completes the receive flow and blocks further sends`() {
    ...
    peerStream.close()
    // 之前发的帧仍然会被收到
    assertEquals(byteArrayOf(9).toList(), peerStream.receive().first().toList())
    val failure = runCatching { stream.send(byteArrayOf(10)) }
    assertTrue(failure.isFailure)
}
```

注意这个测试的第一句断言：**`STREAM_CLOSE` 之前发出的帧仍然会被交付**。这不是巧合——`STREAM_CLOSE` 是普通帧，走同一条有序通道，所以它之前的所有帧都已经在 `Channel` 里了，`close()` 之后已有的元素仍可消费（Kotlin `Channel` 的 `close()` 语义：缓冲不丢，新元素拒绝）。

**这正是用 `Channel` 而不是 `SharedFlow` 的理由。** `SharedFlow` 在关闭时缓冲内容可能被丢。

### 会话死亡时

```kotlin
private fun closeStreamContexts() {
    streamContexts.values.forEach { it.close() }
    streamContexts.clear()
    peerClosedStreams.values.forEach { it.settleFromPeer() }
    peerClosedStreams.clear()
}
```

两个都要做：`channel.close()` 让 `receive()` 结束，`settleFromPeer()` 让 `send()` 抛异常。

对应测试 `session death fails stream sends`。

## `receive()` 只能收集一次

```kotlin
override fun receive(): Flow<ByteArray> = frames.receiveAsFlow()
```

`receiveAsFlow` 允许多次收集，但 `Channel` 是**广播的**——同一帧只会给一个收集者。所以第二次收集会"抢走"第一半数据。

这跟 `Session.messages()` 的行为一致（[06 篇](06-session.md)），但值得文档化：**流的每一帧只能被消费一次**，想要多处消费请自己在应用层 `share`。

## 没做的事

| 缺口 | 后果 |
|---|---|
| 接收侧丢弃策略不可配置 | 想"阻塞而不是丢"就得改 SDK |
| `receive()` 不暴露序号 | 上层无法检测丢帧 |
| 无优先级/多路复用 | 视频和传感器流公平竞争带宽 |
| 无取消语义（只有整条关闭） | 想"暂停"只能关掉重开 |
| `nextSequence` 不可原子 | 并发 `send` = 会话死亡 |

## 本篇自检

- 你的数据帧有顺序保证吗？靠什么校验？
- 消费慢了怎么办？阻塞、丢弃、还是降速？三种的后果分别是什么？
- 对端关闭后，你的 `send()` 立刻失败吗？还是会静默成功？
- 关闭通知会不会抢在前面已发出的数据帧之前到达？（本项目的答案是：不会，因为它们同序）
- 你允许上层并发发送吗？不允许的话，违反后会怎样？

---

下一篇：[10 · 不用真机测 P2P](10-testing.md)
