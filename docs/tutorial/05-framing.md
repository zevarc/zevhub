# 05 · 组帧层：ACK、序号、上下文的取舍

> 对应代码：`protocol/ProtocolImpl.kt`、`protocol/ProtocolKind.kt`、`protocol/ProtocolFrame.kt`、`protocol/FrameIdGenerator.kt`
> 对应测试：`ProtocolImplTest`（5 个）

## 这一层只做四件事

```kotlin
// protocol/Protocol.kt
internal interface Protocol {

    suspend fun send(frame: ProtocolFrame)
    suspend fun receiveOne(): ProtocolFrame          // 握手用：严格顺序
    fun receive(): Flow<ProtocolFrame>               // 会话用：持续流
    suspend fun close()

    suspend fun signalProtocolError(offendingFrameId: Long, cause: Exception)
    fun closeContext(contextId: Long)
}
```

1. 把 `ProtocolFrame` 编码成字节、走 `ByteTransport`
2. 把字节流解回 `ProtocolFrame`，并做**跨帧**校验（`WireDecoder` 只做单帧校验）
3. 对 `MESSAGE` 自动回 ACK
4. 出错时告诉对端，然后整条通道关掉

**它不懂 Message 的内容，不懂 Request 的语义。** 它只知道 kind 的字节值和上下文字段怎么用。

## kind 枚举是唯一真相

```kotlin
// protocol/ProtocolKind.kt
enum class ProtocolKind(val value: Byte) {
    HELLO(0x01), AUTH(0x02), READY(0x03),

    MESSAGE(0x10),

    REQUEST(0x20), RESPONSE(0x21),

    TRANSFER_START(0x30), TRANSFER_DATA(0x31),
    TRANSFER_COMPLETE(0x32), TRANSFER_CANCEL(0x33),

    STREAM_DATA(0x40), STREAM_CLOSE(0x41),

    ACK(0x70),
    ERROR(0x71),
    HEARTBEAT(0x72);

    companion object {
        fun from(value: Byte): ProtocolKind =
            entries.firstOrNull { it.value == value }
                ?: error("Unsupported protocol kind: $value")
    }
}
```

**十六进制分组就是语义分组**：`0x0X` 是握手、`0x1X` 是文字、`0x2X` 是请求、`0x3X` 是批量、`0x4X` 是流、`0x7X` 是协议自身。这个编号方式在调试时价值巨大——抓包看到 `0x31` 立刻知道是传输数据，不用查表。

注意 `STREAM_DATA(0x40)` **没有配套的 `STREAM_OPEN`**。这是 v1 的刻意省略：流的建立复用 REQUEST/RESPONSE 通道（[09 篇](09-stream.md) 讲为什么），所以 kind 表里只有数据传输和关闭两种。

`from()` 用 `error()` 而不是返回 null：**未知 kind 是致命错误**，不是可忽略项。`WireDecoder` 已经先校验过一次，这里任何 `error()` 都说明有代码漏了校验。

## 两个接收入口，为什么

```kotlin
suspend fun receiveOne(): ProtocolFrame = incoming.receive()
fun receive(): Flow<ProtocolFrame> = incoming.consumeAsFlow()
```

同一个 `Channel`，两种消费方式，**不能混用**：

- `receiveOne()`：握手阶段，严格一问一答（`Handshake.run` 就是 `receiveOne` 六次）
- `receive()`：握手之后，交给 `SessionImpl` 的 pump 协程持续消费

`consumeAsFlow()` 而不是 `receiveAsFlow()`：会话层的 pump **就是唯一消费者**，`consumeAsFlow` 保证流结束后 channel 被消费掉，语义更准确（`receiveAsFlow` 允许多次收集）。

`Protocol.kt` 的注释明确写了这条不能违反的约定：

```kotlin
/**
 * Suspend until the next frame arrives. Consumers must be strictly
 * sequential: handshake first, then the session's receive loop.
 */
suspend fun receiveOne(): ProtocolFrame
```

违反会发生什么：握手还没做完，`SessionImpl` 的 pump 已经把 HELLO/AUTH 帧抢走了，握手 `require()` 失败 → 会话建立失败。表现为"偶发握手失败"。

## ACK：自动、无状态、无人消费

```kotlin
private val decodeJob = scope.launch {
    try {
        transport.receive().collect { bytes ->
            for (frame in decoder.feed(bytes)) {
                validate(frame)
                when (frame.kind) {
                    ProtocolKind.MESSAGE.value -> {
                        sendAck(frame.frameId)          // ← 收到就回
                        incoming.send(toProtocolFrame(frame))
                    }
                    // ACK and HEARTBEAT are protocol-internal; the
                    // session layer never sees them.
                    ProtocolKind.ACK.value,
                    ProtocolKind.HEARTBEAT.value -> Unit
                    else -> incoming.send(toProtocolFrame(frame))
                }
            }
        }
        incoming.close()
    } catch (e: Exception) {
        protocolError(0L, e)
    }
}
```

三个决定：

**① 只有 `MESSAGE` 回 ACK。** Transfer 和 Stream 的 DATA 帧不回。为什么？因为那两种有 per-context 序号校验，丢帧会被识别成致命错误（见下）；而 MESSAGE 是"发了就完"，没有序号，所以需要一个不依赖顺序的确认。

**② ACK 和 HEARTBEAT 不进上层。** `SessionImpl` 永远看不到它们，`incoming.send` 被跳过。两者都是协议内部事务。

**两者分工不同，不要混：**

| | ACK | HEARTBEAT |
|---|---|---|
| 方向 | 收到帧的人发 | 主动周期性发 |
| 针对 | 单帧确认 | 对端存活 |
| 载荷 | 被确认的 `frameId`（8 字节） | 空 |
| 消费者 | 暂无 | 协议层自身的判死循环 |

对 ACK 与心跳的关系有个常见的误解：**心跳不能当作可靠传输的替代**。心跳只能告诉你“对端进程还在”，不能告诉你“某一帧到达了”。

**③ ACK 的载荷是被确认的 `frameId`（8 字节大端）**：

```kotlin
private fun encodeU64(value: Long): ByteArray {
    val bytes = ByteArray(8)
    for (i in 0 until 8) {
        bytes[i] = (value shr ((7 - i) * 8)).toByte()
    }
    return bytes
}
```

测试直接把它钉死：

```kotlin
assertArrayEquals(byteArrayOf(0, 0, 0, 0, 0, 0, 0, 100), ack[0].payload)
```

**但是 ACK 现在没有任何消费者。** 没有任何地方等待 ACK、没有重传窗口、没有超时。也就是说：**如果一帧 MESSAGE 在 TCP 之上丢了，本协议不知道。**

TCP 不会丢（它自己会重传），所以"在 TCP 之上不做重传"是站得住的。但这条**必须写进文档**，否则将来换一个不可靠链路（UDP、局域网广播）时，所有人都会以为有重传。见 [ROADMAP](../ROADMAP.md) 阶段 2。

## 跨帧校验：per-context 序号

`WireDecoder` 只能看到单个帧，所以它检查不了"这个序号对不对"。这需要记住**每个上下文期待的下一个序号**：

```kotlin
/** Next expected sequence per Transfer/Stream context. */
private val expectedSequence = ConcurrentHashMap<Long, Int>()

private suspend fun validate(frame: WireFrame) {
    if (frame.kind == ProtocolKind.ERROR.value) {
        throw PeerErrorException()
    }
    if (frame.kind == ProtocolKind.TRANSFER_DATA.value ||
        frame.kind == ProtocolKind.STREAM_DATA.value
    ) {
        // Sequence must advance monotonically within the context.
        val expected = requireNotNull(expectedSequence.merge(frame.contextId, 1, Int::plus)) - 1
        if (frame.sequence != expected) {
            expectedSequence.remove(frame.contextId)
            throw WireException(
                "Out-of-order sequence ${frame.sequence} on context ${frame.contextId}" +
                    " (expected $expected)",
                WireException.CODE_PROTOCOL_VIOLATION
            )
        }
    }
}
```

`merge(contextId, 1, Int::plus)` 是关键写法：**原子的"取值或插入并加一"**，返回值是**加之前**的值，减一就是期望值。第一次遇到某上下文时期望 0，第二次期望 1……

为什么不用 `get` + `put`？因为 `validate` 在**单读协程**里跑，看似不会有并发问题。但 `closeContext` 会被**其他协程**调（`SessionImpl` 的 pump 在 transfer 结束时会调），`ConcurrentHashMap` + `merge` 保证这两条路径不打架。

为什么序号只校验 Transfer/Stream？因为**这两类是一次有序的连续数据**，乱序意味着数据损坏。MESSAGE/REQUEST 是独立的，不需要序号（REQUEST 靠 correlation id 配对）。

对应的失败测试：

```kotlin
@Test
fun `out of order transfer data fails the protocol with a violation code`() {
    // kind = 0x31 TRANSFER_DATA, contextId = 5, sequence 从 1 开始（期望 0）
    ...
}
```

## 心跳：判死的是"沉默"，不是"错误"

TCP 有一个很不符合直觉的行为：**对端设备走出 Wi-Fi 范围时，连接可能几分钟内都不断开**。这期间所有 `send()` 都成功（数据进了本地 socket 缓冲），没有任何异常。

对一个靠"连接还在"推断"对方还在"的上层，这是灾难：用户拔腿走了，你的文件传输还在慢慢"成功"。

所以协议层要自己造一个存活性判据：

```kotlin
// ProtocolImpl
@Volatile
private var silentProbes: Int = 0

private val heartbeatJob = scope.launch {
    while (true) {
        delay(heartbeatIntervalMs)
        if (silentProbes >= MAX_SILENT_PROBES) {
            protocolError(0L, WireException("Peer silent for ..."))
            break
        }
        silentProbes++
        runCatching {
            send(ProtocolFrame(kind = ProtocolKind.HEARTBEAT, frameId = frameIds.next()))
        }
    }
}
```

三个设计点：

**① 用"探针计数"而不是"墙钟时长"。**

第一版写的是 `System.currentTimeMillis() - lastReadAt > 10_000`。这有个真实后果：**它无法被测试**。`runTest` 推进的是虚拟时钟，墙钟几乎不动，判死永远不触发。

改成计数后，整个逻辑只依赖协程 `delay`，可以被调度器驱动。同时把间隔做成可注入参数：

```kotlin
internal class ProtocolImpl(
    private val transport: ByteTransport,
    private val scope: CoroutineScope,
    private val frameIds: FrameIdGenerator = FrameIdGenerator(),
    private val heartbeatIntervalMs: Long = HEARTBEAT_INTERVAL_MS
)
```

生产值 `3_000L`，测试传 `10L` 就能把 12 秒的窗口压缩到 60 毫秒。**"能不能测"是设计约束，不是测试技巧。**

**② 收到任何字节都清零计数**，不只是收到心跳：

```kotlin
for (frame in decoder.feed(bytes)) {
    silentProbes = 0
    validate(frame)
    ...
}
```

因为判的是"对端是否还在说话"，而不是"对端是否在回我的心跳"。一个正在狂发视频帧的对端当然活着。

**③ 心跳不进上层。** 和 ACK 一样在 `when` 里被吞掉：

```kotlin
ProtocolKind.ACK.value,
ProtocolKind.HEARTBEAT.value -> Unit
```

否则每个设备每 3 秒就会在应用的 `messages()` 流里看到一个空帧。

### 为什么不用 ACK 兼当心跳

"给对方发一帧、等它 ACK"看起来更省。但：

- ACK 只对 `MESSAGE` 生效，而心跳是**协议层**的需求，不能依赖上层有没有发消息
- 把判死绑在"上层最近有没有发文字"上，等于业务空闲时连接被误判死亡

**判活必须独立于业务。** 这就是为什么 `HEARTBEAT` 是独立的 kind，而不是复用 `MESSAGE`。

### 判死之后做什么

走的是和"帧格式错误"完全相同的路径：`protocolError()` → 给对端发 ERROR（可能已发不出去）→ 关闭 `incoming` → 关闭 transport。

于是 **`Session` 的四个 flow 全部完成/失败，`pendingRequests` 全部失败，挂起的 `send` 抛异常**。上层不需要一个单独的"心跳超时"回调——它看到的就和链路断开一模一样。

这是刻意的：**上层不该有两套断线处理逻辑。**

## 致命错误的自我了断

```kotlin
private suspend fun protocolError(offendingFrameId: Long, cause: Exception) {
    // Echoing a peer's ERROR back would be a loop; only announce errors
    // that originated locally.
    val peerSignalled = cause is PeerErrorException
    if (!peerSignalled) {
        val errorCode = (cause as? WireException)?.code ?: WireException.CODE_INTERNAL_ERROR
        runCatching {
            send(
                ProtocolFrame(
                    kind = ProtocolKind.ERROR,
                    frameId = frameIds.next(),
                    payload = errorPayload(errorCode, offendingFrameId)
                )
            )
        }
    }
    incoming.close(cause)
    runCatching { transport.close() }
}
```

**ERROR 载荷 = 8 字节有问题的 frameId + 2 字节错误码**（大端），共 10 字节：

```
[ offendingFrameId : u64 ][ errorCode : u16 ]
```

三个决定：

**① 不说错误循环。** 收到对端的 ERROR 时，**不再回一个 ERROR**。否则两边互发 ERROR 直到天荒地老。`PeerErrorException` 就是这个标记，对应测试：

```kotlin
@Test
fun `protocol error frames from the peer are fatal and not echoed`() {
    // ... 收到 ERROR 帧
    assertTrue(failure is PeerErrorException)
    // No ERROR is echoed back to the peer.
    delay(100)
    assertTrue(serverTransport.sentChunks.isEmpty())
}
```

**② ERROR 发送用 `runCatching` 包起来。** 通道已经坏了，发 ERROR 本身可能也失败，不能因为这个把关闭流程卡住。

**③ 关 channel 时带 cause。** `incoming.close(cause)` 让 `SessionImpl` 的 pump 的 `collect` 抛出**具体原因**，而不是一个空的 `ChannelClosedException`。这让日志能区分"对端发了坏帧"和"链路断了"。

`offendingFrameId` 的作用：告诉对端"我拒的是哪一帧"。它从哪来？

```kotlin
// SessionImpl 的 pump
var decodingFrameId = 0L
try {
    protocol.receive().collect { frame ->
        decodingFrameId = frame.frameId
        when (frame.kind) { ... }
    }
} catch (e: Exception) {
    // The frame was valid on the wire but its payload broke the codec:
    // report it to the peer before dying (spec §13).
    if (e is IllegalArgumentException) {
        runCatching { protocol.signalProtocolError(decodingFrameId, e) }
    }
    ...
}
```

这是**两层错误的区别**，很重要：

| 出错层 | 例子 | 谁报错 |
|---|---|---|
| 帧层（`WireException`） | magic 错、长度超限、序号乱 | `ProtocolImpl` 自己，`offendingFrameId` 是 0（还没解析出） |
| 载荷层（`IllegalArgumentException`） | Message 的 type 长度非法、Response 状态字节不认识 | `SessionImpl` 报，因为它知道是哪一帧 |

载荷层的错误为什么也能叫"协议错误"？因为它意味着**对端在用同一个格式但算出了不同的字节**，这是不兼容，不是业务失败。所以整条会话死掉是对的。

## 上下文的生命周期

```kotlin
override fun closeContext(contextId: Long) {
    expectedSequence.remove(contextId)
}
```

为什么必须有这个？如果不删：

1. `expectedSequence` 无限增长（每条 transfer/stream 一条），长时间运行必然内存泄漏
2. 更严重：context id 由 `FrameIdGenerator` 分配，**只增不复用**，所以泄漏量就是"运行期间开过的流总数"

`SessionImpl` 在每条上下文结算时都调用它（`onSettled` 回调里）：

```kotlin
onSettled = {
    transferContexts.remove(contextId)
    protocol.closeContext(contextId)
}
```

## 三个"必须成对"的地方

这一层最容易出的 bug 都是"成对的东西只做了一半"：

| 成对 | 漏了一半的后果 |
|---|---|
| 建上下文 ↔ `closeContext` | 序号表泄漏 + 迟到的帧被当成协议违规 |
| `try` 里 `incoming.send` ↔ `catch` 里 `incoming.close(cause)` | pump 永远挂着，会话"假死" |
| `incoming.close` ↔ `transport.close` | 上层以为死了下层还开着，socket 泄漏 |

第二条尤其隐蔽：`decodeJob` 的 `try` 块里，`collect` 正常结束时走 `incoming.close()`（无异常 = 对端优雅关闭）；抛异常时走 `catch` 的 `protocolError`。**如果 `catch` 里忘了关 `incoming`，`receive()` 的收集者永远等不到结束。**

## 本篇自检

- 你的协议层知不知道业务语义？知道了说明分层错了。
- 你的 ACK 有消费者吗？没有的话它是不是装饰？
- 序号校验需要跨帧状态吗？需要的话这个状态谁删？
- 你的错误上报会不会形成循环？怎么打断？
- 你区分的了"帧坏了"和"载荷坏了"吗？两者上报的时机不同。
- **对端拔腿走了，你的协议多久能发现？** 如果是“等 TCP 自己断”，答案可能是几分钟。
- **你的判死逻辑能被单测驱动吗？** 依赖墙钟就不行。
- 心跳和业务帧是同一个队列吗？心跳被拥塞卡住就失去意义了。

---

下一篇：[06 · 一个 pump 管所有上下文](06-session.md)
