# 08 · 大块数据：分块、背压、单边取消

> 对应代码：`transfer/`（11 个文件）、`session/SessionImpl.kt` 的上下文路由
> 对应测试：`TransferTest`（11 个）、`TransferCodecTest`（5 个）

## 一帧无法表达一个大文件

1 MiB 是帧上限（[02 篇](02-wire-format.md)）。一张手机照片 3~8 MiB，一个视频几百 MiB。所以必须切块，而切块立即带来四个新问题：

1. 怎么宣告"我要传 3 MiB，类型是 image/jpeg"？
2. 怎么让接收方**边收边写**，而不是先攒满 3 MiB？
3. 对方跟不上时，发送方怎么**慢下来**（背压）？
4. 中途一方不想传了，怎么干净地停下来？

对应四组接口，一一对上：

| 问题 | 接口 |
|---|---|
| 元信息 | `TransferMetadata` + `TRANSFER_START` |
| 边收边写 | `TransferDestination.write()` |
| 背压 | `TransferSource.read(maxBytes)` 是 `suspend` |
| 取消 | `TransferResult.Cancelled` + `TRANSFER_CANCEL` |

## 面向生产者的 API：不是 `send(ByteArray)`

```kotlin
// transfer/TransferSource.kt
interface TransferSource {
    val metadata: TransferMetadata
    /** 总字节数，未知时 null。只用于进度上报，不会被校验。 */
    val totalBytes: Long?
    /**
     * 返回下一块数据（最多 maxBytes），返回 null 表示耗尽。
     * 返回的数组会直接交给协议，之后不可修改。
     */
    suspend fun read(maxBytes: Int): ByteArray?
}
```

```kotlin
// transfer/TransferDestination.kt
interface TransferDestination {
    /** 按顺序依次调用。 */
    suspend fun write(data: ByteArray)
    /** 最后一块之后恰好调一次，无论成功、取消、失败。必须幂等。 */
    suspend fun close()
}
```

两个设计点：

**`read()` 是 `suspend`**，这是背压的唯一来源。发送方调用它时如果对方 socket 写不出去，最终会挂在这里。所以 **"不用把整个文件读进内存"是免费的**——只要你的 `read` 自己也是流式的（`InputStream`、`Cursor`、相机帧）。

**`close()` 保证恰好一次且必须幂等**。这是唯一让"接收方不用判断结果"的写法：无论 `Success`、`Cancelled`、还是异常，`close()` 都在，所以把 `flush`/`rename`/`commit` 放这里就对了。测试专门检查：

```kotlin
assertTrue(destination.closed)
```

## 帧序列

```
TRANSFER_START   (metadata + 声明总长)
TRANSFER_DATA ×N (每块带 sequence 0,1,2...)
TRANSFER_COMPLETE
```

或者中途：

```
TRANSFER_START
TRANSFER_DATA ×k
TRANSFER_CANCEL  (任何一端)
```

`TRANSFER_START` 的载荷编码（`TransferCodec.kt`）：

```
[typeLen u16][type utf8][totalBytes i64][metaLen u32][meta bytes]
```

`totalBytes` 用 **-1 表示未知**：

```kotlin
// The sender communicates an unknown total with -1 on the wire.
val declaredTotal = totalBytes ?: -1L
...
declaredTotal.takeIf { it >= 0 }    // 解码
```

**为什么不用 0 表示未知？** 因为 0 是合法的长度（空文件）。协议里"缺失"和"合法值"必须分开——这和 [02 篇](02-wire-format.md) 里 `frameId` 从 1 开始是同一个教训，只是这里用了 -1 而不是占位 0。

## 背压到底在哪生效

`TransferImpl.streamData()`：

```kotlin
private suspend fun streamData() {
    while (true) {
        if (cancelRequested.isCompleted) return
        val chunk = source.read(chunkSize) ?: return
        protocol.send(ProtocolFrame(
            kind = ProtocolKind.TRANSFER_DATA,
            frameId = frameIds.next(),
            contextId = contextId,
            sequence = nextSequence++,
            payload = chunk
        ))
        progressFlow.value = TransferProgress(
            bytesTransferred = progressFlow.value.bytesTransferred + chunk.size,
            totalBytes = source.totalBytes
        )
    }
}
```

`protocol.send` 一路挂到 `TcpTransport.send` → `writeChannel.send(data)`。**`writeChannel` 容量 16**（[04 篇](04-transport.md)）。

所以整条链上的内存上限是：

```
16 帧 × 256 KiB = 4 MiB    （writeChannel 积压）
+ 1 帧在 writeLoop 里写
+ 1 块在 source 手里
```

而不是"文件大小"。这就是为什么 `zetvhub/build.gradle.kts` 里有一句看似无关的配置：

```kotlin
// Transfers up to 3 MiB are held in memory by the unit test fixtures;
// the default 512m heap is not enough.
tasks.withType<Test>().configureEach {
    maxHeapSize = "2g"
}
```

注意是**测试夹具**（`ListDestination` 把所有块攒在内存里）占内存，不是实现。

`DEFAULT_CHUNK_SIZE = 256 * 1024` 是权衡：越小则帧数多、每帧 36 字节头开销占比高（256 KiB 时是 0.014%，很小）；越大则单帧延迟高、内存峰值大。256 KiB 让 1 MiB 上限留了 4 倍余量（`TRANSFER_START` 的元信息也算在帧里）。

## 取消：三端一致，且要能打断阻塞

取消是最容易做错的部分。`TransferImpl` 里同时存在**三个可能让流程卡住的点**：

1. 发送方卡在 `source.read()`（在读一个永远不返回的 `InputStream`）
2. 发送方卡在 `protocol.send()`（对方不读）
3. 发送方卡在 `await()`（等结果）

看它的解法——**一个 watcher 协程专门盯上下文 channel**：

```kotlin
init {
    // Settles the outcome when the peer or the session ends the
    // transfer out of band of the sender coroutine.
    scope.launch {
        while (true) {
            val frame = frames.receiveCatching().getOrNull()
            if (frame == null) {
                // 通道关闭：要么本地取消（自己结算），要么会话死了
                if (!outcome.isCompleted && !cancelRequested.isCompleted) {
                    senderJob.cancel()
                    markSettled()
                    outcome.completeExceptionally(
                        IllegalStateException("Session closed during transfer")
                    )
                }
                return@launch
            }
            if (frame.kind == ProtocolKind.TRANSFER_CANCEL) {
                cancelRequested.complete(Unit)
                frames.close()
                senderJob.cancel()
                markSettled()
                outcome.complete(TransferResult.Cancelled)
                return@launch
            }
        }
    }
}
```

`senderJob.cancel()` 是打断"卡在 `source.read()`"的唯一手段。**注意 `read()` 必须是可取消的挂起调用**——如果你的 `read` 里做了阻塞 IO 又没处理取消，`cancel()` 之后 `senderJob` 会继续跑到读完整个文件。这是实现 `TransferSource` 的**硬性要求**，文档里应该写（目前没写，列在 [ROADMAP](../ROADMAP.md)）。

本地取消：

```kotlin
override fun cancel() {
    if (!cancelRequested.complete(Unit)) return
    frames.close()
    senderJob.cancel()
    scope.launch { announceCancelToPeer() }
    markSettled()
    outcome.complete(TransferResult.Cancelled)
}
```

注意 `cancel()` **不是 `suspend`**（`Transfer.cancel()` 签名如此），所以通知对端只能 `scope.launch` 出去。这意味着**取消通知可能丢失**（进程被杀的极端情况），但对端最终会通过会话死亡感知。

`markSettled` 用 `AtomicBoolean` 保证**恰好一次**：

```kotlin
private fun markSettled() {
    if (settled.compareAndSet(false, true)) {
        onSettled()      // ← 由 SessionImpl 提供：移除上下文 + protocol.closeContext
    }
}
```

## 幂等性：三个 `AtomicBoolean`

`TransferImpl` 里有两个守卫（`settled`、`cancelRequested`），`IncomingTransferImpl` 里有一个（`decided`）。**这不是过度设计**，因为取消/结算可以被以下任意一方触发：

- 本地 `cancel()`
- 对端 `TRANSFER_CANCEL`
- 会话死亡（通道关闭）
- offer 超时
- 发送完成

**五条路径，谁先到谁结算。** 用 `AtomicBoolean` 而不是 `Mutex`：结算动作很短（几次 `complete` 调用），不需要挂起。

## 接收侧：offer 的独立生命周期

```kotlin
override fun accept(destination: TransferDestination): Transfer {
    check(decided.compareAndSet(false, true)) { "Transfer already accepted or rejected" }
    timeoutJob?.cancel()
    return IncomingTransferJob(...)
}

override fun reject() {
    check(decided.compareAndSet(false, true)) { "Transfer already accepted or rejected" }
    timeoutJob?.cancel()
    scope.launch {
        runCatching { protocol.send(/* TRANSFER_CANCEL */) }
        onSettled()
    }
}
```

`reject()` 要专门发 `TRANSFER_CANCEL`，因为对端可能在 `await()`：不发的话对方要等到会话死亡才知道。

而 `startOfferTimeout`（[06 篇](06-session.md) 提过）兜住"上层忘了处理"：

```kotlin
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

对应测试 `ignored offer times out and cancels the sender`（`OFFER_TIMEOUT_MS = 30_000`，测试里用虚拟时间）。

**注意 `accept()` 和 `reject()` 都是同步的（非 `suspend`）**——所以 `reject()` 里只能用 `scope.launch { ... }` 把 `TRANSFER_CANCEL` 发出去，这意味着**取消信令可能在进程被杀时丢失**（对端最终靠会话死亡感知）。

## 隔离性：两件事

### 上下文隔离

两条并发传输各自有 `contextId` + 独立的 `Channel(64)`。路由在 `SessionImpl` 的 pump 里（[06 篇](06-session.md)）：

```kotlin
ProtocolKind.TRANSFER_DATA, TRANSFER_COMPLETE, TRANSFER_CANCEL -> {
    val channel = transferContexts[frame.contextId]
    if (channel != null) {
        channel.send(TransferFrame(frame.kind, frame.payload))
        if (frame.kind != ProtocolKind.TRANSFER_DATA) {
            transferContexts.remove(frame.contextId)   // 结算了，停止路由
        }
    }
    // Frames for unknown or already-settled contexts belong to a
    // cancelled transfer; drop them.
}
```

测试 `concurrent transfers do not interfere` 用一个技巧配对两条传输——**用 metadata 里的标记字节**：

```kotlin
val t1 = clientSession.transfer(ByteSource(TransferMetadata("one", byteArrayOf(1)), ...))
val t2 = clientSession.transfer(ByteSource(TransferMetadata("two", byteArrayOf(2)), ...))
...
serverSession.transfers().collect { offer ->
    val tag = offer.metadata.data[0].toInt()   // 1 或 2
    destinationFor[tag] = destination
    acceptedJobs.add(offer.accept(destination))
}
```

注意那段注释也很实用：

```kotlin
// Awaiting happens outside the collector, otherwise the
// suspended collect cannot see the second offer.
```

**在 `collect` 里 `await()` 会阻塞收集，第二个 offer 就永远收不到。** 这是所有"入站 Flow + 每项一个长任务"的通用陷阱。正确写法是收集时只登记、把长任务 `launch` 出去。

### 与其它路径的隔离

测试 `transfer leaves message and request paths usable`：传输进行中，文字和请求照常工作。

这对**单 pump 架构**是一个强约束：pump 里任何 `send`（挂起）都会同时停掉三条路径。所以路由用的是 `channel.send`（挂起版）……等一下，这确实是个问题：

```kotlin
channel.send(TransferFrame(frame.kind, frame.payload))
```

`TransferFrame` 的 channel 容量是 64。**如果接收方的 `destination.write()` 很慢，这个 channel 会填满，`send` 就会在 pump 里挂起，整条会话停摆。**

这是当前实现的**真实缺陷**：`TRANSFER_DATA` 的路由是阻塞式，而 `STREAM_DATA` 的路由是非阻塞式（`trySend` + 丢弃）：

```kotlin
ProtocolKind.STREAM_DATA -> {
    val channel = streamContexts[frame.contextId]
    if (channel != null) {
        val result = channel.trySend(frame.payload)
        if (result.isFailure) {
            Log.w(TAG, "dropping stream frame ...: queue full")
        }
    }
}
```

**两者取舍不同是对的**：用 `trySend` 丢掉一个 transfer 的数据块会让整个文件损坏（不可接受），所以 transfer 必须阻塞。但"阻塞 pump"的代价是全会话停摆。

真正的解法是**per-context 独立协程消费**（把路由和 `destination.write` 解耦），或者**在 pump 里对 transfer 用 trySend + 明确的丢帧策略（结束整条 transfer）**。当前实现选了"阻塞"，行为正确但会把背压传导到无关路径。列在 [ROADMAP](../ROADMAP.md)。

## 没做的事

| 缺口 | 后果 |
|---|---|
| 无校验和/`hash` | 传输"成功"但内容损坏只能靠上层发现 |
| 无断点续传 | 中途断了要从头再来 |
| 无压缩 | 图片/视频应该上层自己压 |
| `totalBytes` 不校验 | `read()` 给的字节数和声明不符也照收 |

**第 2、3 条是有意的**（属于业务层，SDK 不猜），第 1 条应该在协议层补。

## 本篇自检

- 你的 `source.read` 是 `suspend` 吗？不是的话背压从哪来？
- 你的 `destination.close` 保证恰好一次吗？失败路径也调了？
- 取消能打断"卡在 IO"的发送方吗？靠什么？
- 结算路径数了几条？谁先到都会正确处理吗？
- 你的一条慢消费者会不会拖死同会话的其它业务？

---

下一篇：[09 · 连续流](09-stream.md)
