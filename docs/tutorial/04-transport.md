# 04 · 把阻塞 socket 关进协程

> 对应代码：`transport/ByteTransport.kt`、`transport/TcpTransport.kt`、`tcp/TcpLink.kt`
> 对应测试：`TcpLoopbackTest`

## 抽象只有 3 个方法

```kotlin
// transport/ByteTransport.kt
interface ByteTransport {

    suspend fun send(data: ByteArray)

    fun receive(): Flow<ByteArray>

    suspend fun close()
}
```

没有 `connect`、没有 `connectTimeout`、没有 `isConnected`、没有 `remoteAddress`。

**"怎么建立"不是传输层的事。** 建立连接需要地址、权限、组网状态——那些是链路层（`TcpLink`、`WifiP2pLink`）的活。`ByteTransport` 只承诺一件事：

> 我给你字节，你能有序、可靠地送到；对方给你的字节，我按到达顺序给你。

这个窄接口是"链路可替换"的唯一前提。`ProtocolImpl` 只认 `ByteTransport`，所以换成内存管道（`InMemoryTransport`）就能在 JVM 上测完整个协议栈——这是 [10 篇](10-testing.md) 的基础。

## 为什么是 `Flow<ByteArray>` 而不是 `suspend fun read(): ByteArray`

这三个都必须同时成立：

- **多块**：一次 1 MiB 传输会产生很多块，`read` 一次给一块，调用方循环
- **结束**：`Flow` 完成 = EOF（对端正常关闭）
- **失败**：`Flow` 抛异常 = 传输错误

用 `suspend fun read(): ByteArray?` 也能表达（`null` 表示 EOF），但会丢掉"异常携带"和"背压"的表达能力。`Flow` 天然满足，代价是**只能有一个收集者**——`TcpTransport` 明确写了这条约定：

```kotlin
/**
 * receive() must have a single collector; the flow completes on EOF and
 * fails on transport error.
 */
```

这不是 Flow 的限制，是实现的限制：`Channel.receiveAsFlow()` 被两个收集者同时订阅会**轮流抢数据**，两人各拿到一半。

## `TcpTransport` 的三条并发路径

一个 `TcpTransport` 实例里有三样东西在跑：

```
                 send()          ┌─ writeChannel(16) ─→ writeLoop ─→ socket out
应用 ──(多个协程并发)──→          └
                                   readLoop ←─ socket in ─→ incoming(64) ─→ receive()
```

拆开看：

### 写路径：为什么要一个 Channel

```kotlin
private val writeChannel = Channel<ByteArray>(capacity = 16)

override suspend fun send(data: ByteArray) {
    failure.get()?.let { throw it }
    writeChannel.send(data)
}

private suspend fun writeLoop(output: OutputStream) {
    try {
        for (data in writeChannel) {
            output.write(data)
            output.flush()
        }
    } catch (e: Exception) {
        if (!closed.get()) {
            socket.close()
            fail(e)
        }
    }
}
```

`Session` 里可能有几十个上下文同时在 `send`（每条流一帧、每个请求一帧）。`OutputStream.write` **不是线程安全的**，两个协程同时写会交错出一个字节都不对的帧。

用 `Channel` 串起来 = 天然的**单写者** + **背压**。容量 16 意味着：写不出去的时候第 17 个调用者会挂起，而不是在内存里堆帧。对 1 MiB 的帧来说，16 已经是很深的缓冲了。

`output.flush()` 每帧一次是必要的：`Socket` 的 `OutputStream` 默认有缓冲，不 flush 会攒着不发，P2P 上就是几十毫秒的额外延迟。

### 读路径：块大小有个坑

```kotlin
private suspend fun readLoop(input: InputStream) {
    val chunk = ByteArray(READ_CHUNK)      // 16 * 1024
    try {
        while (true) {
            val n = input.read(chunk)
            if (n < 0) break
            incoming.send(chunk.copyOf(n))   // ← 注意 copyOf
        }
        incoming.close()
    } catch (e: Exception) {
        fail(e)
    }
}

private const val READ_CHUNK = 16 * 1024
```

**`chunk.copyOf(n)` 这个拷贝不能省。** `chunk` 是复用的同一个数组，下一轮 `read` 会覆盖它。如果直接把 `chunk` 交出去，`WireDecoder` 刚拿到的字节就被下一轮改了——这类 bug 表现为"偶发解码错误、负载越高越频繁"，极难定位。

16 KB 的选择：小于它则系统调用次数多（1 MiB 帧要 64 次），大于它则小帧的延迟变大（要攒满才 read）。16 KB 大致等于一个 TCP 窗口的量级，是个安全的默认值。

### 关闭路径：谁先谁后

```kotlin
override suspend fun close() {
    if (!closed.compareAndSet(false, true)) return

    writeChannel.close()          // 1. 让 writeLoop 的 for 循环结束
    incoming.cancel()             // 2. 立刻让 receive() 的收集者停止
    withContext(Dispatchers.IO) {
        runCatching { socket.close() }   // 3. 让阻塞的 read 立刻返回
    }
    readJob.join()                // 4. 等两个协程真的退出
    writeJob.join()
}
```

顺序不能换：

- 先关 `socket` 再关 `writeChannel`：`writeLoop` 还在 `output.write` 里阻塞，`socket.close()` 会让它抛异常，然后走 `catch` → `!closed.get()` 为 false（已经置 true）→ 什么都不做 → `writeChannel` 永远不关。**协程泄漏。**
- 不 `join` 就返回：调用方以为关完了，实际读协程还持有 socket 引用。

这也是为什么 `close()` 是 `suspend`：**必须等协程真的退出。** `AutoCloseable` 的 `close()` 是同步的，所以 `ZevHubImpl` 里用 `runCatching { it.close() }` 在协程里调（见 [06 篇](06-session.md)）。

### 失败传播：一个 `AtomicReference`

```kotlin
private val failure = AtomicReference<Throwable?>(null)

private fun fail(e: Throwable) {
    if (closed.get()) {
        incoming.cancel()
    } else {
        failure.compareAndSet(null, e)
        incoming.close(e)      // ← 把异常塞进 Flow
    }
}
```

两个设计决定：

1. **`incoming.close(e)`** 而不是 `incoming.close()`：收集者会**收到异常**而不是正常结束，这样才能区分"对端优雅关闭"和"链路断了"。
2. **`failure` 记住第一个错误**，`send()` 开头检查它。否则读路径已经死了，写路径还在傻乎乎地往 `writeChannel` 塞数据，最后表现为"**发送方一直成功，接收方一直没反应**"——最典型的协议栈幻觉 bug。

还有一条容易漏的：

```kotlin
} catch (e: Exception) {
    if (!closed.get()) {
        // A live write failure kills the whole path: fail the read
        // side so the upper layers observe the transport going down.
        socket.close()
        fail(e)
    }
}
```

写失败要**主动 `socket.close()`**，把读侧也叫醒。TCP 是全双工的，写一半挂了读可能还在等数据，不主动关就一直挂着。

## `TcpLink`：两个方向的活

`TcpTransport` 只管一条已存在的连接，`TcpLink` 管"怎么建立"。

### 监听（组主 / group owner 侧）

```kotlin
fun listen(port: Int): Flow<ByteTransport> = flow {
    val server = ServerSocket()
    try {
        server.reuseAddress = true
        server.bind(InetSocketAddress(port))
        while (currentCoroutineContext().isActive) {
            // Cancellation must reach the blocked accept(): runInterruptible
            // interrupts the IO thread so the finally below releases the port.
            val socket = runInterruptible { server.accept() }
            socket.tcpNoDelay = true
            emit(TcpTransport(socket, scope))
        }
    } finally {
        runCatching { server.close() }
    }
}.flowOn(Dispatchers.IO)
```

两个关键点：

**`runInterruptible` 不是可选优化。** `ServerSocket.accept()` 是阻塞的，协程取消**不会**打断它。不加 `runInterruptible`，取消 `listen` 之后 `finally` 永远不执行，9527 端口一直被占着，下一次组网时 bind 失败。

这是"把阻塞 IO 放进协程"最经典的坑：**协程取消 ≠ 线程中断**，你得自己用 `runInterruptible`（或 `runInterruptibleIO`）把中断打进去。

**`emit` 在 `flowOn(IO)` 的 flow 里**：每个新连接都 `emit` 一个 `ByteTransport`，所以在 `ZevHubImpl` 里是：

```kotlin
tcp.listen(TcpLink.DEFAULT_PORT).collect { transport ->
    establishSession(transport, initiator = false, expectedPeer = null)
}
```

一次组网可能有多个连接进来（重连、多个 peer），`Flow` 天然支持。

### 连接（成员 / member 侧）带退避重试

```kotlin
suspend fun connect(host: String, port: Int): ByteTransport = withContext(Dispatchers.IO) {
    var attempt = 0
    while (true) {
        try {
            val socket = Socket()
            socket.reuseAddress = true
            socket.tcpNoDelay = true
            socket.connect(InetSocketAddress(host, port), CONNECT_TIMEOUT_MS)
            return@withContext TcpTransport(socket, scope)
        } catch (e: Exception) {
            if (attempt >= MAX_ATTEMPTS) {
                throw IllegalStateException("TCP connect to $host:$port failed", e)
            }
            attempt++
        }
        delay(backoffDelay(attempt))
    }
    @Suppress("UNREACHABLE_CODE")
    error("unreachable")
}
```

**这个重试不是为了"网络不好"，而是为了"接口还没起来"。**

Wi-Fi P2P 组刚形成时，group owner 的 IP 可能已经可用了，但：

- 组主自己的 `listen()` 可能还没 bind 完
- 成员的网卡可能还没拿到 DHCP 地址

于是**第一次 connect 必然 `ECONNREFUSED`**。不重试的话，组网成功率基本为零。

退避：

```kotlin
private fun backoffDelay(attempt: Int): Long {
    val shifted = BASE_DELAY_MS shl (attempt - 1).coerceIn(0, 4)
    return shifted.coerceAtMost(MAX_DELAY_MS)
}
// BASE_DELAY_MS = 500, MAX_DELAY_MS = 10_000, MAX_ATTEMPTS = 10
```

实际间隔：500 → 1000 → 2000 → 4000 → 8000 → 10000 → 10000 ...（`shl` 上限 4 位，即最多 ×16）

注意 `attempt - 1` 在第一次失败时是 0，所以第一次等 500ms。`coerceIn(0, 4)` 防止 `attempt` 很大时移位溢出。

**`tcpNoDelay = true` 必须开。** Nagle 算法会攒小包，对"一次一帧"的协议意味着额外延迟。所有 socket 都设了：`listen` 侧的 accept 结果、`connect` 侧。

## 端口为什么写死 9527

```kotlin
const val DEFAULT_PORT = 9527
```

因为**双方事先没有协商机会**。组网之后唯一能确定的信息是"组主在 `groupOwnerAddress` 上"，端口必须约定。

代价：如果用户手机上别的 App 占了 9527，`bind` 失败。当前实现的做法是**放弃本次组网、等下一次 group event**：

```kotlin
// ZevHubImpl.startListening()
} catch (e: Exception) {
    // Bind failure or listener teardown; the group event flow
    // decides whether listening is retried on the next group.
}
```

更好的做法是失败时上报给应用，但目前是静默的（见 [ROADMAP](../ROADMAP.md)）。

## 没做的事

`ByteTransport` 这一层**没有任何加密**。握手里的签名是空的（`EMPTY_SIGNATURE`），载荷是明文。也就是说同一个 P2P 组里任何设备都能读到你的字节。这是身份阶段要补的（[ROADMAP](../ROADMAP.md) 阶段 3），而**加密的位置就应该在这里**：`ByteTransport` 外面再包一层 `SecureTransport(inner: ByteTransport)`。

这也是窄接口的回报：加加密**不需要改上面任何一层**。

## 本篇自检

- 你的 `read()` 循环有没有 `copyOf`？没有的话谁在改这块内存？
- 你的 `close()` 会不会等 IO 协程退出？不等的话资源什么时候释放？
- 阻塞调用能被取消打断吗？用的是什么？
- 读路径挂了之后，写路径知道吗？两边不一致时的现象是什么？
- 你的 `connect` 重试是因为网络抖动，还是因为"对端还没准备好"？这两种要用的策略完全不同。

---

下一篇：[05 · 组帧层](05-framing.md)
