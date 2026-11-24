# 03 · TCP 会粘包半包，所以必须流式解码

> 对应代码：`wire/WireDecoder.kt`
> 对应测试：`WireDecoderTest` 前半（`reassembles a frame split across chunks` 等 5 个）

## 问题

`Socket.getInputStream().read(buf)` 的语义只有三条：

1. 返回 `-1` 表示 EOF
2. 返回 `n > 0` 表示"我给你了 n 个字节"
3. **不保证 n 里装的是几个完整的帧**

于是你至少会遇到两种现实：

- **半包**：一帧 1 MiB 的传输，`read` 给你 16 KB，剩下 1008 KB 还没到
- **粘包**：对端连发 5 个 200 字节的小帧，内核合并成一次 `read` 给你 1000 字节

结论：**解码器不能是函数，必须是对象**。它得持有状态。

## 实现：一个累积缓冲 + 一个循环

```kotlin
// wire/WireDecoder.kt
class WireDecoder {

    private var buffer: ByteArray = ByteArray(0)
    private var length: Int = 0

    fun feed(bytes: ByteArray): List<WireFrame> {
        if (bytes.isEmpty()) return emptyList()

        // 1. 拼接：旧残留 + 新到的
        val merged = ByteArray(length + bytes.size)
        buffer.copyInto(merged, 0, 0, length)
        bytes.copyInto(merged, length)
        buffer = merged
        length = merged.size

        val frames = ArrayList<WireFrame>(2)
        while (true) {
            // 2. 不够一个头 → 留着等下次
            if (length < WireConstants.HEADER_SIZE) return frames
            ...
            // 3. 头够了但载荷没到齐 → 留着等下次
            val totalSize = WireConstants.HEADER_SIZE + payloadLength
            if (length < totalSize) return frames
            ...
            // 4. 切出一帧，从缓冲里删掉
            buffer = buffer.copyOfRange(totalSize, length)
            length -= totalSize
        }
    }
}
```

结构就这四步。关键在**两个"返回但不报错"的分支**（第 2、3 步）：数据不全不是错误，是常态。

## 五个必须想清楚的问题

### 问题 1：为什么不用 `ByteBuffer` 而用 `ByteArray` + `copyOfRange`

每次切帧都 `copyOfRange` 显然有拷贝开销。这是**故意的简单**：

- 帧上限 1 MiB，单帧拷贝成本可控
- 换成环形缓冲/`ByteBuffer.compact()` 会引入"已读位置"这个状态，而它正是环形缓冲 bug 的主要来源
- 这个类是全项目最不该有 bug 的地方之一（出错就整条连接死）

如果将来真的不够快，替换点是**这个类的内部**，公开签名 `feed(bytes): List<WireFrame>` 不变。**先正确，再快。**

### 问题 2：校验顺序 = 攻击面的顺序

注释里写得明确：

```kotlin
/**
 * Validation order follows the wire spec: magic, version, kind,
 * payload length, then field constraints.
 */
```

顺序有意义：

```
magic → version → kind → payloadLength → 语义约束
```

- `magic/version/kind` 都在前 4 字节，**读 4 字节就能拒掉一条垃圾连接**
- `payloadLength` 在读载荷**之前**校验，所以 1 MiB 上限能挡住 OOM
- 语义约束（`validateHeaderUsage`）在**切出载荷之后**，因为它只跟头部字段有关，但放在这里能少一次循环

注意一个细节：`kind` 的检查用的是完整枚举匹配：

```kotlin
if (ProtocolKind.entries.none { it.value == kind }) {
    throw WireException("Unsupported kind: ${kind.toString(16)}")
}
```

而不是 `when` 里白名单，因为**新增 kind 时不该忘记改校验**。枚举是唯一真相（`ProtocolKind.kt`，见 [05 篇](05-framing.md)）。

### 问题 3：解析头部用了两次 wrap

```kotlin
val header = ByteBuffer.wrap(buffer, 0, WireConstants.HEADER_SIZE).order(BIG_ENDIAN)
val magic = header.short
val version = header.get()
val kind = header.get()

header.position(PAYLOAD_LENGTH_OFFSET)   // 32
val payloadLength = header.int
```

然后拿 ids 时**又 wrap 了一次**：

```kotlin
val ids = ByteBuffer.wrap(buffer, 0, WireConstants.HEADER_SIZE).order(BIG_ENDIAN)
ids.position(FRAME_ID_OFFSET)   // 4
val frameId = ids.long
val contextId = ids.long
val correlationId = ids.long
val sequence = ids.int
```

看起来冗余，但比 `position(...)` 来回跳更不容易错：**读 4 个定长字段，一组连续的 `get` 最不容易出 off-by-one**。`ByteBuffer.wrap` 不拷贝，开销是零。这是"为了可读性牺牲一点优雅"。

### 问题 4：缓冲区的可变性陷阱

```kotlin
buffer = buffer.copyOfRange(totalSize, length)
length -= totalSize
```

`buffer` 是 `ByteArray`，而 `ByteArray` 是**可变**的。如果 `feed` 把 `buffer` 的切片直接当 `payload` 返回（比如 `buffer.copyOfRange(HEADER_SIZE, ...)`——这个确实是拷贝），或者更糟：`Arrays.copyOf` 传出去，调用方改一个字节就会污染后续解码。

当前实现是全拷贝的（`copyOfRange` 每次都新建数组），所以安全。**如果你的实现想优化成零拷贝，就必须立刻把 `WireFrame.payload` 换成只读视图**，否则这是个潜伏的污染 bug。

### 问题 5：出错之后缓冲就不该存在了

`WireDecoder` 里没有任何 `reset()` 或者"跳过坏帧"的逻辑。一旦 `feed` 抛 `WireException`，**这个解码器实例就废了**。

这是刻意的，与 [00 篇](00-why.md) 的"致命错误 = 关闭连接"一致：

- 长度字段不可信 → 后续所有帧的边界都不可信
- 没有"重新同步"的可能（没有帧间隔标记、没有转义）
- 所以上层 `ProtocolImpl` 收到异常就直接 `transport.close()`

**如果你的格式需要"能跳过坏帧"，那必须在格式里留同步标记（比如 magic 重复出现、或者长度校验和）。** 本格式不支持，就不假装支持。

## 五个对应测试

```kotlin
@Test fun `reassembles a frame split across chunks`()
@Test fun `decodes multiple coalesced frames in one chunk`()
@Test fun `holds a partial header until more bytes arrive`()
@Test fun `holds a partial payload until more bytes arrive`()
@Test fun `empty chunks are ignored`()
```

看第一个测试的写法值得学：

```kotlin
// 逐字节喂进去，最极端的半包
val decoder = WireDecoder()
val results = mutableListOf<WireFrame>()
bytes.forEach { b ->
    results += decoder.feed(byteArrayOf(b))
}
```

**逐字节喂**是最狠的碎片化测试。真实网络不会这么碎，但测试要覆盖最坏情况。同样第三个测试专门喂 `HEADER_SIZE - 1` 字节，第四个专门喂 `HEADER_SIZE + payloadLength - 1` 字节——**边界各差一个字节**。

`empty chunks are ignored` 那条也有实际意义：`TcpTransport` 的读循环里 `n` 不可能是 0（`read` 返回 0 只在长度为 0 的 buffer 上发生），但防御一下无害，且能避免 `copyInto` 的无谓复制。

## 一个容量考量

```kotlin
val frames = ArrayList<WireFrame>(2)
```

初始容量 2：大多数 `feed` 调用返回 0 或 1 个帧，偶尔粘包 2~3 个。用默认容量 10 会浪费一点，用 1 会立刻扩容。这种微优化不重要，重要的是**不要写成 `ArrayList()` 然后每次扩容三次**——这个循环在网络路径上每毫秒都过一遍。

## 本篇自检

- 你的解码器是函数还是对象？如果是函数，你怎么表达"数据不够"？
- 你的 `feed` 返回列表还是单个？返回单个意味着调用方要自己循环，容易漏。
- 你会不会因为"数据不够"就抛异常？那对端发大帧时永远失败。
- 有没有一处 `read` 出来的数据你**没拷贝**就交给了上层？如果上层的 buffer 还会复用（比如 `ByteArray` 池），这就是必崩的 bug。

---

下一篇：[04 · 把阻塞 socket 关进协程](04-transport.md)
