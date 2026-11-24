# 02 · 36 字节帧头：手写二进制格式的坑

> 对应代码：`wire/WireConstants.kt`、`wire/WireCodec.kt`、`wire/WireFrame.kt`、`wire/WireException.kt`
> 对应测试：`WireCodecTest`（7 个）

## 头部布局

大端（Big Endian），固定 36 字节，字段顺序永不改变：

| 偏移 | 长度 | 字段 | 类型 | 约束 |
|---:|---:|---|---|---|
| 0 | 2 | magic | `0x5A56` | 固定，认错就关连接 |
| 2 | 1 | version | `1` | 固定，版本不同直接拒 |
| 3 | 1 | kind | `Byte` | 必须命中 `ProtocolKind` 枚举 |
| 4 | 8 | frameId | `Long` | **非 0**，全局唯一 |
| 12 | 8 | contextId | `Long` | 仅 Transfer/Stream 帧非 0 |
| 20 | 8 | correlationId | `Long` | 仅 REQUEST/RESPONSE 非 0 |
| 28 | 4 | sequence | `Int` | 仅 DATA 帧可非 0 |
| 32 | 4 | payloadLength | `Int` | `[0, 1 MiB]` |
| 36 | n | payload | bytes | |

编码实现一共 15 行（`WireCodec.kt`）：

```kotlin
fun encode(frame: WireFrame): ByteArray {
    val total = WireConstants.HEADER_SIZE + frame.payload.size
    return ByteBuffer.allocate(total)
        .order(ByteOrder.BIG_ENDIAN)
        .putShort(WireConstants.MAGIC)
        .put(WireConstants.VERSION)
        .put(frame.kind)
        .putLong(frame.frameId)
        .putLong(frame.contextId)
        .putLong(frame.correlationId)
        .putInt(frame.sequence)
        .putInt(frame.payload.size)
        .put(frame.payload)
        .array()
}
```

## 七个坑，逐条讲

### 坑 1：magic 不是装饰

`0x5A56` 的作用是**尽早发现"这条连接根本不是这个协议"**。想象一条 TCP 连接被端口扫描器、被别的 App、被上一次运行残留的连接踩中，你会收到一堆看不懂的字节。4 字节开销换"第一个 2 字节就知道搞错了"，非常划算。

`Z=0x5A, V=0x56`——它是旧名 `ZevBond` 的首字母缩写，改名为 `ZevHub` 后**故意保留没改**。原因见坑 5。

### 坑 2：大端不是风格问题

```kotlin
.order(ByteOrder.BIG_ENDIAN)
```

Java 的 `ByteBuffer` **默认就是大端**。这行是显式写出来给读者看的，不是必需的。但 `putShort`/`putLong`/`putInt` 只要忘了设 order，就会写出小端字节——而你自己的解码器如果也用默认值，**测试会全绿、跨设备就炸**（不同 JVM 实现、不同 NDK 路径）。

所以 `WireCodecTest` 的第二个测试直接把字节钉死：

```kotlin
@Test
fun `header layout matches the frozen wire spec`() {
    val bytes = WireCodec.encode(frame())

    assertEquals(36, bytes.size)

    // Big endian, field order: magic, version, kind, frameId, contextId,
    // correlationId, sequence, payloadLength.
    assertEquals(0x5A, bytes[0].toInt() and 0xff)
    assertEquals(0x56, bytes[1].toInt() and 0xff)
    ...
    assertEquals(0x08, bytes[11].toInt() and 0xff)
    assertEquals(0, bytes[32].toInt())
    ...
}
```

**这类测试不是"测试逻辑"，是"锁定格式"。** 它保护的不是 bug，是"别人已经实现的另一份代码"。

### 坑 3：所有整数都要 `and 0xff`

Kotlin 的 `Byte` 是有符号的。`bytes[0].toInt() and 0xff` 里的 `and 0xff` 不是洁癖：

```kotlin
val kind = 0x80.toByte()
kind.toInt()        // -128
kind.toInt() and 0xff  // 128
```

解码侧全是这个模式，`WireDecoder` 里 `payloadLength` 也是 `header.int`（`Int`，安全），但 `kind`、`version` 是 `Byte`，必须以 `and 0xff` 解读。忘了就会得到"版本号是 -1"这种鬼故事，而 `-1 != 1`，行为上碰巧正确——这正是最危险的 bug：**错误代码掩盖了错误原因**。

### 坑 4：载荷长度是 `Int` 但上限是 1 MiB

```kotlin
const val MAX_FRAME_PAYLOAD = 1024 * 1024
```

`Int` 能表达 2 GiB，但协议只接受 1 MiB。这个上限的作用是**在分配内存之前拒绝**。如果按头部声明的长度直接 `ByteArray(n)`，一个恶意/损坏的 4 字节头就能让对端 OOM：

```kotlin
// WireDecoder.kt
if (payloadLength < 0 || payloadLength > WireConstants.MAX_FRAME_PAYLOAD) {
    throw WireException("Invalid payload length: $payloadLength")
}
```

注意 `payloadLength < 0` 这一半：`Int` 的第 31 位是符号位，所以**负数长度也必须拒**。测试里有一条专门的：

```kotlin
@Test
fun `rejects payload above one MiB`() {
    val header = WireCodec.encode(frame(ByteArray(1))).copyOfRange(0, 36)
    // Overwrite payload length with 1 MiB + 1.
    header[32] = 0x00; header[33] = 0x10; header[34] = 0x00; header[35] = 0x01
    assertThrows(WireException::class.java) { WireDecoder().feed(header) }
}
```

边界值是 `1 MiB` 本身**允许**（`round trip at the one MiB payload boundary`），`1 MiB + 1` 拒绝。

### 坑 5：头一旦发布就不能改

`version` 是 1，`magic` 是 0x5A56，字段顺序、宽度、语义——**全部冻结**。

改名 `ZevBond` → `ZevHub` 时，代码里 305 处引用都换了，但 `MAGIC` 一个字节都没动。因为一旦发布过一版 SDK，**已经装在用户手机上的旧版本会拿这个头跟新版本互连**。改 magic 等于协议不兼容，而协议不兼容的正确做法是升 `version`，不是改 `magic`。

同理 `WireCodecTest` 里那句注释是严肃的：`header layout matches the frozen wire spec`。

**实践建议**：在任何字段定稿前，先写这个"锁格式"测试，然后逼自己把格式冻结的规则写进文档。宁可多加 4 字节 padding，也不要将来改字段宽度。

#### 冻结包含什么：不只帧头，还有 kind 枚举

这是个容易漏掉的范围问题。本项目在补心跳时新增了一个 kind：

```kotlin
ACK(0x70),
ERROR(0x71),
HEARTBEAT(0x72);   // ← 新增
```

**加一个枚举值就是改协议。** 解码器对这个 kind 的校验是枚举匹配：

```kotlin
if (ProtocolKind.entries.none { it.value == kind }) {
    throw WireException("Unsupported kind: ${kind.toString(16)}")
}
```

所以旧版本对端收到 `0x72` 会以 `Unsupported kind` 断掉整条连接。

本次能加是因为 **SDK 未发版，没有兼容面**。一旦发过版，就只能：

1. 把它塞进已协商的能力位（v1 没有），或者
2. 升 `version` 并实现多版本共存

**冻结清单**（完整列表，写在协议规范里）：

| 项 | 位置 |
|---|---|
| `MAGIC` / `VERSION` | `WireConstants` |
| 8 个字段的顺序、宽度、语义 | `WireFrame` |
| 全部 `ProtocolKind` 枚举值 | `ProtocolKind` |
| 各 kind 的载荷格式 | 各 `*Codec` |
| `WireException.CODE_*` 数值 | `WireException` |

**一个自检办法**：问自己"对端用旧版本收到这个字节，会发生什么？"。如果答案是“断连”，那就是协议变更，不能当成普通功能上线。

### 坑 6：`frameId` 全局唯一而非会话唯一

```kotlin
class FrameIdGenerator {
    private val next = AtomicLong(1)   // 从 1 开始
    fun next(): Long = next.fetchAndIncrement()
}
```

起点是 **1 不是 0**，因为 `0` 被用作"无此字段"的占位（`contextId = 0`、`correlationId = 0` 都表示"不适用"）。如果 `frameId` 也可能是 0，就没法区分"没设"和"设成了 0"。

`ZevHubImpl.establishSession` 里有一条关键注释：

```kotlin
// One frame id generator per connection: handshake and session share
// the session-local monotonic sequence, per the wire spec.
val connectionFrameIds = FrameIdGenerator()
```

**握手和会话必须共用一个生成器**，否则 `frameId` 在会话内不单调。这个细节很难从 API 上看出来，是协议层的隐含约定。

### 坑 7：语义校验放在哪一层

`WireDecoder.validateHeaderUsage` 做的事是**跨字段语义校验**，不是长度校验：

```kotlin
val withCorrelation = kind == ProtocolKind.REQUEST.value ||
    kind == ProtocolKind.RESPONSE.value
val withContext = kind == TRANSFER_START/DATA/COMPLETE/CANCEL || STREAM_DATA/CLOSE
val withSequence = kind == TRANSFER_DATA || STREAM_DATA
```

也就是说一张表：**哪个 kind 允许哪几个字段非 0**。违反就抛 `WireException(CORRELATION/CONTEXT/...)`。

为什么放在解码器而不是各业务层？因为这是**"帧是否自洽"**的问题，跟业务无关。放在业务层意味着每个业务都要重复校验一遍，而且漏一个就是漏洞。

对应测试 `WireDecoderTest` 里有一整组（13 个测试里 7 个是这块）：

```kotlin
`rejects request without correlation id`
`rejects response without correlation id`
`rejects message with a correlation id`
`rejects transfer frame without context id`
`rejects message with a context id`
`rejects message with a non-zero sequence`
`rejects zero frame id`
`accepts request with correlation id and transfer frames with context id`
```

注意最后那条**正向**用例：只测"该拒的都拒了"，很容易把校验写成"全拒"然后误以为通过。

## 错误码也属于冻结的格式

```kotlin
class WireException(message: String, val code: Int = CODE_INVALID_FRAME) : Exception(message) {
    companion object {
        const val CODE_INVALID_FRAME = 0x0001
        const val CODE_UNSUPPORTED_VERSION = 0x0002
        const val CODE_UNSUPPORTED_KIND = 0x0003
        const val CODE_INVALID_PAYLOAD = 0x0004
        const val CODE_INVALID_CONTEXT = 0x0005
        const val CODE_INVALID_CORRELATION = 0x0006
        const val CODE_PROTOCOL_VIOLATION = 0x0007
        const val CODE_INTERNAL_ERROR = 0x0008
    }
}
```

这些值会**装进 ERROR 帧发给对端**（格式见 [05 篇](05-framing.md)）。所以它们和帧头一样是协议的一部分：**对端可能用这个码做日志分类或告警**。加新码只能追加，不能改旧值。

## 本篇自检

- 你能不看代码写出头部 8 个字段的名字和宽度吗？
- 你的格式里"长度不可信"如何防备？(记住：分配内存前拒绝)
- 你的字段里有没有一个"既可能是合法值又表示缺失"的坑？(本项目的答案是：用 0 表示缺失，因此真实值从 1 开始)
- 你有哪些东西已经冻结了？写下来。

---

下一篇：[03 · TCP 会粘包半包，所以必须流式解码](03-streaming-decoder.md)
