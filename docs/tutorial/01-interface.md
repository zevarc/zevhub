# 01 · 先定 API，再定协议

> 对应代码：`zevhub/src/main/java/com/zevarc/zevhub/ZevHub.kt`、`session/Session.kt`

新手最常见的顺序是：先想二进制格式 → 再想协议 → 最后包一层 API 给用户。
本项目是**反过来的**：先把"用户要写什么代码"钉死，再让协议去满足它。

原因很简单：**API 是承诺，协议是实现细节。** 协议改一次是所有端一起改；API 改一次是所有接入方一起改。先把成本高的那个定下来。

## 门面：只有 6 个方法

```kotlin
// zevhub/ZevHub.kt（节选）
interface ZevHub : AutoCloseable {

    val device: Device

    suspend fun start()

    fun discover(): Flow<Device>

    suspend fun connect(device: Device): Session

    fun incomingSessions(): Flow<Session>

    override fun close()
}
```

六个方法的形状不是随便定的，每个都对应一个设计决定：

| 方法 | 设计决定 |
|---|---|
| `start()` 是 `suspend` | 底层要注册 DNS-SD 服务、拿 channel，**会失败**。不返回 `Boolean`，失败就抛 |
| `discover()` 返回 `Flow` | 发现是**持续**过程，不是"查一次拿个列表" |
| `connect()` 是 `suspend` | 邀请对方 + 组网 + TCP + 握手，全程可能要 30 秒 |
| `incomingSessions()` 返回 `Flow` | 别人也可能连你，且可能不止一次 |
| `device` 是属性不是方法 | 身份在 `create()` 时就确定了，同步可读 |
| 继承 `AutoCloseable` | 用 Kotlin 的 `use { }` 自动收尾 |

注意 **`discover()` 和 `incomingSessions()` 是分开的**。看起来像"上线的人"一件事，实际是两件：

- `discover()`：对方**还没跟我组网**，靠广播发现
- `incomingSessions()`：对方**已经跟我组网并握手完成**，是一条可用会话

实现里这两条路径甚至不共享代码（`WifiP2pLink.discover` vs `ZevHubImpl.establishSession`）。`ZevHub.kt` 的注释里专门写了一句提醒，因为这是接入方最容易踩的坑：

```kotlin
/**
 * Discovery only surfaces devices reachable over the air. A peer that
 * has already joined this device's P2P group does not answer discovery
 * probes, so an established peer will not appear while the group is
 * up. Use the session obtained from [connect] or [incomingSessions]
 * instead of re-discovering a connected peer.
 */
fun discover(): Flow<Device>
```

## 会话：四种语义，两两成对

```kotlin
// session/Session.kt（节选，省略注释）
interface Session {

    val device: Device

    // ① 文字
    suspend fun send(message: Message)
    fun messages(): Flow<Message>

    // ② 请求/响应
    suspend fun request(request: Request, timeoutMs: Long = 30_000L): Response
    fun requests(): Flow<IncomingRequest>

    // ③ 大块数据
    fun transfer(source: TransferSource): Transfer
    fun transfers(): Flow<IncomingTransfer>

    // ④ 连续流
    suspend fun openStream(type: StreamType): Stream
    fun streams(): Flow<IncomingStream>

    suspend fun close()
}
```

四种语义，每种都是「**出站一个动作 + 入站一个 Flow**」的对子。这个对称性是有意的：

- 出站 = 我主动发起（`suspend`，可能要等对方）
- 入站 = 对方发起（`Flow`，我必须监听否则对方会卡住）

**入站为什么必须是 `Flow` 而不是回调？** 因为回调丢了就没有第二次机会，而 `Flow` 至少有背压和取消语义。实现里所有入站都是 `Channel(64)`，满了会拒绝或者超时（[06 篇](06-session.md) 详述）。

## 两处不对称，都是被逼的

### 不对称一：`transfer()` 不挂起，`openStream()` 挂起

```kotlin
fun transfer(source: TransferSource): Transfer          // 立刻返回
suspend fun openStream(type: StreamType): Stream        // 等到对方同意
```

大块数据可以**先发再说**：`transfer()` 立刻返回一个句柄，数据要等对方 `accept` 才真正开始流动。对方不愿意收，就走 `TransferResult.Cancelled`，不抛异常。

流不行。流的定义是"一条有序的帧通道"，如果 `openStream()` 立刻返回，接下来 `send()` 的帧该往哪个上下文放？对方还没同意，上下文 id 就没达成一致。所以只能挂起等协商结果，拒绝就抛 `StreamRejectedException`。

### 不对称二：失败的处理方式

| 场景 | 怎么表达 | 为什么 |
|---|---|---|
| 请求的业务失败 | `Response.Failure(errorCode, data)` | 业务错误是**正常返回值**，会话继续可用 |
| 请求超时/取消 | 抛 `TimeoutCancellationException` | 只是本地等待结束，对方不受影响 |
| 链路死了 | 抛异常 | 没有会话了，无可返回 |
| 传输被拒/取消 | `TransferResult.Cancelled` | 终态之一，不是错误 |
| 流被拒 | 抛 `StreamRejectedException` | 没有句柄可返回，只能抛 |

`Session.kt` 里对这条写得很直白：

```kotlin
/**
 * The timeout is a local operation semantic: expiring or cancelling the
 * call stops only the local wait and never affects the peer. A business
 * failure is returned as [Response.Failure], not thrown; transport or
 * protocol death throws.
 */
suspend fun request(...)
```

这条规则的价值在于：**调用方能用 `try/catch` 的边界区分"我这边决定放弃"和"通道真的没了"**。如果超时也抛业务异常、链路死亡也抛业务异常，接入方就没法做重试决策。

## 门面里绝不出现的东西

对照 `Session.kt` 的公开签名，以下词一个都没有：

`Socket`、`TCP`、`WiFi`、`P2P`、`UTF-8`、`JSON`、`frame`、`byte order`、`port`、`IP`

`Device` 只有两个字段（`device/Device.kt`）：

```kotlin
data class Device(val id: DeviceId, val name: String?)
```

**没有 MAC 地址、没有 IP、没有端口。** 这些是"怎么连"的事，属于链路层。MAC 只在 `DiscoveryRegistry` 内部用来做配对（[07 篇](07-discovery.md)），连接时由链路自己查。

`ByteArray` 是唯一泄漏到底层的类型，这是刻意的：载荷语义交给上层服务，SDK 不猜你传的是图片还是 protobuf。

## 可见性约定

```
ZevHub / Session / Stream / Transfer / IncomingXxx   → public（承诺）
ZevHubImpl / SessionImpl / ProtocolImpl / WireCodec  → internal（随便改）
```

`internal` 不是摆设。它就是第 11 篇讲的"API 稳定性"的地基——**实现全在 internal 里，你才能不改公开 API 的前提下重构**。当前公开表面收敛到 24 个顶层声明，其余均 `internal`。

## 一个反直觉的小决定

`request()` 的超时默认值是 `30_000L`，写在 `companion object` 里：

```kotlin
companion object {
    const val DEFAULT_REQUEST_TIMEOUT_MS = 30_000L
}
```

而不是给个"无限等待"。因为**无限等待在 P2P 场景下等于泄漏**：对方可能已经走进电梯了，你的协程会一直挂着。默认值才是大多数人会用的值——默认值错了，等于 API 设计错了。

## 本篇自检

拿你自己的 SDK 想法套一遍这四个问题：

1. 用户第一行代码长什么样？如果不写这一行，你其实没设计 API。
2. 哪些操作必须挂起？"必须等对方"的那几个才是。
3. 入站事件用什么承载？没有背压的方案会在慢消费者上炸。
4. 失败分了几种？分不清"我放弃"和"通道死了"，接入方就没法写重试。

---

下一篇：[02 · 36 字节帧头](02-wire-format.md)
