# ZevHub 技术路线与欠账清单

> 设计目标：**不依赖底层链路**，为上层提供文字、图片、流等格式的传输。
> 当前版本：**v0.1.0 已发布**（JitPack）；协议 `version = 1`（冻结）；103 个单测全绿。

这份文档只记两件事：**已经有什么**、**已知的窟窿**。不写"未来要做的功能"，那些属于需求池。

---

## 一、能力现状

SDK 目前提供四种传输语义，全部已在 `:zevhub` 内实现并有测试覆盖：

| 语义 | 入口 | 状态 | 测试 |
|---|---|---|---|
| 文字 | `Session.send` / `messages()` | ✅ | `SessionImplTest` |
| 请求/响应 | `Session.request` / `requests()` | ✅ | `SessionRequestTest`（10） |
| 大块数据（图片/文件） | `Session.transfer` / `transfers()` | ✅ | `TransferTest`（11） |
| 连续流（音频/视频帧） | `Session.openStream` / `streams()` | ✅ | `StreamTest`（7） |

连接可靠性（原缺失，近期补齐，移植自 FastAir 的成熟实现）：

| 能力 | 入口 | 状态 | 测试 |
|---|---|---|---|
| 心跳判死 | `ProtocolImpl` 内部 `HEARTBEAT` 帧 | ✅ | `ProtocolImplTest`（2） |
| 写路径可取消 | `TcpTransport.writeLoop` 的 `ensureActive` | ✅ | 现有取消类测试覆盖 |
| 指数退避重连 | `ZevHubImpl.scheduleReconnect` | ⚠️ 代码完成，**未测试** | — |
| 会话终态通知 | `SessionImpl(onClosed = ...)` | ✅ | `SessionRequestTest`（2） |

**注意：三种格式目前都是"字节通道"，没有类型契约。** 图片传过去对方只知道"这是 3 MiB 的某种数据"，不知道 MIME、尺寸、是否缩略图。契约层是阶段 1 的事。

### 分层现状

```
ZevHub         发现 / 连接 / 会话 / 重连              ✅ 完成（重连未测试）
  Session      文字 / 请求 / 大块 / 流 / 终态回调     ✅ 完成
    Protocol   帧 / ACK / 心跳 / 序号 / 致命错误      ✅ 完成
      ByteTransport  字节流                            ✅ 完成（仅 TcpTransport）
        WifiP2pLink + TcpLink  组网 + TCP             ✅ 完成
```

已冻结的线格式共 15 个 kind，其中 `HEARTBEAT(0x72)` 是在 v0.1.0 之前加的。**v0.1.0 起有兼容面了**：之后新增或改动任何 kind 都是破坏性变更，必须升 `version` 并做多版本共存。

---

## 二、阶段 1 · 让三种格式有类型契约

现在 `TransferMetadata` 是纯 opaque bytes（[SDK 的刻意设计](tutorial/08-transfer.md)），但"图片"和"文件"的差别是**双方要能达成一致的语义**。

建议独立模块 `:zevhub-codec`（不进 AAR 的 `api`，可选依赖）：

| 契约 | 内容 |
|---|---|
| `TextPayload` | UTF-8、可选时间戳、长度上限策略 |
| `ImagePayload` | MIME、宽高、字节数、是否缩略图、原图引用 |
| `FilePayload` | 文件名、MIME、大小、sha256 |
| `MediaStream` | 帧边界、关键帧标记、时间戳、丢帧策略 |

**关键决策**：契约不进 `zevhub` 核心。理由：核心不猜载荷语义是它保持中立的唯一办法；而"不依赖链路"和"不依赖业务"是同一个思路的两次应用。

**验证点**：`:app` 的 debug 界面能发一张真图并收到，接收侧能读出 MIME。

---

## 三、阶段 2 · 补可靠性

### 2.1 ACK 是死代码（高优先级）

`ProtocolImpl` 收到 `MESSAGE` 会**自动回 ACK**：

```kotlin
ProtocolKind.MESSAGE.value -> {
    sendAck(frame.frameId)
    incoming.send(toProtocolFrame(frame))
}
```

但 `ACK` **没有任何消费者**：不等待、不重传、不超时。全项目 `grep` ACK 只有 4 处，全在发送侧。

两条出路，必须选一条并写进文档：

- **A（推荐）**：明确声明"TCP 之上不做重传"，把 ACK 移除或降级为可选的心跳。删代码比留死代码安全。
- **B**：实现重传窗口（发送侧记未确认 frameId，超时重发）。代价是 `MESSAGE` 从"无状态"变成"有状态"，且需要处理重复投递。

**决定（已实施）**：选 A 的变体——**心跳承担存活性，ACK 保持"只发不收"**。

理由：ACK 和心跳解决的是两个不同问题。心跳回答"对端还在吗"，ACK 回答"这一帧到了吗"。在 TCP 之上，帧不会丢，所以"这一帧到了吗"没有实际用途；但"对端还在吗"是 TCP 完全答不了的问题（设备走出 Wi-Fi 范围时连接可以几分钟不断开）。所以心跳必须有，ACK 保持现状并在文档里写明"不做重传"。

副作用：`ACK` 仍然是**没有消费者**的代码。要么将来接上重传，要么删掉——不要留着让人以为有可靠性保证。

### 2.2 `Session.close()` 的清理路径（已修复）

**原判断有误，实际是真缺陷。** 早期结论是"`pumpJob.cancel()` 抛出的 `CancellationException` 会被 pump 的 `catch (e: Exception)` 捕获，所以 `failPendingRequests` 实际执行了"。

补 `onClosed` 钩子时发现：`pumpJob.cancel()` 让协程**直接终止**，既不执行 `collect` 之后的收尾，也不进入 `catch`。当时之所以看起来正常，是因为 `close()` 里后面紧跟的 `protocol.close()` 等调用间接让 transport 关闭、`collect` 正常完成——**靠巧合，不是设计**。

现在 `close()` 显式完成全部七个收尾动作，并用 `terminated` 哨兵保证 `onClosed` 恰好触发一次。回归测试：`closing a session locally does not look like a link failure`。

### 2.3 transfer 的路由会阻塞 pump（架构性取舍）

```kotlin
// SessionImpl pump 内
ProtocolKind.TRANSFER_DATA, ... -> {
    val channel = transferContexts[frame.contextId]
    if (channel != null) {
        channel.send(TransferFrame(frame.kind, frame.payload))   // ← 挂起版
    }
}
```

对比 `STREAM_DATA` 用的是 `trySend`（满了丢弃 + 日志）。

**当前是故意的**：丢一个 transfer 的数据块会让文件损坏，所以必须阻塞。但代价是**接收方 `destination.write()` 慢时，整个会话（文字、请求、其它流）一起停摆**。

三条候选：

1. 每条 transfer 一个独立消费协程，pump 只做 `trySend` + 满则**取消整条 transfer**（而不是阻塞）
2. pump 里对 transfer 保留阻塞，但把 `Channel(64)` 换成 **`Channel(RENDEZVOUS)`** 并明确文档化"慢消费者会阻塞会话"
3. 维持现状 + 文档化风险

倾向 1：**"一个业务拖死全部业务"是 SDK 不可接受的失败模式**。

### 2.4 其它

| 缺口 | 位置 |
|---|---|
| 重连**逻辑未测试**（依赖真机 P2P，只测了触发它的 `onClosed`） | `ZevHubImpl.scheduleReconnect` |
| 会话恢复（重连后旧会话的上下文/未完成请求全部丢失，不续传） | `ZevHubImpl` |
| `Stream` 接收侧丢帧策略不可配置 | `SessionImpl.pump` |
| `Stream.receive()` 不暴露序号，应用无法感知丢帧 | `Stream` |
| transfer 无校验和 | `TransferCodec` |
| `TransferSource.read` 未文档化"必须响应取消" | `TransferSource.kt` |
| `Stream.send` 并发调用 = 会话死亡，但没写进 KDoc | `Stream.kt` |

---

## 四、阶段 3 · 安全与发布

### 3.1 握手是空的（安全）

```kotlin
// Handshake.kt
payload = HandshakeCodec.encodeAuth(EMPTY_SIGNATURE)   // ByteArray(0)
// receiveAuth 里：// Signature verification is deferred to the identity stage.
```

现状：**任何能进同一个 P2P 组的设备都能跟你会话，且载荷明文可读。**

格式已留好位置（`HELLO` 有 `publicKey` + `nonce`，`AUTH` 有 `signature`）。要做：

| 项 | 内容 |
|---|---|
| 密钥对 | 在 `LocalIdentity` 里生成并持久化（签名密钥） |
| HELLO | 真公钥，`DeviceId` 改为公钥指纹或绑定 |
| AUTH | 用对方 nonce 签自己的身份，互相验签 |
| 传输加密 | `SecureTransport(inner: ByteTransport)` —— 窄接口的回报：加这个不用改上面任何一层 |

**当前定位必须写进 README**：同房间可信设备互联，不是陌生人安全通信。

### 3.2 SDK 发布

| 要做 | 现状 |
|---|---|
| `maven-publish` 配置 | ❌ 无 |
| 统一版本号（`gradle.properties`） | ❌ 无 |
| SDK 的 `README.md` | ❌ 无（仓库根也没有） |
| `binary-compatibility-validator` | ❌ 无（**建议最先加，零改造**） |
| `explicitApi()` | ❌ 无 |
| `internal` 泄漏收口（**已完成**，公开表面剩 24 个顶层声明） | ✅ |
| KDoc 三要素（挂起性/失败表达/调用方责任） | 部分 |

### 3.3 协议版本策略

现在 `WireConstants.VERSION = 1`，握手只认 1：

```kotlin
require(hello.version == HandshakeCodec.PROTOCOL_VERSION)
```

将来升到 2 时，"一台设备升了 App、一台没升"必须能优雅降级或明确报错。**多版本共存要在写 v2 之前想清楚**，不能等。

### 3.4 发布与消费（已发版 v0.1.0）

SDK 以 AAR 形式被上层 App 消费。已发布 tag `v0.1.0`，JitPack 自动构建。

```kotlin
// settings.gradle.kts
dependencyResolutionManagement {
    repositories { google(); mavenCentral(); maven { url = uri("https://jitpack.io") } }
}

// app/build.gradle.kts
implementation("com.github.zevarc:zevhub:v0.1.0")
```

**坐标不是 JitPack 文档里的常见形式。** 文档通常写 `com.github.<user>.<repo>:<repo>:<tag>`，但本项目的 `:zevhub` 声明了自己的 `group = com.zevarc`，JitPack 尊重发布者的 group，实际路径落在 `com/github/zevarc/zevhub/v0.1.0/`，因此坐标是 `com.github.zevarc:zevhub`。写错的表现是依赖树里该模块 `FAILED`，而 `curl` 打 JitPack 路径却返回 200。

**如何确认可用**：删掉本地 `~/.m2/repository/com/zevarc` 后仍能构建，才算真的脱离本机。

```bash
./gradlew clean :app:assembleDebug   # 消费方，本地无任何 ZevHub 产物
```

**发版流程**：

```bash
./gradlew :zevhub:testDebugUnitTest   # 103 个测试
git tag -a v0.1.1 -m "..."            # 新版本必须换 tag
git push origin v0.1.1                # JitPack 自动构建
```

改 `gradle.properties` 的 `zevhub.version` 只影响 `publishToMavenLocal` 与本地产物，**不影响 JitPack**——JitPack 用 tag 名作为版本。

**已回退的尝试**：先用 `mavenLocal()` 打通链路（本机可用、免 token、改完立刻生效），但它只在本机有效，换机器或新 clone 就构建不了。已切 JitPack。

**仍然缺的：**

| 项 | 原因 |
|---|---|
| AAR 签名（`signing`） | Maven Central 强制；企业内仓库通常也要求。JitPack 不需要 |
| `binary-compatibility-validator` | 已发版，改公开 API 现在会**静默破坏** FastAir 等消费方 |
| 远端默认分支 | 远程目前只有 `dev`，GitHub 默认分支仍是初始的 `main`（空），需在网页上改 |

---

## 五、测试欠账

| 没测 | 规模 | 优先级 |
|---|---|---|
| `WifiP2pLink`（0 测试） | 378 行，全靠真机 | 高（需要 mock framework 骨架） |
| `TcpTransport` 背压 | 现有假链路 `UNLIMITED`，测不到 | 中 |
| `ZevHubImpl` 组网流程 | 依赖 P2P | 中 |
| 并发 `Stream.send` 会死会话说 | 值得写"验证错误用法" | 低 |
| `ExampleUnitTest`（`2 + 2 == 4`） | 模板残留 | 删 |

---

## 六、文档欠账

`docs/tutorial/` 已完成 12 篇（00–11）。缺：

| 文档 | 内容 |
|---|---|
| `README.md`（仓库根） | 这是什么、怎么加依赖、30 行示例 |
| 协议规范全文 | 现在只有散落在代码注释里的 `spec section N` 引用，**没有一份完整的 spec 文件**。这是最大的文档缺口：注释说"按 §15"，但 §15 在哪？ |
| 权限说明 | SDK 声明了 6 条权限，为什么、哪些可选 |

---

## 七、建议的推进顺序

每一步都以"可验证"结尾：

1. **补协议规范全文** —— 把代码注释里的 §N 收成一份 `docs/spec/wire-v1.md`，这是教程和实现共同的地基
2. **`Stream` 双档语义**（可靠档 / 实时档）—— 你要做的画面、音频、摄像头、远程控制四类场景，语义需求正好相反：控制指令不可丢，媒体帧必须可丢。现在的 `Stream` 只有"满了丢新帧"一种，拿它跑远程控制会丢按键
3. **阶段 1 契约层**（`:zevhub-codec`）—— 图片/流的真正落地
4. **`binary-compatibility-validator` + README + publish** —— 变成"能给别人用"的东西
5. **阶段 3 安全** —— 最后做，因为它需要设计评审

**ACK 决策已完成**（见 2.1）：心跳负责存活性，ACK 保持只发不收。

---

## 八、分层现状说明

`ByteTransport` 是可替换的（[04 篇](tutorial/04-transport.md)），但**"怎么发现对方、怎么组网"都在 `ZevHubImpl` 里**（依赖 `WifiP2pLink` + `TcpLink`）。

即：**抽象边界在当前链路之下，不在链路之上。** 写文档、讲课时这一点要讲准——不要说成"链路完全可替换"。
