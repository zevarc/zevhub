# AGENTS.md

ZevHub：一个 Android P2P SDK，设计目标是**不依赖底层链路**，为上层提供文字、请求/响应、大块数据（图片/文件）、连续流四种传输语义。仓库含 SDK（`:zevhub`）和一个 Compose 调试 App（`:app`）。

## 验证命令

必须显式给两个环境变量，否则分别报 `Unable to locate a Java Runtime` 和 `SDK location not found`（`local.properties` 里的 `sdk.dir` 是别人机器上的路径，不可依赖）：

```bash
ANDROID_HOME=$HOME/Library/Android/sdk \
JAVA_HOME="/Applications/Android Studio.app/Contents/jbr/Contents/Home" \
./gradlew :zevhub:testDebugUnitTest
```

当前基线：**16 个测试类 / 103 个测试全绿**。已发布 tag `v0.1.0`。App 构建：`./gradlew :app:assembleDebug`。

发布：打 tag 推送到 `github.com/zevarc/zevhub`，JitPack 自动构建。消费坐标是 **`com.github.zevarc:zevhub:<tag>`**（不是常见的 `com.github.user.repo:repo`，因为本模块声明了自己的 `group = com.zevarc`）。本机调试也可用 `./gradlew :zevhub:publishToMavenLocal` 配 `mavenLocal()`，但那只在本机有效。

仓库没有 lint/ktlint 配置，单元测试是唯一的自动化验证手段。改动协议、会话、传输层后必须跑测试。

## 模块与分层

```
ZevHub           发现 / 连接 / 会话          ZevHub.kt
  Session        文字 / 请求 / 大块 / 流     Session.kt
    Protocol     帧 / ACK / 心跳 / 序号 / 致命错误  protocol/
      ByteTransport  纯字节流（可替换）      transport/
        WifiP2pLink + TcpLink                discovery/ tcp/
```

包职责：

| 包 | 职责 |
|---|---|
| `wire` | 36 字节帧头、1 MiB 上限、流式解码。**格式已冻结** |
| `transport` `tcp` | `ByteTransport` 抽象 + 阻塞 socket 的协程封装 |
| `protocol` | 组帧、MESSAGE 自动 ACK、心跳判死、per-context 序号校验、致命错误关闭 |
| `discovery` `handshake` `identity` | Wi-Fi P2P 双通道发现、UUID 身份、HELLO/AUTH/READY |
| `session` | 单 pump 分发所有上下文；request correlation；transfer/stream 路由 |
| `transfer` `stream` `message` `request` | 四种业务语义 |

## 硬规则

- **线格式已冻结，且 v0.1.0 已发版**：`wire/` 的 `MAGIC`/`VERSION`/字段布局、`ProtocolKind` 全部 15 个枚举值、各 codec 载荷格式、`WireException.CODE_*` 数值，都不能改。**新增一个 kind 就是协议变更**——旧版本收到会以 `Unsupported kind` 断连。发版后改任何公开 API 都会静默破坏消费方（FastAir 已依赖 v0.1.0）。
- **致命错误 = 关闭整条连接**，没有"跳过坏帧"的选项（长度不可信则边界不可信）。
- **单 pump 架构**：`SessionImpl` 的 pump 里任何挂起的 `send` 都会停摆所有上下文。新增分发一律优先 `trySend` 并明确满了怎么办。
- **失败的表达方式必须区分**：业务失败是返回值（`Response.Failure` / `TransferResult.Cancelled`），本地放弃是超时/取消，链路死亡才抛异常。
- 公开 API 只暴露 `ZevHub` / `Session` / `Stream` / `Transfer` / `IncomingXxx` / `Device`；实现、线格式、codec、链路实现一律 `internal`（当前公开表面 24 个顶层声明，别再加）。
- Android 平台调用尽量挤到边缘（`LocalIdentity.from(context)` 的 `persist` 参数化、`DiscoveryRegistry` 纯 JVM），否则无法单测。

## 文档

- `docs/tutorial/` —— 00–11 共 12 篇设计实现教程，每篇指向真实文件与测试
- `docs/ROADMAP.md` —— 能力现状、已知缺陷、分阶段路线。**改设计前先看这里**

## 已知缺陷（详见 ROADMAP）

- ACK 无消费者（只发不收）。决策已定：心跳负责存活性，ACK 不做重传，但它仍是死代码
- 重连逻辑 `ZevHubImpl.scheduleReconnect` 未测试（依赖真机 P2P）
- 重连后旧会话的上下文和未完成请求不续传
- transfer 路由用挂起 `send`，慢消费者会阻塞整个会话
- 握手签名为空，无加密
- `WifiP2pLink`（378 行）零测试覆盖
