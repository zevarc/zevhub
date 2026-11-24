# 从零设计一个 Android P2P SDK —— ZevHub 实现讲解

这套教程讲的不是"怎么用 ZevHub"，而是**怎么做出一套跟 ZevHub 一样的东西**：
在一堆 Android 设备之间，不依赖互联网、不依赖云服务，把文字、图片、连续流送过去，并且让上层调用者完全不用关心底下是 Wi-Fi P2P、还是普通 TCP、还是别的东西。

## 这套教程的原则

1. **只讲仓库里真实存在的代码。** 每篇都给出文件路径，你可以直接打开对照。
2. **问题先行。** 先讲"不这么做会死在哪里"，再讲实现。只讲实现的书，读者抄完就忘。
3. **每篇都有可跑的验证。** 指向 `zevhub/src/test/` 下已有的测试，不是伪代码。
4. **承认没做完的部分。** 每一篇末尾有"当前实现的窟窿"，比假装完备有用。

## 阅读前提

- Kotlin 语法、`suspend`/`Flow`/`Channel` 的基本用法（不懂协程的话，第 06 篇会卡住）
- 知道 TCP 是流、`read()` 可能一次只给你半个包
- 二进制格式、字节序（大端）知道个大概

不需要：写过 Wi-Fi P2P、写过协议栈、懂密码学。

## 目录

| 篇 | 主题 | 对应代码 |
|---|---|---|
| [00](00-why.md) | 为什么 Android 上"不依赖链路"很难 | — |
| [01](01-interface.md) | 先定 API，再定协议 | `ZevHub.kt` `Session.kt` |
| [02](02-wire-format.md) | 36 字节帧头：手写二进制格式的坑 | `wire/` |
| [03](03-streaming-decoder.md) | TCP 会粘包半包，所以必须流式解码 | `WireDecoder.kt` |
| [04](04-transport.md) | 把阻塞 socket 关进协程 | `transport/` `tcp/` |
| [05](05-framing.md) | 组帧层：ACK、序号、上下文的取舍 | `protocol/` |
| [06](06-session.md) | 一个 pump 管所有上下文 | `session/` |
| [07](07-discovery.md) | Wi-Fi P2P 的坑：双通道发现与组网 | `discovery/` `handshake/` |
| [08](08-transfer.md) | 大块数据：分块、背压、单边取消 | `transfer/` |
| [09](09-stream.md) | 连续流：序号校验与有界缓冲 | `stream/` |
| [10](10-testing.md) | 不用真机测 P2P：103 个测试怎么搭 | `src/test/` |
| [11](11-ship.md) | 发布 SDK：可见性、R8、版本兼容 | `build.gradle.kts` |

技术路线与欠账清单见 [../ROADMAP.md](../ROADMAP.md)。

## 跑起来

这个仓库**必须显式给两个环境变量**才能构建，否则会分别报
`Unable to locate a Java Runtime` 和 `SDK location not found`：

```bash
ANDROID_HOME=$HOME/Library/Android/sdk \
JAVA_HOME="/Applications/Android Studio.app/Contents/jbr/Contents/Home" \
./gradlew :zevhub:testDebugUnitTest
```

> `local.properties` 里的 `sdk.dir` 是别人机器上的路径，不要依赖它。

当前基线：**16 个测试类 / 103 个单元测试全绿**，`:app:assembleDebug` 可出包。

跑单个篇对应的测试：

```bash
./gradlew :zevhub:testDebugUnitTest --tests "com.zevarc.zevhub.wire.*"
./gradlew :zevhub:testDebugUnitTest --tests "com.zevarc.zevhub.transfer.*"
```

## 一套术语，先对齐

| 词 | 在本项目里指什么 |
|---|---|
| 帧 / frame | 一次 `WireCodec.encode` 出来的完整字节块：36 字节头 + 载荷 |
| 载荷 / payload | 帧头之后的应用数据，[0, 1 MiB] |
| 上下文 / context | 一条 Transfer 或一条 Stream 的身份，`Long`；路由靠它 |
| 关联 id / correlation | 一次 Request/Response 配对的身份，`Long`；只出现在这两种帧上 |
| 序号 / sequence | 一条上下文内 DATA 帧的递增编号，`Int`；只出现在 DATA 帧上 |
| 会话 / session | 两个设备之间握手成功后的一条协议通道 |
| 链路 / link | 会话之下提供字节流的通道（P2P、TCP……），接口是 `ByteTransport` |
