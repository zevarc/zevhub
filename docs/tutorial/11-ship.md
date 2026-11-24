# 11 · 发布 SDK：可见性、R8、版本兼容

> 对应代码：`zevhub/build.gradle.kts`、`app/build.gradle.kts`、`zevhub/src/main/keepRules/`、`zevhub/consumer-rules.keep`

## 现状：能出 AAR，但还不能给别人用

```kotlin
// zevhub/build.gradle.kts
plugins { alias(libs.plugins.android.library) }

android {
    namespace = "com.zevarc.zevhub"
    compileSdk { version = release(37) }
    defaultConfig {
        minSdk = 24
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
    testOptions { unitTests { isReturnDefaultValues = true } }
}

dependencies {
    api(libs.kotlinx.coroutines.core)     // ← api 不是 implementation
    testImplementation(libs.junit)
    testImplementation(libs.kotlinx.coroutines.test)
}
```

能缺的都缺：没有 `publishing`、没有版本号、没有 `explicitApi()`、没有二进制兼容性校验、没有 `consumer-rules.pro` 内容。这一篇讲每一项为什么必要、怎么加。

## 1. `api` vs `implementation`：协程必须 `api`

```kotlin
api(libs.kotlinx.coroutines.core)
```

因为公开 API 里到处都是 `Flow`、`suspend`：

```kotlin
fun discover(): Flow<Device>
suspend fun openStream(type: StreamType): Stream
```

如果写成 `implementation`，使用方**编译期拿不到 `Flow` 类型**，必须自己再加一条 `coroutines` 依赖。更糟的是版本不一致时会出 `NoSuchMethodError`。

**规则：出现在公开签名里的类型，依赖必须是 `api`。** 其余一律 `implementation`。

当前项目只有 coroutines 一个依赖，所以这条规则暂时只影响一行。但一旦加 `okio`、`protobuf` 之类，就得逐个判断。

## 2. `explicitApi()` 或者"靠人守 internal"

Kotlin 默认**所有声明都是 public**。所以"忘了写 `internal`"的后果是**泄漏成公开 API**，而你并不知道。

项目现在的做法是**约定 + 目录结构**：

```
ZevHub.kt / Session.kt / Stream.kt / Transfer.kt / IncomingXxx.kt    → public
ZevHubImpl.kt / SessionImpl.kt / ProtocolImpl.kt / WireCodec.kt      → internal
```

`internal` 在 Kotlin 里是**模块级**——整个 `:zevhub` 一个模块，所以 `internal` 对 `:app` 不可见。这点是对的。

**问题**：靠约定会漏。这类错误不是“未来风险”，而是**刚发生过**：本轮修改前有 11 个内部件忘写 `internal`（`FrameIdGenerator`、`ProtocolKind`、`ProtocolFrame`、`WireFrame`、`MessageCodec`/`RequestCodec`/`ResponseCodec`、`TcpLink`/`TcpTransport`、`WireCodec`/`WireDecoder`/`WireConstants`/`WireException`），全在公开 API 表面上。

修改是机械的：每个类加一个 `internal`。`wire` 那组做完后，公开表面从 35 个顶层声明降到 **24 个**，全部是承诺给上层的类型。

编译器当年就报了一个连带错误，值得注意：

```
e: ProtocolFrame.kt:4:5 'public' function exposes its 'internal' parameter type 'ProtocolKind'.
```

`ProtocolFrame` 是公开 `data class`，参数是 `ProtocolKind`。**把一个内化了，另一个就也必须是 internal**——这正好说明“泄漏”是有传染性的，不收拾就会一漏一大片。

但 `WireFrame` 这个**编译器不会报**，因为它只被同样是 public 的 `WireCodec`/`WireDecoder` 使用。**这才是最危险的一类：类型彼此“抱团”成为公开 API，编译器没有任何意见。**

**解法一：`explicitApi()`**

```kotlin
kotlin {
    explicitApi()      // 每个公开声明必须有可见性修饰符和明确返回类型
}
```

好处是**新增 public 变成必须显式决定**。代价是**现有代码会一次性报一堆错**（很多 `fun f() = expr` 需要改成 `fun f(): T`）。改造量大约是本篇成本的主体。

**解法二：`binary-compatibility-validator`**（JetBrains 官方插件）

```kotlin
plugins { id("org.jetbrains.kotlinx.binary-compatibility-validator") }
```

它把当前公开 API 导出一个 `api/zevhub.api` 文件，CI 里比较。**任何公开签名的改动都会让构建失败**，除非你显式更新那个文件——而更新它就是"我在改公开 API"的确认动作。

这两个不是二选一，是互补：`explicitApi` 管"不小心泄漏"，`validator` 管"不小心改坏"。

**先上 `validator`，再上 `explicitApi`。** 前者零改造就能加，后者需要一次清理。

## 3. R8 / ProGuard：两个文件，两种作用

```
zevhub/src/main/keepRules/    ← 打 AAR 时用（保护 SDK 自身不被裁掉）
zevhub/consumer-rules.keep     ← 使用方 app 打 release 时自动合并（保护 SDK 的反射/序列化）
```

当前两个文件都只有注释模板，**没有任何实际规则**。这现在是对的——因为 SDK 里没有反射、没有 JNI、没有依赖类名的逻辑。

**什么时候需要往里加：**

| 场景 | 加到哪 |
|---|---|
| 用反射读自己的类 | `keepRules/` |
| 上层需要继承/实现 SDK 的接口 | `consumer-rules.keep` |
| 用了 `kotlinx.serialization` | 它自带规则，不用写 |
| JNI 方法 | `keepRules/` + `-keepclasseswithmembernames` |

**SDK 作者最容易忽略的是 `consumer-rules.keep`。** 如果 SDK 里有任何"app 打 release 后被 R8 裁掉就崩"的东西（比如通过 `Class.forName` 找实现类），必须放这里，否则每个接入方都会踩同一个坑。

现在 SDK 的设计（纯接口 + 工厂）没有这个问题，**保持这样就别加**。

## 4. 发布配置

当前完全没有。要发布到 Maven 需要的（以 `maven-publish` 为例）：

```kotlin
plugins {
    alias(libs.plugins.android.library)
    `maven-publish`
}

android {
    defaultConfig {
        // 使用方从这里读 minSdk 等
    }
    publishing {
        singleVariant("release") {
            withSourcesJar()
        }
    }
}

afterEvaluate {
    publishing {
        publications {
            create<MavenPublication>("release") {
                from(components["release"])
                groupId = "com.zevarc"
                artifactId = "zevhub"
                version = "0.1.0"
            }
        }
    }
}
```

**`version` 必须有地方统一管理**，现在没有。建议放在 `gradle.properties`：

```properties
zevhub.version=0.1.0
zevhub.versionCode=1
```

因为将来可能有 `zevhub-codec`（图片/流的类型契约，见 [ROADMAP](../ROADMAP.md) 阶段 1），多模块版本必须一起升。

## 5. `minSdk = 24` 的代价：必须做 API 分支

```kotlin
defaultConfig { minSdk = 24 }
```

`minSdk` 24（Android 7.0）意味着**不能直接用高版本 API**。项目里已经到处在处理：

```kotlin
// app/MainActivity.kt
private fun requestPermissions() {
    val wanted = if (Build.VERSION.SDK_INT >= 33) {
        arrayOf(Manifest.permission.NEARBY_WIFI_DEVICES)
    } else {
        arrayOf(Manifest.permission.ACCESS_FINE_LOCATION)
    }
    ...
}
```

```xml
<!-- zevhub/src/main/AndroidManifest.xml -->
<uses-permission android:name="android.permission.NEARBY_WIFI_DEVICES"
    android:usesPermissionFlags="neverForLocation" />
<uses-permission android:name="android.permission.ACCESS_FINE_LOCATION"
    android:maxSdkVersion="32" />
```

```kotlin
// WifiP2pLink.kt
return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
    getParcelableExtra(name, T::class.java)
} else {
    @Suppress("DEPRECATION")
    getParcelableExtra(name)
}
```

**这些分支是 SDK 的固有成本。** 每次用新 API 都要问"24 支持吗"，不支持就得写两条路径。`minSdk` 每降一级成本显著上升——24 已经是"Wi-Fi P2P 相对稳定"的合理下限。

顺带一个坑：`ACCESS_FINE_LOCATION` 只在 `maxSdkVersion="32"` 下需要，因为 33+ 用 `NEARBY_WIFI_DEVICES`。但**很多国内 ROM 在 33 上仍然检查位置权限**，实际接入方经常两个都申请。这属于平台兼容性，不是 SDK 能解决的。

## 6. 清单合并：`<uses-permission>` 会自动传染

SDK 的 `AndroidManifest.xml` 里有 6 条权限：

```xml
<uses-permission android:name="android.permission.INTERNET" />
<uses-permission android:name="android.permission.ACCESS_NETWORK_STATE" />
<uses-permission android:name="android.permission.ACCESS_WIFI_STATE" />
<uses-permission android:name="android.permission.CHANGE_WIFI_STATE" />
<uses-permission android:name="android.permission.CHANGE_WIFI_MULTICAST_STATE" />
<uses-permission android:name="android.permission.NEARBY_WIFI_DEVICES" ... />
<uses-permission android:name="android.permission.ACCESS_FINE_LOCATION" android:maxSdkVersion="32" />
```

**这些会自动合并进使用方的 App**。好处是接入方不用手抄；代价是：

- 用户装你的 App 会看到"需要附近设备/位置"的权限清单
- 如果接入方只用 SDK 的一小部分（比如只想要文字传输），也背上了全部权限

有个技巧能缓解（**当前没用，但值得知道**）：

```xml
<uses-permission android:name="android.permission.ACCESS_FINE_LOCATION"
    tools:node="remove" />
```

接入方可以显式移除。但**别指望接入方知道**——所以 SDK 的 README 必须写清楚"我声明了哪些权限，为什么"。

## 7. 版本兼容性：三个必须想清楚的场景

| 变更 | 需要做什么 |
|---|---|
| 加一个新的 `Flow` 方法 | 版本号 patch，旧版本不支持也无害 |
| 改 `WireFrame` 的字段语义 | **协议 version 升级**，两端必须同时更新 |
| 改 `Session.request` 的签名 | 版本号 major，接入方要改代码 |

**协议版本和库版本是两条独立的号。** 现在 `WireConstants.VERSION = 1`，将来 SDK 0.2.0 可能仍然用协议 1。这是必须的——**库升级不该强制所有设备立即互不兼容**。

反向也不成立：如果协议升到 2，SDK 必须能同时说 1 和 2（或者明确拒绝 1），否则"一个人升了 App，另一个人没升"就完全连不上。当前的 `Handshake` 只支持一个版本：

```kotlin
require(hello.version == HandshakeCodec.PROTOCOL_VERSION) {
    "Unsupported protocol version: ${hello.version}"
}
```

**多版本共存是协议化之后第一件麻烦事**，别等到 v2 才想。见 [ROADMAP](../ROADMAP.md)。

## 8. 文档：SDK 的文档就是 API

有一条硬规则值得抄：**每个公开方法的 KDoc 必须写清楚三件事**：

1. 会不会挂起、挂多久（超时）
2. 失败怎么表达（抛 / 返回值 / 流结束）
3. 调用方的责任（比如"必须收集否则对方会卡住"）

项目里做得最好的例子（`Session.kt`）：

```kotlin
/**
 * Streams requests made by the peer. Answer each one through
 * [com.zevarc.zevhub.request.IncomingRequest.respond]; the flow must
 * be collected for the peer's requests to be processed.
 */
fun requests(): Flow<com.zevarc.zevhub.request.IncomingRequest>
```

**"the flow must be collected for the peer's requests to be processed"** ——这一句省掉了接入方半天的调试。

做得不够的地方（列在 [ROADMAP](../ROADMAP.md)）：

- `TransferSource.read` 没有写"必须响应取消"
- `Stream.send` 写了不能并发，但没写"违反会导致会话死亡"
- `Session.close()` 没写"pending request 会等到超时"（[06 篇](06-session.md) 提过这个缺陷）

## 本篇的落地顺序（如果给这个项目排）

1. **`binary-compatibility-validator`** —— 零改造，立刻防回归
2. **`version` 统一进 `gradle.properties`**
3. **补 KDoc 三要素**（尤其 `TransferSource`、`Stream.send`）
4. **`maven-publish` 配置 + 一个 `README.md`**（当前 SDK 连 README 都没有）
5. **`explicitApi()`** —— 放最后，因为要改一批现有签名

## 本篇自检

- 公开签名里的类型，依赖声明是 `api` 还是 `implementation`？
- 你怎么知道"这次改动破坏了公开 API"？靠 review 还是靠 CI？
- `minSdk` 的每个分支你都测了吗？只测最新系统等于没测下限。
- 你的协议版本和库版本是同一个号吗？同一个号会在第一次"库升级但协议没变"时出问题。
- 接入方读到你的 README，能知道要申请哪些权限吗？

---

回到 [目录](README.md) · 技术路线见 [ROADMAP](../ROADMAP.md)
