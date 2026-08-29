# 竞品横评：Phira-MP 五种实现的代码实证对比与经验吸收清单

> 本文档基于 **2026-08** 对五个 Phira 联机服务端实现的源码实证审计（非 README 转述），
> 覆盖代码质量、架构、并发、协议编解码、鉴权、热路径、房间语义、玩家体验、运营面与供应链 12 个维度。
> 目的有二：① 作为 r0semi-mp 技术债审计（见 `tech-debt-audit.md`）的环境参照；② 沉淀"该从竞品学什么/不该学什么"的经验矩阵。
> 对等的五家全景画像（各家定位/强弱/选型指引）见 `server-comparison.md`；本文以 r0semi 为中心，聚焦吸收矩阵。

## 审计对象

| 项目 | 语言 | 规模 | 定位 |
|---|---|---|---|
| `phira-mp`（原版 TeamFlos） | Rust | ~3k 行（phira-mp-server 3003 行） | 官方参考实现 |
| `gooophira-mp` | Go | ~44k 行 + 15.4k 行测试（90 文件） | 功能最全 "全家桶" |
| `phira-mp-nodejsver` | TypeScript/Node | ~10k 行（46 文件） | 插件生态型 |
| `jphira-mp` | Java/netty | ~10.6k 行（128 文件） | MC 式插件生态 |
| `r0semi-mp` | Rust | ~21.6k 行（含测试，2026-08-29 复测） | 架构与内存极简派 |

## 维度 1：代码质量与测试

- **原版**：几乎无测试；鉴权每次 `reqwest::Client::new()` 现场建 HTTP 客户端（RSS 30-50MB 元凶）；挂起检测靠 `dangle_mark: Arc<()>` 强引用轮询（hack 手法）。
- **gooophira**：并发注释教科书级（锁序倒置规避直接写在 dispatch_play.go）；90 个测试文件分布均匀（roomlogic 20 / user 19 / dispatch 19 / room 18 / **protocol_hack 18** / proxyprotocol 13）。`half.go` 手写 bit-exact f16 转换，注释明确"与 Rust half crate 及 TS Float16Array 逐位一致"。
- **nodejsver**：移植态度认真，逐条标注原版源码行号（如 `Source: phira-mp-common/src/lib.rs:17-19`）；心跳参数注明推导依据。
- **jphira**：大量静态全局状态（`static SUSPENDED Map`、`static TIMER`）；Lombok + Bukkit 风格事件。
- **r0semi**：`forbid(unsafe_code)` 全 workspace、core 禁 `unwrap/expect`、契约 crate `missing_docs=deny`、clippy pedantic 全量 `-D warnings`；注释引用架构章节号（§4.9 等）。

## 维度 2：架构设计

- **原版**：session→room→user 直接互调，鉴权硬编码 URL，无扩展模型。
- **gooophira**：分层清晰但核心单体；扩展靠进程隔离 Agent（SQLite/webhook 外挂）+ 可选 Redis。
- **nodejsver**：domain/network/plugins 分层 + 插件注册表（权限/路由生命周期），插件生态最成熟（联邦组网/dashboard/锦标赛）。
- **jphira**：CancellableEvent + Listener 插件 API（JitPack 发布中）；协议在独立外部库 `jphira-mp-protocol`。
- **r0semi**：契约层 5-crate，依赖方向 CI 物理强制；`impl` 连 core 都不许认识；换实现 = 组合根换工厂 + 契约测试全绿。**无插件生态（显式非目标）**。

## 维度 3：并发处理（关键差距维度）

- **原版**：共享状态 + 各处 `RwLock` + AtomicBool；**广播循环内联 await 每个接收者**（慢客户端卡全房间）；无限速、无内存守卫、无队列上限。
- **gooophira**：每连接一 goroutine；两级锁（全局 `state.Mu` + 每房间 `room.Mu`），热路径只碰房间锁（"Touches/Judges 热路径仅持此锁，不同房间完全并行"）；用巧妙的异步 DisbandRoom 规避锁序死锁。发送通道 256 帧有界，满则异步踢人；`net.Buffers` 合并最多 64 帧批量写。
- **nodejsver**：单事件循环——无数据竞争但同步阻塞拖垮全场；写路径无背压处理，慢消费者内存静默膨胀；SIGTERM 处理器注释原文 *"In a real production app, you might want to shutdown gracefully here"*（Nothing done）。
- **jphira**：netty boss/worker 线程组 + ConcurrentHashMap + 房间内 lifecycleLock；调度器独立线程池。
- **r0semi**：每房间一个 actor + 有界 mpsc(1024)；actor 内 `&mut self` 串行零锁；队列压力三级分类（热路径 DropIfFull / 生命周期事实 Wait / 其余 Reject）。**按字节记账**：全局在途字节 64MiB 硬顶 + 每连接 8MiB + 已鉴权 1000 上限，三层平衡（charge/consume/Drop guard）。

## 维度 4：协议编解码与健壮性

| 防线 | 原版 | gooophira | nodejsver | jphira | r0semi |
|---|---|---|---|---|---|
| ULEB128 溢出守卫 | 🔴 payload 内 ULEB 无守卫（数组长度可触发 shift 溢出） | ❌ 无（Go 移位语义安全） | BigInt 安全 | 外部库 | ✅ `shift>=64 → Err(UlebOverflow)` |
| 帧上限 | ✅ 2MiB（帧头 pos>32 + len>2MiB） | ✅ 4MiB | ✅ 1MiB | 外部库 | ✅ 2MiB |
| **鉴权前降级** | ❌ 未鉴权可发 2MiB | ❌ 统一上限 | ❌ 统一 | ? | ✅ **4KiB pre-auth**（`PRE_AUTH_MAX_PACKET`） |
| 类型级约束 | 手动校验 | ParseRoomID 具名守卫 | 未集中 | 外部库 | ✅ `Varchar<32>/<200>` + `RoomId` charset |

r0semi 的两段式帧上限是全局唯一：`packet_limit: Arc<AtomicU32>` 初始 4KiB，鉴权成功后 `store(MAX_PACKET_SIZE)` 放开——**未鉴权连接的内存放大系数被限制在 1/500**。

## 维度 5：鉴权链路与顶号（安全关键）

- **r0semi**：新鉴权成功 = `authenticate_flow` 中 `registry.register`（epoch+1）；`handle_frame` 派发前校验 `current_epoch(user_id) == state.epoch`，不匹配拒绝 + force_close（ISSUE-0009 已修，有 stale_connection 回归测试）。
- **gooophira**：显式顶号——"重连顶号：踢出旧会话"（session_auth.go:211）；network_test 断言"old connection should be kicked when same user reconnects"。
- **nodejsver**：无顶号处理（未见）。
- **jphira**：suspend/resume 校验房间仍含该玩家。
- **原版**：用户对象还在即重挂 session，**旧 TCP 不死**——同 id 双活命令交织（ISSUE-0009 原始漏洞）。

## 维度 6：热路径（Touches）端到端

```
原版:handler→判live→tokio::spawn(每批次新任务)→broadcast_monitors→对每接收者await写
gooophira:handleTouches→room.Mu→游戏时间跟踪→观战聚合缓冲(自适应50/20/10ms)→批量合并写
r0semi:Touches入有界通道(DropIfFull)→actor串行→解析targets只投monitor→EncodeCache编码一次→直写
```

三家语义一致（都是"有观战者在场时"转发——`room.live |= monitor` 是客户端与服务端镜像的同一条语义），但 r0semi 的热路径零锁零分配；gooophira 的观战聚合缓冲是独有优化。

## 维度 7：房间语义与断线重连

- **原版**：`Weak<User>` + `strong_count>0` 清扫 → **幽灵座位**（ISSUE-0001 原始问题）；无重连窗口、无陈旧连接校验。
- **gooophira**：tagged union 状态最丰富（含 ReconnectNotified 防刷屏、StartedAt 记局时长）；DangleToken 指针身份校验 + 宽限期 + 房内播报倒计时；谱面匹配反作弊 + Played 重试静默幂等 + ready 强制倒计时（60s 到期 Aborted 未准备者，配 18 个测试）。
- **nodejsver**：房间元数据最全（密码/黑白名单/消息历史）；中途断线标记弃赛无恢复窗。
- **jphira**：**5 分钟挂起可恢复**（全场最长窗口）；标准状态模式类。
- **r0semi**：路由 miss 重放（3×20ms）防幽灵座位；session epoch 校验拒陈旧连接；`reconnect_window` 可配（默认 10s）；ISSUE-0010 CreateRoom 非幂等已处置（deployment.md §9 客户端指引）；WaitForReady 60s 强开倒计时已对照 gooophira 落地（B1）。

## 维度 8：玩家可见文案 i18n（原 r0semi 最短板，2026-08 已补）

- **原版**：Mozilla **Fluent** 三语（en-US/zh-CN/zh-TW），per-user LANGUAGE 作用域，报错键位齐全（`create-id-occupied`/`join-game-ongoing`/`join-cant-monitor`...）——与真客户端同源方案。
- **gooophira**：l10n 包（en-US.json/zh-CN.json），房间日志也走本地化键（`log-room-cycle`）。
- **jphira**：I18nService + lang 资源目录（zh-CN/en-US）。
- **nodejsver**：中文硬编码日志为主。
- **r0semi**：~~业务错误英文硬编码~~ **B2 已落地（2026-08）**——业务错误按用户 language 三语本地化（`l10n.rs`，对照原版 Fluent 键位，契约零变更）；welcome/maintenance 可配置保留；zh-TW 繁简校正对齐原版 Fluent。遗留 ISSUE-0013（EN Title Case）为产品决策点。

## 维度 9：HTTP 管理面

- **gooophira**：完整运营平台——ban user/room、broadcast、console 命令+日志流、contest 每房间配置、**OTP 双步管理员认证**、replay 配置、**runtime-config 带 rollback**、用户 move/disconnect、metrics。
- **r0semi**：~~`/rooms` + `/healthz` 极简主义~~ **管理 API 阶段 0–3.6.1 已落地（2026-08-28）**——只读观测（/admin/rooms?state=、单房详情、users、metrics）+ 写面系统命令（kick/ban/disconnect/broadcast，通道防竞态不用锁）+ Bearer 认证 + 审计环（持久化 audit.jsonl）+ runtime-config 一步回滚（跨重启可用）+ observer 热插拔（ban/anticheat）+ bans/config 快照持久化；刻意不做 OTP/Web GUI/WS console（取舍见 `admin-api.md`）。
- **nodejsver**：Web dashboard（默认关闭，`ADMIN_PHIRA_ID` 圈权限）。
- **jphira / 原版**：无。

## 维度 10：依赖供应链（本轮最震撼数字）

| 项目 | 依赖规模 |
|---|---|
| 原版 | **Cargo.lock 325 crates**（reqwest 拖动 hyper/tower/rustls 全家桶） |
| r0semi | **运行时 118 crates**（原版的 36%）+ 真 SDK conformance dev 树 85 包 = lock 203（无 reqwest） |
| gooophira | go.mod 直依 10 个（redis/sqlite/飞书 SDK/websocket） |
| nodejsver | 运行时直依仅 6 个（express/ws/js-yaml）出乎意料地瘦 |
| jphira | netty×4 + log4j2×5 + guava + caffeine + zstd-jni + 外部协议库 |

运行时 118 vs 325 不只是体积：**供应链审计面缩小到三分之一**（conformance 测试引入的 85 包 dev 树不进生产二进制）；且 r0semi 是五家中唯一把许可证/漏洞纳入 CI 闸门（cargo-deny）的。

## 维度 11：读侧洪水防御与半开连接治理

- **r0semi**：握手 + PROXY 前 `HANDSHAKE_TIMEOUT(5s)` 专杀"connect 后不发版本"僵尸；等首字节超时即拆。
- **gooophira**：完整 deadline 阶梯（proxyParse → handshake 10s → heartbeat），每阶段 `SetReadDeadline` 重置；model_state.go 周期 cleanup ticker。
- **nodejsver**：心跳三振制（30s ping + 10s 容忍 + 连续漏 3 次判死）+ "恢复正常"恢复日志（排障价值高）+ ECONNRESET 可疑活动上报遥测。
- **jphira**：netty ReadTimeoutHandler。
- **原版**：monitor 任务轮询 last_recv，无握手超时。

## 维度 12：真客户端兼容性（独有维度，见 client-conformance.md）

**gooophira / jphira 特有的 `protocol_hack.go`** 沉淀了生产环境换来的**真客户端怪癖补偿**——但那些怪癖的"必要性"应经开源客户端源码验证，而非直接信任。r0semi 已把该机会兑现（2026-08）：`phira-server/tests/conformance.rs` 以游戏客户端 Cargo.toml 锁定的同一 rev（cc822df）真 SDK（`phira-mp-client`）为对端，跑 client-behavior-review §5 的 A1–A6 崩溃猎手剧本——"怪癖传闻"升级为可执行一致性断言。详见 `client-conformance.md`。

## 经验吸收矩阵（该学什么 / 不该学什么）

> **状态回写（2026-08-29）**：下表多数"该学"项已吸收落地——B1（ready 60s 强开倒计时）、B2（错误 i18n 三语）、B6（观战聚合缓冲）、D2（版本握手校验）、谱面匹配反作弊（P1）、game_time 尾追加（ISSUE-0007）、runtime-config 一步回滚（管理 API 阶段 3）、bench 工具链进 CI（flamegraph workflow）、C1 拆分第一步（admin.rs/storage.rs 抽出）。两项被**明确拒绝**：OTP 双步（自建服过重，通道防竞态 + Bearer 替代——见 `admin-api.md` §0）、广播级消息按玩家语言本地化（encode-once 前提冲突）。另新增 gooophira/原版都没有的反作弊两翼：跨房 record 重放检测（P2）与成绩频率观测（R2）。剩余未做：deadline 阶梯、心跳恢复日志/ECONNRESET 遥测、对局中重连窗口延长、Played 幂等口径拍板。

### 🟢 该学（与 r0semi 架构正交，搬来不动骨架）

| 来源 | 学习项 | 解决自家债务 | 移植难度 |
|---|---|---|---|
| 原版 | Fluent 三语 i18n（lang 字段已在契约） | B2 错误文案英文 | 🟢 天级 |
| 原版 | game_time 进度哨兵（NEG_INFINITY 设计） | ISSUE-0007 | 🟢 |
| gooophira | 谱面匹配反作弊（record.Chart 校验） | 新能力 | 🟢 |
| gooophira | Played 重试静默幂等 | B2 相关误伤 | 🟢（需 ADR 决策 vs AlreadyUploaded） |
| gooophira | ready 强制倒计时（走 r0semi 的 Tick 插座） | B1 Tick 空壳 | 🟡 |
| gooophira | 观战聚合自适应缓冲 | 新能力 | 🟢 纯 server 层 |
| gooophira | deadline 阶梯（per-phase SetReadDeadline） | 半开连接治理最后一块 | 🟢 |
| gooophira | 版本握手校验（ver != protocolVersion 即断） | D2 | 🟢 五分钟 |
| nodejsver | 心跳恢复日志、ECONNRESET 可疑活动遥测 | 可观测性 | 🟢 |
| jphira | 重连窗口状态感知（对局中延长） | reconnect 体验 | 🟡 |
| jphira | netty pipeline 分阶段（拆 server.rs 蓝图） | C1 上帝文件 | 择机 |
| gooophira | OTP 管理认证 + runtime-config rollback | 运维安全 | 中期 |
| gooophira | bench 工具链进 CI | 防性能退化 | 🟢 |

### 🔴 不该学（动摇架构前提）

- Redis 多实例共享 / 联邦组网——单进程内存态是 r0semi 全部分析前提（未来多实例需求再立项）。
- 运行时插件系统——契约测试 + 整体替换已是更硬的替代品。
- 广播消息按玩家语言本地化——与 EncodeCache 的 encode-once 前提（同事件各目标字节相同）冲突；响应类（1对1）可本地化，广播类需语言中立方案。
- gooophira 的全局 `state.Mu` 两级锁——actor 模型下不存在该问题形态。

## 结论

- **Go 版赢在"现在"**（功能密度、生产实战怪癖知识）；**r0semi 赢在"以后"**（对抗性防御、字节记账、契约体系）。
- r0semi 的独特护城河不是某个机制，而是把"协议正确性可证明、内存可量化、可替换性可机器验证"三件事都做成了 CI 资产——见 `client-conformance.md` 提出的第四层资产。
