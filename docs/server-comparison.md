# Phira 联机服务端五实现全景对比

> 对象：`phira-mp`（原版）· `gooophira-mp`（Go）· `phira-mp-nodejsver`（TS/Node）· `jphira-mp`（Java/netty）· `r0semi-mp`（本仓库，Rust 重写）。
> 方法：2026-08-27 对五个本地副本的**源码实证审计**（每条关键结论附证据路径），非 README 转述；README 自述与代码不符处单独标注。
> **2026-08-29 复核更新**：四家对照副本未漂移（本地 HEAD 2026-05-31 ~ 2026-08-01，工作区干净），r0semi 一列数据全量重测并重写（行数/依赖/测试数/能力面均为当日实测，口径见 §7）。
> 姊妹篇：`competitor-review.md`（12 维度 + 经验吸收矩阵，以 r0semi 为中心的视角）；本文是对等的五家全景画像——各自是谁、强在哪、弱在哪、适合谁。

---

## 0. TL;DR

- **原版 phira-mp 是"协议圣典"**：3003 行把 wire 协议写得无可挑剔（derive 宏保证读写对称），但工程防护接近零——鉴权硬编码、模块成环互调、广播串行 await、无重连窗口。它是所有重写版共同的语义基准。
- **gooophira-mp 赢在"现在"**：功能密度五家第一——.phirarec 回放录制、SQLite 统计、Redis 共享、OTP 管理台、runtime-config rollback、飞书/Discord webhook、真客户端怪癖补偿库。代价是 4.4 万行 Go 单体 + 两级锁的复杂度，且作者自述"AI 赶工、不建议生产使用"。
- **phira-mp-nodejsver 赢在"门槛最低 + 溯源最认真"**：逐行标注原版源码行号，插件 SDK 类型独立发布，README 性能数据漂亮（自报未验证）。短板是单事件循环无背压，且 README 承诺的 8 个内置插件并不随仓库分发。
- **jphira-mp 赢在"Java 生态嵌入 + 5 分钟挂起恢复"**：netty pipeline 分阶段 handler 是教科书式结构，JitPack 一行引入当库用。短板是静态全局单例遍布与其"可嵌入"定位自相矛盾，且协议在外部库中不可见。
- **r0semi-mp 赢在"以后"，且"以后"正在到账（2026-08-29 复核）**：08-27 审计后的两天里，管理 API 全家桶（只读观测 + 写面干预 + Bearer 认证 + 审计环 + runtime-config 一步回滚 + observer 热插拔 + 管理事实持久化）、反作弊三件套（谱面匹配 / 跨房 record 重放检测 / 成绩频率观测）、CPU 优化线（读侧合读每帧 -49%）与真客户端 SDK 一致性测试（conformance.rs）全部落地；仍是唯一把"内存可量化（实测 RSS 4.3–5.2MB）、对抗性输入可证明（fuzz+守卫）、可替换性可机器验证（check-deps+契约测试）"三件事做成 CI 资产的实现。剩余显式非目标：回放录制 / Redis / 联邦 / Web 面板。

---

## 1. 五家一览

| | 原版 phira-mp | gooophira-mp | phira-mp-nodejsver | jphira-mp | r0semi-mp |
|---|---|---|---|---|---|
| 语言/运行时 | Rust/tokio | Go 1.26 | TypeScript/Node 18+ | Java 17(推21)/netty 4.2.3 | Rust/tokio (edition 2024) |
| 规模 | **3003 行**（server 1369 / common 812 / client 579 / macros 243） | **44171 行**（288 文件；测试 15407 行 ≈35%） | src **8683 行**（29 文件）+ test 1275 行 | main **8242 行**（105 文件）+ test 4876 行 | **21642 行**（api 2577 / core 4771 / impl-v1 1020 / contract 1847 / server 11427，含测试；08-29 实测） |
| 依赖面 | Cargo.lock **325 包**（reqwest 全家桶） | go.mod 直依 9 个（redis/sqlite/larksuite…） | 运行时直依仅 **6 个**（express/ws/js-yaml…） | netty×3 + log4j2×5 + guava/caffeine/orbit/zstd-jni + 外部协议库 | Cargo.lock **203 包**（= 运行时 118 包不变【原版的 36%】+ 真 SDK conformance dev 树 85 包；全 lock 无 reqwest，手写 HTTP/1.1） |
| 血统 | 本体 | 语义复刻（命令一一对应） | 逐行移植（源码行号注释） | 协议复刻在独立外部库 | 只复刻 common 协议语义，内部架构全新 |
| 测试 | 几乎无 | 90 文件 + 4 处 bench，CI `-race` | jest 16 文件，eslint 偏松 | 27 文件，JUnit5 + Mock 基建 | 242 测试函数（含契约/fuzz/压测/真 SDK conformance），6 道 CI 闸门 |
| CI | 无 | ci.yml：vet + test -race + 12 平台交叉编译 | test.yml：Node 18/20/22 矩阵 | test.yml + 自动 pre-release | fmt/clippy pedantic/check-deps(+ADR连续性)/test/cargo-deny/semver 六闸门 + nightly musl 产物发布 + flamegraph 采样 workflow（手动） |
| 运营面 | 无 | 完整（OTP/ban/runtime-config/console WS/web GUI） | dashboard 插件默认关（不随仓库分发） | 无 | `/rooms` `/healthz` + **`/admin/*` 全家桶**（只读观测/写面干预/Bearer 认证/审计环/runtime-config 回滚/observer 热插拔；无 OTP/Web GUI——刻意取舍） |
| 持久化 | 无 | SQLite 6 表 + Redis 缓存降级 | 封禁 IP/ID 持久化文件 | 无（内存态；回放 zstd 压缩存盘） | 管理事实文件持久化（bans/audit/config 快照，tmp+rename 原子写 + fail soft）；房间/会话态仍内存（关服清空是特性） |

---

## 2. 血统地图

```
phira-mp-common/src/command.rs ──── 308 行 = wire 协议唯一权威说明书
        │
   ┌────┼──────────────┬─────────────────┐
   │    │              │                 │
gooophira     nodejsver          jphira            r0semi
commands.go   Commands.ts        jphira-mp-protocol  phira-api/src/*
"16/20 条     "Source: command.rs external lib    non_exhaustive 枚举
 与原版一一对应"  :157-178"逐条溯源    (2.2.1, 本地未见)   + 契约测试钉死
```

四家衍生都指向同一权威：原版 `phira-mp-common`。区别在**忠实策略**——gooophira 和 nodejsver 把自己当"移植"，逐条对齐；jphira 把协议下沉外部库（本仓库审计不到）；r0semi 把协议升格为带契约测试的独立 crate（`phira-contract/src/rooms.rs` 里"任何实现必须通过"的用例集）。另外原版的 `phira-mp-client` crate 不是测试桩而是**真客户端集成用的同一套 SDK**（525 行，oneshot 回调 API + SRV 解析），这给所有实现提供了字节级对称参照——r0semi 已把它做成可执行一致性断言：`phira-server/tests/conformance.rs` 以游戏客户端 Cargo.toml 锁定的同一 rev（cc822df）真 SDK 为对端跑 A1–A6 崩溃猎手剧本（2026-08 落地）。

---

## 3. 各家深度画像

### 3.1 原版 phira-mp —— 轻装圣典，裸奔上生产

**架构**：session→room→user 双向互调成网，无边界可言——`session.rs` 的 `process()` 调 room 的五个方法，room.rs:193-224 反过来写 user 的锁并调 `user.try_send`，`User::dangle()` 同时操作 server 两张全局表和 room。并发模型是"共享状态 + 29 处细粒度锁"：全局 SafeMap/IdMap 两张 `RwLock<HashMap>`，Room 五把 RwLock + 三 AtomicBool，User 两个 RwLock + AtomicBool/Mutex，命令处理全程 `&self` 靠锁保护。

**热路径的两个硬伤**：广播 `room.rs:164-180` 对每个接收者 `try_send(cmd.clone()).await` **内联顺序等待**——一个慢客户端卡住整房间；TouchFrame/Judgement 每批 `tokio::spawn` 新任务（session.rs:395-417），高帧率下任务创建风暴。

**防护缺失清单**：`BinaryReader::uleb()` 无移位上限（bin.rs:44-55，debug 可 panic）；`array()` 用 `(0..self.uleb()?)` 触发 Vec 巨量预分配（bin.rs:21-23）；鉴权前连接可直接发 2MiB 包；token 校验无速率限制。挂起检测用 `dangle_mark: Arc<()>` 强引用计数轮询（session.rs:107-122 注释零解释的 hack）。

**鉴权**：`const HOST = "https://phira.5wyxi.com"`（session.rs:30）+ `/me` `/chart/{id}` `/record/{id}` 三处硬编码；认证路径每次 `reqwest::Client::new()`（session.rs:188），SelectChart/Played 用 `reqwest::get()` 便捷函数同样现场建客户端——TLS 元数据池缓存失效是 RSS 30–50MB 的元凶。

**顶号与重连**：同 id 重登只换 `Weak<Session>`（session.rs:209-215），**旧 TCP 连接不死**，继续读写直到自己的心跳超时自然枯萎（server.rs:79-88 用 `ptr_eq` 判断清理归属）——r0semi 的 ISSUE-0009（陈旧连接双活）正是从这继承的原始漏洞形态。

**i18n**：反倒是亮点之一——Mozilla Fluent 三语（en-US/zh-CN/zh-TW `.ftl` 编译期内嵌），per-user `task_local` LANGUAGE 作用域（session.rs:266-268），但键位只有 6 个。

**强在哪**：
1. `command.rs` 单文件就是完整协议规范，derive 宏 `#[derive(BinaryData)]` 保证读写实现天然对称——这是它能当"圣典"的根本原因；
2. `phira-mp-client` 是真客户端在用的集成 SDK，任何实现的最终兼容裁判；
3. 3000 行读得完全懂的参考语义，心跳常量（3s/2s/10s）、Fluent i18n、六值 Judgement repr(u8) 这些细节被全部四家继承。

### 3.2 gooophira-mp —— 生产怪癖知识库全家桶

**规模与自我定位**：Go 1.26，288 文件 44k 行，35% 是测试。README 特性清单覆盖 PROXY v1/v2、单 IP 限速、全局连接上限、`.phirarec` 回放录制、Agent 外挂进程隔离；结尾作者自白："这个项目是用 AI 赶工赶出来的……建议别在正式服务器上使用"（README 结尾原文）。

**架构与并发**：每连接 readLoop+writeLoop 两个 goroutine；发送通道 `sendCh=256` 满 256 帧→异步踢慢消费者；写侧用 `net.Buffers.WriteTo` 合并最多 64 帧一次 syscall。两级锁：全局 `ServerState.Mu`（model_state.go:30）+ 每房间分段锁（model_room.go:66-68 注释："Touches/Judges 热路径仅持此锁，不同房间完全并行"）；`isRoomOnlyCmd` 把 CmdTouches/CmdJudges/CmdPlayed 划为仅房间锁。但大多数其他命令仍走全局 state.Mu 串行，且锁序死锁风险靠纪律规避——dispatch_play.go:116-121 自述异步 DisbandRoom "否则 lock ordering inversion 自死锁"。

**独有资产一：真客户端怪癖补偿库** `protocol_hack.go`：① `forceSyncHost` 延迟对齐房主态；② `forceSyncInfo` 按 T/delay/2delay 时序补发"假观战者加入+离开"触发客户端回放录制、再用 SelectChart 伪装→真实状态两步修复房间状态机；③ 重连 WaitForReady 态同样伪装选谱再切回。默认补偿延迟 10ms（注释说明 jphira/netty 当年用 2ms，Go 无 setImmediate 故保守取 10ms）。这些是从生产环境换来的经验——但正如 `client-conformance.md` 所说，应经开源客户端源码验证而非直接信任。

**独有资产二：完整运营平台**：HTTP 管理 API（docs/API.md 全量文档）——admin rooms/users/metrics、用户 move/disconnect、ban/disband/broadcast、**runtime-config 带 rollback 快照**（model_state.go:92）、console 日志流 WebSocket、contest 每房间配置/白名单、replay 上传下载鉴权删除；**OTP 双步提权**（request ssid → CLI approve → verify 临时 token）；`web/` 目录是 Vite+React 离线控制台前端。

**独有资产三：持久化与外联生态**：SQLite 六表（users/matches/match_results/player_stats/chart_stats/consumed_events 幂等 DDL）；Redis 实例间共享 token/成绩缓存（TTL 6h，不可达自动降级本地 LRU）；webhook 四种载荷（generic/discord/onebot_v11/feishu）HMAC 签名 + 幂等账本重试；核心进程与 Agent 通过 unix socket/命名管道 IPC + outbox 模式隔离（Agent 离线不影响对局，查询返 503）。

**玩法语义 richest**：断线状态机 tagged union 三态 `StateSelectChart / StateWaitForReady{Started} / StatePlaying{Results,Aborted,ReconnectNotified,StartedAt}`；非对局 dangle 宽限 10s、对局走 `playing_reconnect_grace` 配置；ready 强制倒计时 60s 到期踢未准备者标 Aborted（10/5/3/2/1s 播报）；Played 重试静默幂等；顶号显式踢旧会话并发 logged-in-elsewhere；谱面匹配轻量校验（record.Chart 缺失时 fail-open 放行——反作弊诚实地说只做了一半）；观战聚合自适应缓冲（积压 <50→50ms、>200→20ms、紧急 10ms flush，monitor_buffer.go:17-23）。

**协议防线**：帧 = LEB128(u32) 长度前缀，会话层 maxFrameSize 4MiB（协议默认负载 2MB）；版本字节 !=1 即断开（比 nodejsver 的 warn-only 严格）；心跳阶梯 3000/2000/10000ms 每次读前 SetReadDeadline；PROXY v1/v2 解析超时 1s。命令集 16 client/20 server 与原版一一对应，扩展全在 HTTP/IPC 侧不动 TCP 协议——克制得好。

**弱在哪**：全局 state.Mu 仍是多数命令的瓶颈路径；锁序问题要靠注释和测试守护；无 golangci-lint（只有 vet）；反作弊 fail-open；作者自己都不建议生产使用。

### 3.3 phira-mp-nodejsver —— 最认真的移植与最大的承诺落差

**规模与技术栈**：v0.6.2，TypeScript strict，src 29 文件 8683 行；运行时依赖只有 express/ws/cookie-parser/express-session/dotenv/js-yaml 六个（出乎意料地瘦），dev 侧 typescript 5.9 + jest 30 + @yao-pkg/pkg 四平台单文件打包。domain（auth/protocol/rooms）/network/plugins 三层清晰。

**最大优点：溯源纪律**。协议代码逐条标注原版行号——Commands.ts:37 `// Source: phira-mp-common/src/command.rs:157-178`、TcpServer.ts:19-20 标注 lib.rs/session.rs 双来源、Rust Result 映射 `{ok:true,value}|{ok:false,error}` 也注明出处。这让它的协议可信度在文档层面是五家最高的。

**功能亮点**：心跳三振制（30s ping / 最后消息 40s 阈值 / 连漏 3 次 + 5s 巡检，TcpServer.ts:23-26 注释推导过程）；ECONNRESET/ECONNABORTED 计入可疑活动遥测，按真实 IP 5 分钟窗口 ≥10 次自动 banIp 7 天（回环只告警）；50 连接/IP DoS 限流；Proxy Protocol v2 手写解析含签名段；断线弃赛标记 isFinished + 0 分成绩 + 广播 Abort；房间有 password/blacklist/whitelist 字段和 join 检查、50 条消息环形缓冲。

**致命落差一：无背压**。`socket.write()` 不检查 drain/highWaterMark（TcpServer.ts:505-518），收包 `Buffer.concat([state.buffer, data])` 无界累积（:177）——单事件循环下一个慢消费者就能拖动全场内存；同步 `readFileSync/writeFileSync` 在插件配置读取路径上会阻塞事件循环。

**致命落差二：README 承诺 vs 仓库交付**。README 列出的 federation/dashboard/tournament/titles 等 8 个"Built-in Plugins"均不在仓库（plugins/ 只有一个问候语 example）；HttpServer 本体只有 `/api/version` 和 `/api/status` 两个路由，README 的 admin API 全部依赖不存在的 web-dashboard 插件；密码房字段存在但核心 TCP 协议里 CreateRoom 只有 id 参数，密码判定通路未接线；消息历史只存不回放；docs/STRUCTURE.md 还在描述已删除的目录。协议版本字节不匹配仅 warn 不拒。

**其余风险**：SIGTERM 有优雅关闭（await app.stop 后 exit 0，比 competitor-review 早先记录的要好），但 uncaughtException 只打日志继续跑（注释自认 production 上值得商榷）；官方 API URL 在 config 与 handlers/auth.ts 内嵌两处硬编码并存；中文文案硬编码不可切换（connectionId 都是 `连接-${Date.now()}` 格式）。

**强在哪**：入门部署成本五家最低（npm ci 即跑 + pkg 打包 exe）；插件 SDK 独立仓库持续同步（sync-plugin-sdk.yml）；路由生命周期治理（Express layer 跟踪、卸载时摘除、unload 后 503 兜底）比多数动态语言框架做得讲究；同 id 重连做房间内连接迁移。

### 3.4 jphira-mp —— MC 式插件生态与静态单例的矛盾体

**定位**：README 自述"为性能与扩展性的平衡而生"，部署仿 Minecraft 服务端（`java -jar --port --plugin --proxy-protocol --language`，控制台 `stop` 关服）；JitPack 发布坐标可用，插件示例在独立仓库，插件系统还在 PluginSystem-Prototype 项目重构中（自述处于过渡态）。协议在外部库 `jphira-mp-protocol:2.2.1`（本地未克隆，帧上限等细节审计不到）。

**架构质量其实很高**：netty pipeline 分阶段 `AuthenticateHandler→PlayHandler→RoomHandler` 状态机式切换（握手期 HandshakeDecoder 协商后换业务 pipeline，ServerChannelInitializer.java:61-85）+ `ReadTimeoutHandler(5s)`；28 个事件类覆盖 create/join/leave/host/state/suspend 全流程 Pre/Post 对；`CancellableEvent + orbit EventBus` lambda 注册 + MiniInjector `@Inject`——插件 API 的形态设计在五家里最接近正统（类 Bukkit）。

**并发模型**：boss 1 + worker 核数线程；`LocalRoom.lifecycleLock` synchronized 只保护 join/leave/getView 快照一致性，广播与状态迁移锁外做；玩家/房间解析用 `ConcurrentHashMap.compute` 原子 get-or-create/resume；还探测 Java 21 虚拟线程决定用 `newVirtualThreadPerTaskExecutor` 还是固定池（ThreadFactoryCompat.java:29-91）。

**招牌功能：5 分钟挂起/恢复**（全场最长窗口）：断线入 SUSPENDED 表并 schedule 强制离房，resume 校验房间仍含该玩家否则 ResumeFailedException；顶号拒新会话并给旧连接发 logged_in_elsewhere。i18n 走 I18nService（zh-CN/en-US 资源目录，缺语言回退 zh-CN）。zstd-jni 压缩回放记录。

**弱在哪**：静态全局单例遍布（SUSPENDED/TIMER/PLAYERS/ROOMS/INSTANCE 全 static，事件走 `Server.postEvent` 静态门面）与其"作为库引入"的 JitPack 定位直接矛盾——多实例嵌入必互相污染；token 明文进 info 日志（AuthenticateHandler.java:47）；TIMER 硬编码单线程调度池不可关停；测试偏浅层单元（MockPhiraServer 存在但不跑真实网络 pipeline）；文档几乎只有 README。

### 3.5 r0semi-mp —— 把正确性变成 CI 资产

**架构（五家唯一"契约分层"）**：`phira-api`（货架规格：non_exhaustive 枚举 + trait，**零 tokio**，deps 仅 thiserror/half/async-trait——已验证 crates/phira-api/Cargo.toml）← `phira-core`（柜台：总线/会话/生命周期，禁 unwrap/expect）← `impl-rooms-v1`（货物，**连 core 都不许认识**）← `phira-server`（老板/组合根，唯一认识所有人）。依赖方向由 `tools/check-deps.py` ALLOW 表物理强制进 CI；换实现 = 组合根换工厂 + `phira-contract/src/rooms.rs` 契约测试全绿。11 个 ADR 沉淀每个决策（0001 actor 并发 … 0011 事件插座）。组合根拆分（C1）已启动：admin.rs / storage.rs 自 server.rs 上帝文件抽出（2026-08-28）。

**并发宪法**：每房间一个 actor + 有界 mpsc(1024) 串行 + actor 内 `&mut self` 零锁；队列压力三级分类——热路径 DropIfFull（丢新保活）、生命周期事实 Wait、其余 Reject（ADR-0005）；时间/连接事实全部命令化（Tick/UserDisconnected/UserDangleExpired），生命周期任务单一生产者派发，impl 层禁止开后台任务。热路径编码一次：EncodeCache 帧 Arc 指针缓存（条目钉住源 Arc 防 ABA，ADR-0009）+ Outbound::Encoded 直写。08-27 审计后 CPU 优化线三项落地：写批处理（`recv_many` 攒 64 帧一次 `write_all`）、Metrics 热路径无锁化（Touches/Judges 单原子计数，锁帧归零）、读侧合读（4KiB pending 缓冲 + 游标消费，A/B 对拍每帧 CPU 66.1→33.4µs，**-49%**）。

**资源红线写成常量并可验证**：全局在途字节 `MEMORY_GUARD_LIMIT = 64MiB`（server.rs:761）+ 每连接 sendq `PER_CONN_MEM_LIMIT = 8MiB`（:764，超限踢）+ 已鉴权连接上限 1000，三层 charge/consume/Drop-guard 记账平衡（tests/memory_guard.rs 盯着）；**两段式帧上限**是五家唯一：`PRE_AUTH_MAX_PACKET=4KiB`（stream.rs:83）鉴权通过才 `store(MAX_PACKET_SIZE=2MiB)`（server.rs:1709）——未鉴权内存放大系数限到 1/500。读侧 payload 窗口亦已入账（ReadCharge → 全局 64MiB 闸门，Drop guard 兜底任何退出路径）——账外区域闭合，"声明 2MiB 帧"洪水在读路径即被闸住。

**输入防御纵深**：ULEB 移位 ≥64 直接 Err；类型级约束 `Varchar<32>/<200>` + RoomId 字符白名单；双层 fuzz（解码器 proptest 固定种子 + fuzz_frames 真 TCP 垃圾流）；读侧合读的 pending 缓冲上界即 4KiB 读缓冲（不随输入增长），垃圾流下内存恒定；压测 harness 入库（tests/pressure.rs，回环 1500 连接 ~1.5–2.3Gbps 0 panic 0 内存膨胀，负载 29@2核暴露 CPU 小包处理为真实瓶颈——ARCHITECTURE §10.1.1）。

**鉴权链路**：手写 HTTP/1.1 GET（http:// 明文供 Oracle 环境 / https:// rustls 单栈 ring+webpki-roots），TLS 配置进程级 OnceLock 单例（对比原版每请求新建 reqwest Client）；回源 5s 超时；token 先过 CR/LF 净化再拼 Authorization 头（http.rs "Never Trust the Client"）——鉴权上游同为 `https://phira.5wyxi.com` 但基址集中配置。08-28 加固两连：302 有限跟随（同 host 白名单 + 3 跳上限，显式拒绝 30x）、响应体 16MiB 上限。

**重连/顶号语义**：session epoch——新鉴权 register 即 epoch+1，派发前校验 `current_epoch(user_id)==state.epoch` 否则拒绝 + force_close（ISSUE-0009 已修，tests/stale_connection.rs 回归）；路由 miss 重放 3×20ms 防幽灵座位（ADR-0007）；`reconnect_window` 可配（默认 10s，config.rs:58）；贵命令每连接限速 CreateRoom 1/s、JoinRoom/SelectChart/Played 5/s 超限 TooManyRequests（ADR-0008）。近期已落：Tick 通电的 WaitForReady 60s 强开倒计时（对照 gooophira 语义）+ 业务错误按用户 language 本地化（对照原版 Fluent 方案，契约零变更）——competitor-review 中"B1 Tick 空壳/i18n 英文硬编码"两条短板已成历史。

**管理 API 与持久化（08-27 审计后落地，阶段 0–3.6.1 全绿）**：三职责域 × 三条既有通道——只读观测（`/admin/rooms?state=` 过滤、单房详情、`/admin/users`、`/admin/metrics`）走快照零风险；写面干预（AdminKick/AdminBan/AdminBroadcast/AdminDisconnect 系统命令族，管理动作排队进房间 actor——**通道防竞态，不用锁**）+ 静态 **Bearer 认证**（默认 loopback 绑定）+ 有界审计环（256，持久化 audit.jsonl）；配置域 runtime-config 热更 + **一步 rollback**（跨重启可用，二次回滚 409）+ observer 热插拔（ban / anticheat）。持久化只做"**管理事实**"（bans.json / audit.jsonl / config.current.json / config.last.json，组合根 storage.rs 独占）：tmp+rename 原子写、fail soft、契约/core/impl 零感知；房间/会话态仍内存——"关服清空"是显式特性。对照 gooophira 的刻意取舍：不做 OTP 双步（自建服过重）、不做 Web GUI（面板 = 纯 API 消费方，阶段 4）、不做 WS console（gooophira 的复杂度来源）。

**反作弊三件套（Moderator 契约插座 §7.3 兑现）**：P1 谱面匹配——Record.chart 数据口 + 回注点谱面校验（fail-open 口径与 gooophira 一致）；P2 **AntiCheatObserver 跨房 record 重放检测**——第二个真实 Moderator，同一 record 跨房重投被 Moderated 拒绝（端到端测试钉死），热插拔 `kind=anticheat` + `/admin/anticheat` 读面；R2 成绩频率观测（on_event 面首个真用途）：60s 窗口 ≥10 局 → `high_frequency` flag，纯观测不自动拦。gooophira 的"反作弊诚实地说只做了一半"（谱面 fail-open）在 r0semi 补上了重放检测与频率观测两翼，且观察者接口被第二个实例再次定形。

**真客户端一致性断言（维度 12 兑现）**：`phira-server/tests/conformance.rs` 以**真客户端在用的同一 SDK**（`phira-mp-client`，rev cc822df 与 Phira 游戏客户端 Cargo.toml 锁定对齐）为对端，跑 client-behavior-review §5 的 A1–A6 剧本——"服务端多说话会不会炸客户端"从推理变成测试。dev-dependency 引入 85 包（hickory SRV 解析/moka/icu 等），生产二进制零新增，全 lock 仍无 reqwest；cargo-deny 已加 dev-only hickory 漏洞豁免。

**实测成绩**：量产稳态 RSS **4.3–5.2MB / 峰值 4.6–5.4MB**（低于 7–15MB 预算下界）不变；新增 CPU 维度可复核数据——bench_broadcast 入库（300 客户端 4739 帧/s 全额节奏）+ flamegraph CI workflow（Linux perf 权威复核，collapsed 栈 artifact 可数值对比）+ 同帧率 A/B 对拍：读侧合读使**每帧 CPU 成本 66.1→33.4µs（-49%）**。Cargo.lock 203 包（运行时 118 = 原版 36%），仍是五家唯一把许可证/漏洞审计（cargo-deny）与契约 semver（cargo-semver-checks）放进 CI 闸门的（另加 nightly musl 静态产物自动发布）。

**弱在哪（诚实清单，2026-08-29 复核）**：无回放录制、无 Redis/联邦、无 Web 面板（管理 API 已备，面板属阶段 4，前端刻意不进仓库）；房间态不持久化（关服清空是显式特性）；单人项目总线风险；CPU 优化线已收官（读合读/写批处理/无锁化），剩余为架构级约束（tokio 单 IO driver；记账 SeqCst 属安全锁 A 边界维持不动）；为 encode-once 放弃广播级多语言内容的取舍仍在。真客户端一致性断言已从"在建设中"变为已落地（conformance.rs 真 SDK 剧本）；观战聚合缓冲已对照 gooophira MonitorBuffer 落地（commit fef36a1，B6）；"无管理台/无持久化"两条已从清单移除（管理面阶段 0–3.6.1 + 管理事实持久化落地）。

---

## 4. 八个专题横向对比

### T1 并发模型谱系

| 实现 | 模型 | 锁/共享点 | 广播路径 | 慢消费者处置 |
|---|---|---|---|---|
| 原版 | tokio 任务 + 共享状态 &self | 29 处 RwLock/Atomic 细粒度竞逐 | 逐接收者 clone + **内联 await** | 无处置（整房间陪等）|
| gooophira | goroutine ×2/连接 + 两级锁 | 全局 state.Mu + 房间 Mu 分段 | 房间锁内聚合 + 自适应缓冲缓冲 flush | sendCh 256 满则异步踢 |
| nodejsver | 单事件循环 | 无锁无竞争也无并行 | socket.write 直发 | **无背压**（静默膨胀）|
| jphira | netty 多线程 + 静态全局 | lifecycleLock（仅成员快照）+ CHM.compute | broadcastToMonitors 独立通道，锁外 | ReadTimeoutHandler |
| r0semi | 房间 actor + mpsc 串行 | 零锁（&mut self） | targets 由 impl 计算，EncodeCache 编码一次直写 | DropIfFull 保活 + 8MiB 队列顶 + kicker |

### T2 输入防御纵深（对着恶意字节流的底线）

| 防线 | 原版 | gooophira | nodejsver | jphira | r0semi |
|---|---|---|---|---|---|
| ULEB128 溢出守卫 | ❌ 可 shift 溢出 | ✅ Go 安全语义 | BigInt（读端有 shift>32 校验） | 外部库（不可见） | ✅ shift≥64 → Err |
| 帧长上限 | 2MiB | 4MiB | 1MiB | 外部库（不可见） | 2MiB |
| 鉴权前降级 | ❌ 未鉴权即 2MiB | ❌ 统一 | ❌ 统一 | ? | ✅ **4KiB pre-auth** |
| 类型级输入约束 | Varchar<20> 房间号白名单 | ParseRoomID 具名守卫 | 部分 | 外部库 | ✅ Varchar<32>/<200> + RoomId charset |
| 模糊测试 | 无 | 无专项（有 framing bench） | protocol 相关测试 | 无 e2e pipeline 测试 | ✅ 解码器 fuzz + 真 TCP 垃圾流 |
| 版本字节不匹配 | 仅记录不校验（log-only，任何字节都收） | 断开（ver != 1 即断） | ⚠️ 仅 warn | 仅记录不校验（log-only，外部库） | ✅ 拒绝不匹配（D2 已落地，回归测试） |
| 内存兜底 | 无 | 连接上限+IP 限速 | 50 conn/IP 限流 | ReadTimeoutHandler | ✅ 64MiB 全局字节账本 + 8MiB/conn + 1000 已鉴权上限 |

### T3 断线-重连-顶号

| 实现 | 掉线窗口 | 陈旧连接校验 | 顶号 |
|---|---|---|---|
| 原版 | 无（掉线即离开流程） | ❌ 同 id 双活至旧连接自然死亡 | 软顶号（僵尸共存） |
| gooophira | 非对局 dangle 10s / 对局 grace 配置；ReconnectNotified 防刷屏 | DangleToken 指针身份 | ✅ 踢旧 + logged-in-elsewhere |
| nodejsver | 无窗口，Playing 弃赛计 0 分 | 房间内连接迁移或踢旧线 | 部分（迁移优先） |
| jphira | **5 分钟挂起**（resume 校验房间仍含该玩家） | suspend/resume 配对 | ✅ 拒新会话？否——旧连接收 logged_in_elsewhere |
| r0semi | reconnect_window 默认 10s 可配 + miss 重放 3×20ms 防幽灵座位 | ✅ epoch 校验 + force_close（回归测试） | ✅ epoch+1 使旧 epoch 全部失效 |

### T4 鉴权链路（全都指向上游，姿势各异）

| 实现 | 上游调用方式 | 缓存/重试 | 加固 |
|---|---|---|---|
| 原版 | 每次 `reqwest::Client::new()`，HOST 硬编码三处 | 无 | token 限长 32 |
| gooophira | FetchUserInfo 抽象接口 + integration 实现，端点可配默认同源 | token TTL 6h + Redis 共享 | 500ms 线性退避重试 |
| nodejsver | GET /me Bearer（config 与 auth.ts 双处 URL 硬编码） | 无明显缓存 | stress_ 虚拟 token 仅非生产 |
| jphira | PhiraFetcher caffeine 4 缓存 | 结果 10min/万条 | ⚠️ token 明文进日志；插件事件可在上游查询前短路注入 |
| r0semi | 手写 HTTP/1.1 + rustls 单例配置，基址集中 | 组合根 trait 可替换（AuthHandler） | token CR/LF 净化 + 5s 超时 + 契约层 AuthOutcome + 302 有限跟随（同 host 白名单 3 跳）+ 响应体 16MiB 上限 |

### T5 i18n

| 实现 | 方案 | 语言 | 备注 |
|---|---|---|---|
| 原版 | Mozilla Fluent 三包编译期内嵌，per-user task_local | en-US/zh-CN/zh-TW | 键位 6 个但是标准方案源头 |
| gooophira | l10n JSON 包（连房间日志键 `log-room-cycle` 都本地化） | en-US/zh-CN | |
| nodejsver | 中文硬编码日志/公告 | — | connectionId 格式都是中文 |
| jphira | I18nService + lang 资源目录 + --language 启动参数 | zh-CN/en-US，缺省回退 zh-CN | |
| r0semi | 错误文案按用户 language 本地化（B2，契约零变更）；welcome/maintenance 可配 | 对照 Fluent 键位设计 | 曾是最短板，已补 |

### T6 功能面（“能给真人玩的东西”）

| 功能 | 原版 | gooophira | nodejsver | jphira | r0semi |
|---|---|---|---|---|---|
| 开房/选图/准备/游玩/观战 | ✅ | ✅ | ✅ | ✅ | ✅ |
| ready 强制倒计时 | ❌ | ✅ 60s + 播报 + Aborted | 控制台 fstart 命令 | ✅（状态机） | ✅ Tick 60s 强开（对齐 gooophira 语义） |
| 回放录制 | ❌ | ✅ .phirarec + 上传/下载站 | ❌ | ✅ zstd 存储 | ❌ |
| 密码/黑白名单房 | ❌ | banroom/kick preserve | 字段有、TCP 通路未见 | 锁房 + host 整体开关 | ❌（AdminBan 用户封禁已落地；密码/黑白名单房仍不在范围） |
| 重试幂等 Played | ❌ | ✅ 静默成功 | ❌ | ? | PartOf AlreadyUploaded 语义留档（ISSUE 口径） |
| 反作弊 | ❌ | ⚠️ 谱面匹配 fail-open（自述半套） | ❌ | ❌ | ✅ 谱面匹配 + 跨房 record 重放检测 + 成绩频率观测（Moderator 插座 + 热插拔 + /admin/anticheat） |
| 管理 HTTP 面 | ❌ | ✅ 全家桶 + OTP + runtime-config + Web GUI | 依赖未分发插件 | ❌ | ✅ /admin/* 全家桶（观测+干预+热更回滚+审计持久化；Bearer，无 OTP/Web GUI——刻意） |
| 持久化 | ❌ | SQLite+Redis | 封禁持久化 | 内存态 | 管理事实文件持久化（bans/audit/config 快照）；房间态内存（显式特性） |

### T7 工程纪律（CI 会拦什么）

| 实现 | lint 强度 | 供应链审计 | 契约/兼容性盯防 | 专项工具链 |
|---|---|---|---|---|
| 原版 | 无 | 无 | 无 | derive 宏 |
| gooophira | vet only（无 golangci-lint） | go.mod 存档 | 无 | bench 三阶段进 CI、12 平台交叉编译、-race |
| nodejsver | eslint9 flat（no-explicit-any 仅 warn） | npm audit 未接 CI | 无 | jest 18/20/22 矩阵、pkg 打包 |
| jphira | 无 lint 闸门 | 无 | 无 | JitPack/shadowJar 发布 |
| r0semi | clippy **pedantic 全量 -D warnings** + forbid(unsafe) + api missing_docs=deny | ✅ cargo-deny 许可+漏洞 | ✅ cargo-semver-checks + 契约测试 crate | check-deps 依赖方向 + check-adr 编号连续性 + fuzz + 压测/基准入库 + 真客户端 SDK conformance + flamegraph 采样 workflow（手动） |

### T8 性能与资源（有实测的只有一家，且已从"确认瓶颈"走到"完成优化"）

r0semi 是唯一公开可复核运行时数据的，08-27 审计后数据面进一步加厚：
- **内存**：RSS 稳态 4.3–5.2MB / 峰值 4.6–5.4MB（ARCHITECTURE §10.1.1），回环 1500 连接灌流 ~1.5–2.3Gbps、0 panic、RSS 不膨胀——不变；
- **CPU**：瓶颈确认为小包处理后**优化线收官**——写批处理（64 帧一次 write_all）→ Metrics 热路径无锁化（锁帧归零、lock_contended -38%）→ 读侧合读（同帧率 A/B 对拍每帧 CPU **66.1→33.4µs，-49%**）；基准与工具链全部入库（bench_broadcast 300 客户端 4739 帧/s 全额节奏 + flamegraph workflow 的 Linux perf 权威复核与 collapsed 栈 artifact）；剩余为架构级约束（tokio 单 IO driver）。

nodejsver README 自报 5000 并发零错误/连接 TPS 620——**自报口径，本审计未复现**。goophira/jphira/原版无公开数据；gooophira 有 bench 工具链但结果未固化文档。原版 RSS 30–50MB 来自媒体池分析（reqwest/rustls 元凶），属于结构性推断。

---

## 5. r0semi 到底哪里不同（六条差异点）

1. **分层不是愿望，是 CI 产物**。别家"分层清晰"靠自觉（gooophira 单体内聚、nodejsver 目录约定、jphira 包结构），r0semi 的依赖方向被 `check-deps.py` ALLOW 表物理拦截，impl 连 core 都不 import。"子系统可整体替换"因此可机器验证。
2. **内存是被记账的资源，不是碰运气的副作用**。64MiB 全局在途字节账本 + 每连接 8MiB 队列顶 + 1000 已鉴权上限 + 4KiB 鉴权前降级，四道护栏带平衡性测试——五家中唯一按**字节**而非按**帧数**管内存的；读路径 payload 窗口也已入账（ReadCharge + Drop guard 兜底），账外区域闭合。
3. **时间事实命令化**。goroutine ticker（gooophira）、setInterval 巡检（nodejsver）、ScheduledExecutorService（jphira）都是"各处顺手开定时器"；r0semi 禁止 impl 层拥有时钟，Tick/超时/断线全部经单一生产者进入队列——确定性来自宪法而不来自 review。
4. **广播热路径编码一次**。原版逐人 clone await；gooophira 聚合缓冲是"减少 syscall"仍多次编码；r0semi EncodeCache 让同一事件对所有目标只编码一次、Arc 直写——这是丢掉 per-user 内容定制（如广播级 i18n）换来的，取舍写在 ADR-0009。
5. **对抗面假设不同**。别人防"客户端出错"，r0semi 防"客户端怀有恶意"：ULEB 守卫、类型级长度、fuzz 双层、pre-auth 降级——原版一家三口的每个解码 bug 形态都有对应工事。
6. **演进过程本身是资产**。11 ADR + issues 台账 + 横评/审计文档互相引用，让"为什么这么做"可考古；四家对照项目几乎没有决策记录（gooophira 的锁序注释是散点的例外）。08-27 审计后的两天里管理面/反作弊/CPU 优化三线并进，全程 commit + 文档轨迹可考古——演进纪律自证。

与之对应的**代价**：回放录制/Redis/联邦/Web 面板仍是空白（管理 API 先行）、单人维护、为 encode-once 放弃了广播级多语言内容、CPU 剩余项为架构级约束（tokio 单 IO driver）。

---

## 6. 各自强在哪：一句话 + 选型指引

| 实现 | 一句话护城河 | 选它当你…… |
|---|---|---|
| phira-mp（原版） | 3000 行写清了整个 wire 协议，且带着真客户端在用的 SDK | 要**读懂协议**、做客户端或做字节级兼容验证 |
| gooophira-mp | 生产怪癖补偿 + 回放/统计/webhook/管理台的即用全家桶 | 要**今天就把服开起来运营**，能接受单体复杂度和作者免责声明 |
| phira-mp-nodejsver | 溯源最认真的 TS 移植 + 最低上手门槛 + 插件 SDK | 团队是 **Node 栈**、要做二次开发/插件生态 |
| jphira-mp | netty 分阶段 pipeline 教科书 + 5 分钟挂起恢复 + JitPack 嵌入 | 生活在 **JVM 生态**，想把房间服嵌进更大系统 |
| r0semi-mp | 三重 CI 资产（内存可量化/恶意输入可证明/替换性可机器验证）+ 开箱即用的管理面与反作弊（API-first，无 Web GUI） | 目标机器**挤满服务**、长期演化、对鲁棒性有执念、接受 API-first 运营 |

结论与 `competitor-review.md` 相同但现在有全景证据支撑：**Go 版赢在"现在"，r0semi 赢在"以后"——且"以后"正在到账**（互学通道已兑现：强开倒计时、错误 i18n、观战聚合缓冲、谱面匹配、版本握手、runtime-config 回滚、bench 工具链均已对标落地；学习清单剩 deadline 阶梯、心跳恢复日志/ECONNRESET 遥测、对局中重连窗口延长等少数项）。

---

## 7. 审计边界与方法声明（诚实条款）

- 本文档基于 2026-08-27 本地副本快照，**2026-08-29 复核更新**：四家对照副本未漂移（本地 HEAD 2026-05-31 ~ 2026-08-01），r0semi 一列数据全量重测；上游仓库随后演进可能导致个别结论过期。
- r0semi 复测口径：行数 = 物理行含空行（PowerShell Get-Content 计数）；测试数 = `rg "#\[(tokio::)?test\]"` 计 242；依赖 = Cargo.lock `name =` 条目计 203（其中 85 包为 017be37 引入的真 SDK dev 树，118 为运行时口径）；行号引用（server.rs:761/764/1709、stream.rs:80/83、config.rs:58）为 08-29 实查。
- **jphira 的协议编解码在外部库 `jphira-mp-protocol`（本地未克隆）**，其帧上限/ULEB 守卫情况标注"外部库不可见"，不算其缺点也不算优点。
- **nodejsver 的 README 能力矩阵显著大于仓库交付物**（内置插件/admin API/密码房通路），文中已逐一标注"未接线/不在仓库"。
- nodejsver 性能数据为其 README 自报，未经本审计复现；r0semi 数据出自自家压测 harness 与 ARCHITECTURE §10.1.1 实测记录，方法学为回环灌流。
- gooophira 作者在 README 中自述项目系 AI 赶工、不建议生产使用——引用于此仅为完整性，不构成对其工程质量的全盘否定（其测试密度五家最高）。
- 原版目录下的 `docs/ARCHITECTURE.md` 与 `docs/adr/` 为 r0semi 项目叠加的重写设计文档，并非上游产物；上游原版自身近乎零文档。
- r0semi 的个别 gooophira 细节（bench yml 具体阈值）未逐行核对，标注"待查证/以代码为准"；「版本字节不匹配」行已按 2026-08-27 复核更正（原版 = log-only 不校验、jphira = log-only、r0semi = 拒绝不匹配，D2 已落地）。
