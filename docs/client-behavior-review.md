# 真客户端行为审计（Phira 官方客户端 × phira-mp-client SDK，2026-08-27）

> 目的：为 `client-conformance.md` 的"真客户端一致性断言"提供**源码级依据**——服务端的每条
> 兼容性约束不再来自传闻或猜测，而来自官方客户端 + 官方 SDK 的逐行证据。
> 姊妹篇：`tech-debt-audit.md`（自身债务）/ `server-comparison.md`（五实现全景）。
>
> **许可证边界（重要）**：游戏客户端仓库 Teamflos/Phira 为 **GPL-3.0**——只读参照、行为对照可以，
> 任何代码拷贝进本项目即许可证污染。协议/SDK 仓库 Teamflos/phira-mp 为
> **Apache-2.0**——可作为 conformance 测试的 dev-dependency 引入（见 §8 回填建议）。

## 0. TL;DR

- 真客户端的联机逻辑分两层：**用法层**（`phira/src/mp/panel.rs`，825 行 UI）+ **SDK 层**
  （`phira-mp-client`，525 行；客户端 Cargo.toml 锁定 rev `cc822df`——本地 `/c/git/phira-mp`
  恰好就在该提交，两层证据对齐无漂移）。
- SDK 是**一碰就碎**的设计：至少 5 处 `unwrap()` 会因服务端"多说话"而 panic、未知枚举变体会
  断连——这给出了 conformance 最硬的一批不变式（§5/§6）。
- 重连 = 全新 TCP + 重新 Authenticate，房间快照在鉴权响应里回来（原版与 r0semi 已验证一致）；
  协议里不存在独立的 GetClientState 命令。
- l10n 校对：zh-CN 六条与原版逐字一致 ✅；zh-TW 发现一处简体字混入（`占用`→应为`佔用`），
  **已修正并测试通过（2026-08-27）**；EN 表存在 Title Case 决策点（§7）。
- TCP 协议对旧客户端**零前向兼容**：加枚举变体必炸、结构体尾部追加安全（读端不校验剩余字节）
  ——v2 新功能必须走 HTTP/IPC（gooophira 模式），这条现在是"有证据的结论"而非经验直觉。

## 1. 三层地图与血统

```
官方游戏客户端 C:/git/Phira（GPL-3.0）
  phira/src/mp.rs          → 4 行门面：tl_file!("multiplayer") + pub use panel::MPPanel
  phira/src/mp/panel.rs    → 825 行联机 UI（连接/建房/准备/聊天/对局进出）
        │ 依赖（Cargo.toml workspace：git = "…/phira-mp", rev = "cc822df"）
        ▼
SDK 协议层 /c/git/phira-mp（Apache-2.0，rev cc822df = 本地 HEAD）
  phira-mp-client/src/lib.rs      → 525 行：oneshot 回调式 Client 门面
  phira-mp-client/src/resolver.rs → 54 行：SRV 地址解析
  phira-mp-common/src/lib.rs      → Stream 帧读写 + 心跳常量（3s/2s/10s）
  phira-mp-common/src/command.rs  → 308 行 wire 协议唯一权威（derive 宏读写对称）
```

注意：**多人类代码不在客户端仓库里**——此前 GitHub 目录摸底看到的 `phira/src/mp/` 只是 UI，
真正的会话/编解码全在 git 依赖 `phira-mp-client/-common` 中。

## 2. 连接生命周期（客户端视角的硬事实）

| 步骤 | 客户端行为 | 证据 | 对 r0semi 的约束 |
|---|---|---|---|
| 寻址 | `Authority` 解析：显式端口直连；无端口查 SRV `_phira._tcp.<host>`；**裸 IP 无端口直接报错** | resolver.rs:13,35 | deployment 文档应写明：纯 IP 接入必须写 `host:port` |
| 握手 | `Stream::new(Some(1), …)`：**客户端先写版本字节**，随后才有任何帧 | common/lib.rs:58-63 | r0semi stream.rs 服务端先读后校验（D2 已落地）✅ |
| 首命令 | 必须是 Authenticate；原版对鉴权前其它包 warn + **忽略不断连** | 原 session.rs:263 | r0semi 同语义（server.rs:1299-1310，含 Ping 白名单）✅ |
| 心跳 | 每 3s 发 Ping，2s 内等 Pong；超时 `ping_fail_count+1`，成功清零 | client lib.rs:135-160, common:17-18 | Pong 必须 <2s 回——r0semi 走 try_send 即时回 ✅ |
| 断线判定 | **UI 层**读 `ping_fail_count() >= 2` → 自动重连（新 TCP + 新鉴权） | panel.rs:387-391 | 与 `reconnect_window=10s` 默认值同量级：最坏 2×(3+2)=10s 才发起重连，窗口恰好覆盖 |
| 重连恢复 | `Authenticate(Ok((me, Option<ClientRoomState>)))` 把房间快照带回来；**协议无独立 GetClientState 命令** | 原 session.rs:247-256；command.rs:279 | r0semi server.rs:1423-1444 已同构实现（§6.5-23）✅ |
| 收包容错 | 解码失败 → warn + **退出收包循环**（连接僵死，靠心跳失败→UI 重连兜底，≤10s） | common/lib.rs:130-135 | 见 §6 演进约束：发未知变体等于踢掉旧客户端 |

## 3. SDK 的请求-响应纪律

- 除 Ping/Touches/Judges 外每个命令都挂一个 oneshot 回调；`rcall` 发完才注册回调，
  统一 **7s 超时**（client lib.rs:27,250-258）。慢命令超时给用户弹错，但连接还活着。
- **响应到达时若无待定回调 → `take().unwrap()` panic**（client lib.rs:424）。推论：
  - 服务端**绝不能重复发同一操作响应**（哪怕相隔很久）；
  - 服务端**绝不能主动发** CreateRoom/JoinRoom/Ready/Played 等 op-response 形状的帧。
- 本项目核实：重放机制是**路由表轮询重查**（lifecycle.rs:296-306），不重放任何历史帧 →
  不存在重复响应风险。此结论值得固化成契约断言（§8-A1）。
- panic 传播路径：process() 在 recv 任务内 await，panic 使 recv 任务死亡 → 停止处理入站 →
  Pong 无人 notify → 心跳连续失败 → ≤10s 后 UI 自动重连并从鉴权响应恢复房间。
  （即：误发响应不会永久杀账号，但会制造一次 10s 级"闪断"，线下表现为随机掉线。）

## 4. 客户端房间状态机与服务端职责

客户端本地状态 = `ClientRoomState`（command.rs:256-266），全部由三条途径维护：
鉴权响应快照、JoinRoomResponse、以及若干**服务端推送**。推送面的脆弱点：

| 推送 | 客户端处理 | 证据 | 服务端红线 |
|---|---|---|---|
| `Message::LockRoom/CycleRoom/LeaveRoom` | 就地改房态字段 | client lib.rs:453-469 | **必须在用户"仍在房间"时才发**——发给无房用户 = `as_mut().unwrap()` panic |
| `ChangeState(room)` | 清空 live_players 缓存；`is_ready = is_host`（房主自动变已准备！） | :474-480 | 同上，无房必炸；另外服务端若不希望"房主自动 ready"，换状态前要先改房主（gooophira forceSyncHost 补偿的对象大概率就是这个语义耦合） |
| `ChangeHost(bool)` | 改本地 is_host | :481-483 | 同上，无房必炸 |
| `OnJoinRoom(user)` | `live |= user.monitor`（观战者进场会把 live 置 true）；无房则忽略 | :491-495 | 安全，可随时发 |
| 其余 `Message::*` | 进消息列表供 UI 渲染 | panel.rs:411-467 | 无 panic 面，可宽松 |

UI 层的门控（服务端无需重复实现，但决定玩家实际能触发什么命令）：
- 选谱：本地判 host + 判 SelectChart 态（panel.rs:208-222）；RequestStart 前强制下载谱面（:232-251）。
- Ready 同样先下载再发（:359-361, :360 check_download(false)）。
- 建房成功后客户端**自己合成**本地房态 `{is_host:true, users:{me}}`，不信服务端回执形状（SDK lib.rs:287-305）。
- 入房后本地置 `locked/cycle=false`，即使进的是锁定房也显示未锁（SDK lib.rs:318-328 保真度缺口，无害）。
- 对局收尾自动化：正常完赛 → `Played{id}`；`RECORD_ID==-1`（没打完）→ 自动 `Abort()`（panel.rs:634-644）。
  ⇒ 服务端收到的 Abort 大多是客户端自动发的，不应按"恶意/异常"从严处置。
- 聊天是编译开关：`CHAT_ENABLED = cfg!(feature="chat")`，**默认关闭**（panel.rs:35）→
  真客户端大盘里 Chat 流量趋近于零，D1 的 2/s 限速影响面极小。
- 断开按钮只是丢弃 Client（Arc 最后一个引用释放 → Stream::drop abort 双任务 → 连接关闭）（panel.rs:380-385）。

## 5. "服务器多说话就出事"清单（conformance 不变式候选）

把 §3/§4 的脆弱点汇总成可断言的形式，每条都有 SDK 行号背书：

- **A1 响应唯一性**：任一 op-response 变体（CreateRoom/JoinRoom/LeaveRoom/LockRoom/CycleRoom/
  SelectChart/RequestStart/Ready/CancelReady/Played/Abort/Authenticate/Chat）在单连接生命周期内
  只能作为请求的直接应答发送一次。无对应请求 → 真客户端 panic（lib.rs:424）。
- **A2 房间推送前置条件**：Message::{LockRoom,CycleRoom,LeaveRoom}、ChangeState、ChangeHost
  仅可在服务端认定该用户 in-room 时发送。无房 → 真客户端 panic（lib.rs:453-483 各 unwrap）。
- **A3 Ping↔Pong 延迟预算**：Pong 必须 2s 内到达（common:18）；否则计数累积至 UI 重连。
- **A4 鉴权前语义**：非 Authenticate 命令仅忽略、不断连（双方一致，回归测试可钉住）。
- **A5 重连环路完整性**：任意时刻重连，Authenticate(Ok) 的 room 快照必须让客户端可继续
  （尤其 Playing/WaitForReady 态——这是 ISSUE-0010 孤儿房与 gooophira 伪装选谱补偿共同盯的场景）。
- **A6 字节级编解码对称**：所有 ServerCommand 序列化可被原版解码器消费（Oracle 工程既役）——
  外加 §6 的尾部字节规则。

## 6. 协议演进硬约束（v2 功能面的铁律）

读了 derive 宏生成代码（phira-mp-macros/src/lib.rs struct_read/build_derive_enum）：

- **结构体尾部追加字段 = 旧客户端安全**：派生读端逐字段读、**从不校验剩余字节** →
  服务端多发尾部字节被旧客户端静默丢弃。⇒ 给 `ClientRoomState` 尾部追加字段（ISSUE-0007
  方案 A 的载体）对存量客户端**可行**；前提是本项目自己的读端同样容忍尾部字节（需给
  phira-api binary.rs 加一条"容忍 trailing bytes"回归测试——当前未必断言了这一点）。
- **枚举加变体 = 旧客户端断连**：未知 tag 走 `bail!("invalid enum")`（宏 build_derive_enum），
  客户端 recv 循环 break（common/lib.rs:130-135）→ ≤10s 闪断。⇒ **TCP 上永远不加
  ServerCommand/Message 变体**；管理 API、回放上传、 ban 通知等 v2 能力一律走 HTTP/IPC
  旁路（gooophira "扩展全在 HTTP/IPC 侧"的做法由此从经验升格为协议必然）。
- 帧长度上限 2MiB 客户端读侧也有（common/lib.rs:121-123），与协议一致；
  LEB128 移位上限 pos>32 报错（:117-119）——客户端比旧版原版服务端 decoder 更防御。

## 7. l10n 校对结果（B2 增量收尾）

三方比对：原版服务端 ftl（6 条）↔ r0semi l10n.rs 三语表 ↔ 客户端 multiplayer.ftl（50+ 条 UI 键）。

| key | 原版 en-US | r0semi EN | 一致? |
|---|---|---|---|
| create-id-occupied | Room ID is occupied | room id occupied | ❌ 大小写 |
| join-game-ongoing | Game is ongoing | game is ongoing | ❌ 大小写 |
| join-room-full | Room is full | room is full | ❌ 大小写 |
| join-room-locked | Room is locked | room is locked | ❌ 大小写 |
| join-cant-monitor | Permission denied. You can't monitor this room. | no monitor permission | ❌ 整句不同 |
| start-no-chart-selected | No chart selected | no chart selected | ❌ 大小写 |

（EN 列差异 2026-08-27 用原版 ftl 逐条复核确认；zh 两列比对方法见下。）

- zh-CN：r0semi 与原版**逐字一致** ✅（逐条 substring 精确比对，2026-08-27）。
- zh-TW：五条一致；`create-id-occupied` 曾混入简体 `占`（原版为 `佔` U+4F54）——
  **已修正**（l10n.rs，`cargo test -p phira-server --lib` 19 passed）。教训：该表当初按
  简体字形人工"转换"，繁体校验应走机器比对（本文即用 od/substring 比对法）。
- EN 差异成因已知：r0semi EN 表刻意镜像 impl 现行英文以保"本地化前后字节不变"
  （l10n.rs:12-14 的声明），但这套现行英文本身≠原版措辞。是否切换到原版 Title Case 措辞
  属产品决策（切了更像官服、代价是自家 Oracle/测试基线要同步），建议记入 issue 由 owner 定夺，
  本文档只登记不动码。
- 服务端文案的展示链路已核实：客户端把 auth/操作错误的原始字符串经 anyhow context 包装后
  show_error 透出（如 panel.rs:195,219）→ 中文用户真的能看到 B2 本地化后的中文报错，**B2 投入有效**。
- 客户端 multiplayer.ftl 的 50+ 键全是 UI 本地文案（按钮/系统消息模板），服务端除了
  Message 广播里的 user/score 等插值外**无需供数** → 无"缺键"问题，无需对照扩表。

## 8. 回填建议（映射到既有债项台账）

| 债项/目标 | 本文给出的增量 | 建议动作 |
|---|---|---|
| client-conformance 崩溃猎手（立即档残留） | §5 A1-A6 不变式清单（每条带行号） | 新建 conformance 工程，dev-dependency 引 Apache-2.0 的 `phira-mp-client`，用真 SDK 直连 r0semi 跑剧本；A1/A2 用"负面注入"断言服务端永不发出 |
| D3 protocol_hack 层 | ChangeState 的 `is_ready=is_host` 语义耦合（§4）很可能是 gooophira forceSyncHost/forceSyncInfo 补偿的真实对象 | 开源客户端源码在手：先写剧本复现"需要补偿吗"，答案大概率是"不需要"——r0semi 按协议本义发即可，unless 剧本证明旧版客户端时序敏感 |
| ISSUE-0007 game_time | 方案 A 载体确认：ClientRoomState **尾追加**字段对存量客户端安全（§6）；且 protocol 无 GetClientState，快速通道本来就是鉴权响应快照 | 维持"B 挂起、随断线恢复立项"决策不变；届时走尾追加 + 自家读端补"容忍尾部字节"测试 |
| C3 手写 HTTP 客户端 | 官方上游是明文 HTTP API（`https://phira.5wyxi.com`），无 CDN/302 迹象的证据仍缺 | 维持原判：等管理 API 接入时一并加固 |
| ISSUE-0010 孤儿房 | 客户端建房 id 来自用户输入框（≤20 字符 [A-Za-z0-9_-]，panel.rs:607→RoomId 校验 command.rs:80-93），无法阻止用户手输同 id 重试 | 维持文档指引方案；可加 deployment 建议房主端引导 uuid 后缀 |
| ARCHITECTURE 文档 drift 防线 | — | 把 §2 表格常量（3s/2s/7s/fail≥2/SRV 规则/版本字节方向）摘录进 interop 或 client-conformance 章节，标"以本文行号为源" |

## 9. 审计边界

- **官方文档站（teamflos.github.io/phira-docs）经核查零联机协议内容**：全部章节为谱面格式
  （RPE/PE/phi）、respack、shader/UML、构建指南——协议唯一权威仍是原版
  `phira-mp-common/src/command.rs`，客户端行为权威只能靠读客户端源码（本文即结论）。
  后人无需再探路。

- 游戏客户端锁 rev 未在仓库内显式记录（浅克隆 HEAD=51b05cb，2026-08-27）；SDK 侧 rev 明确为 cc822df 且与本地 phira-mp 副本完全一致——**协议证据链闭环**，UI 层行为可能随后续版本漂移。
- SongScene 打歌循环里 Touches/Judges 的发送节奏未深读（对服务端 conformance 影响限于负载形态，已在压测覆盖范围）。
- "客户端会怎么处理我们已经发出的消息"均已核；"我们尚未实现的官方行为"不在本文范围（属功能面规划）。
- GPL 仓库零拷码：本文仅有行为描述与行号引用，无源码移植。
