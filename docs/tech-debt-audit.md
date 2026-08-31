# 技术债审计报告（r0semi-mp，2026-08）

> 本文档是 2026-08 对 r0semi-mp 自身代码的债务审计结论，方法为源码实证（非文档转述）。
> 竞品环境见 `competitor-review.md`；本报告聚焦 r0semi 自身的可改进项，
> 分四级，均含代码定位。两条新的实质性 bug 已按项目纪律落档：
> [ISSUE-0011](issues/0011-encode-cache-pointer-key-aba.md)（EncodeCache ABA）与
> [ISSUE-0012](issues/0012-session-registry-unbounded-growth.md)（SessionRegistry 增长）。
>
> **2026-08-31 状态同步注记**：审计后部分项已清偿，本次逐项对照代码回写——
> B4/B5/D1/D2 ✅ 关闭；C3 热重载 ✅（仅剩子域 CDN allowlist）；C1/C4/B6/优先级表
> 进度与剩余项修正；"丢旧保新"表述统一（实现为丢新 + 慢消费者断连，效果等价）。
> 同步基线：`cargo test --workspace` 307 全绿（2026-08 实测）。

## 审计方法

- 分类：潜伏 Bug / 功能欠账 / 结构性债 / 防御缺口（B/C/D 为"还没做完债"，A 多为"优化引来的债"）。
- 判据：每条均需可引用的代码/文档定位；对照 ARCHITECTURE.md 承诺与 ADR 意图。

---

## A 级：潜伏 Bug（现在没炸，不代表不会炸）

### A1. EncodeCache 指针键 ABA 危害 —— 本次审计新发现 → **ISSUE-0011**（✅ 已解决 2026-08）

`SessionSink::deliver` 以 `Arc::as_ptr(frames) as usize` 为缓存键（server.rs:672-676），
但缓存条目只持有编码结果 `Arc<Vec<u8>>` 不持有源帧 Arc（server.rs:513-547）。
注释断言"旧帧指针不复用，留着只会是死条目"（server.rs:540）——**该前提不成立**：
Rust 分配器可复用同尺寸类释放块的地址。地址复用 = 新事件命中死条目 = 观战者收到历史陈旧帧。

- 危害：静默数据正确性缺陷（无 panic、无日志，现象似网络抖动，极难排查）。
- 触发：需 ≥1 观战者在线 + 地址碰撞；统计上随运行时长线性累积必然发生。
- **修复（方案 A 已落地）**：`EncodeCache` 条目升级为 `EncodeEntry { _pin, bytes }`，`_pin` 持源 Arc 克隆（`Box<dyn Any + Send + Sync>`）钉住地址；`get_or_encode(key, pin, encode)` 三参；删除错误断言；4 个回归测试。详见 ISSUE-0011 文末修复记录。

### A2. actor 内 await 回源 = 房间级队头阻塞 —— ✅ 已修复（2026-08，两段式 + 兜底）

`handle_played` 中 `self.deps.api.fetch_record(id).await` 在房间 actor 内等待官方 API
（最长 5s，impl-rooms-v1/src/lib.rs handle_played）。期间该房间所有命令（含触摸帧）排队。

- 危害：5 人一局，一人交成绩卡全场数秒。
- **修复（两段式已落地，§4.9-2 规则 2）**：`Played` 只做受理（幂等预检 + in-flight 登记，
  立即回 Ok），core 房外任务回源（`Bus::with_api` 注入，完成后以 `RecordFetched` 系统
  命令回注）；回注失败（core 有界重试 2 次耗尽 / player 不匹配）→ 提交者按"无有效成绩"
  结算为 aborted，保证 GameEnd 必然触发（房间不卡 Playing）——契约测试
  `record_fetch_failure_settles` 钉死。
- 代价：客户端不再收到回源失败的错误响应（受理即 Ok），失败以结算 + 日志呈现——协议
  A1 不变式禁止再造响应帧（client-behavior-review §5），此取舍为设计决策。

---

## B 级：功能欠账（插座已装，电器没买）

| # | 欠账 | 证据 | 影响 | 参照 |
|---|---|---|---|---|
| B1 | **Tick 是空壳**（✅ 已解决 2026-08——WaitForReady 60s 倒计时，契约测试 ready_countdown_tick 三场景） | lib.rs:851 "v1 无玩法倒计时…占位（§4.6）"；但 bus.rs 队列策略已分类 `Tick→DropIfFull` | 无倒计时/超时强开，对局或无限挂起 | gooophira ready 倒计时（60s 强制开赛，未准备 Aborted，18 测试） |
| B2 | **lang 字段躺契约里没用**（✅ 已解决 2026-08——server 出口按 lang 本地化，l10n 静态表零依赖） | `UserIdentity.lang` 存在（auth.rs）；错误 `"already uploaded"`/`"game is ongoing"` 英文硬编码 | 中文玩家看英文报错 | 原版 Fluent 三语（en/zh-CN/zh-TW，per-user 作用域） |
| B3 | **Metrics 收集了但不暴露**（✅ 已解决 2026-08） | bus.rs `Metrics::snapshot()`（含 calls/ok/business/internal），但 /healthz 仅 conn_count/rooms/version（server.rs 1121） | 可观测性数据进黑洞 | 一小时可暴露 |
| B4 | ISSUE-0007 game_time 缺失（✅ **已解决 2026-08-27**——方案 A：`GetClientState` 尾追加 last_game_time + actor 内双时机记录/重置，回归测试） | 重连恢复"玩家打到哪"无进度维度 | 断线恢复体验缺一维 | 原版 `NEG_INFINITY` 哨兵 + AtomicU32 |
| B5 | ISSUE-0010 CreateRoom 非幂等（✅ **已处置 2026-08-27**——方案 D→C：deployment.md §9 客户端指引；协议级根因留待协议演进） | 响应丢失重试 → RoomIdOccupied + 孤儿房 | 弱网玩家建房失败率上升 | 协议级限制，已留档 |
| B6 | **观战转播逐命令直发**（✅ 已解决 2026-08——B6 聚合缓冲 + Tick 心跳；**遗留**："丢旧保新"为策略表述、实际丢新 + kicker 5s 断连；阈值硬编码未进 config）| live 下 Touches/Judges 立即产出 Relay*，朴素客户端 60Hz 单帧即 ~480 cmd/s/房小包洪峰 | 网络/syscall/包数放大 | gooophira AggregatingMonitorBuffer（50ms 动态窗合并 + flush 编码一次） |

B1 的修复与架构最贴合：Tick 插座已预埋，只需生命周期任务周期发 `Tick{now}` + 房间状态机加倒计时字段。B2/B3 是全场最低成本高收益项。
> **修复注记（2026-08）**：B1 已通电（WaitForReady 倒计时 + 超时驱逐复用 evict）；B2 已落地（对照原版 Fluent 三语语义，但用零依赖静态文案表实现于协议出口，lang 存 SendSlot 随会话生灭）；B3 同日完成（/healthz 暴露 Metrics）。
> **状态同步注记（2026-08-31）**：B4 已关闭（ISSUE-0007 方案 A——last_game_time 尾追加 2026-08-27 落地，回归测试）；B5 已处置（ISSUE-0010 方案 D→C——deployment.md §9 客户端指引，协议级根因留待演进）；B6 遗留 = 慢消费者实际为**丢新 + kicker 5s 断连**（"丢旧"是策略表述，效果等价），且内存守卫/踢人阈值仍为硬编码 `const` 未进 config（见 AGENTS.md 安全锁）。

---

## C 级：结构性技术债

### C1. server.rs 上帝文件（拆分已触发：admin.rs 790 行已抽出，server.rs 仍 2341 行，2026-08 实测）

SessionSink、EncodeCache、CommandLimiter、ConnectionAdmission、Backpressure、RoomListSink、
CompositeSink 全挤在一个文件。违背自身"薄缝"哲学。**拆分蓝图**（netty 已示范）：

```
server.rs → front-gate/(admission, proxy, limiter)
          + sink/(session_sink, encode_cache, room_list_sink)
          + shutdown/(grace, notify)
```

**触发时机**：不要为拆而拆，绑定到下一个必然功能（管理 API / 回放 Sink）进场时"顺便"抽出。
**进度（2026-08-31 同步）**：管理 API 进场已触发第一步——`http_serve`/`http_accept_loop` 抽至 `admin.rs`；
sink/shutdown 组块与 `front-gate/` 拆分未动（server.rs 期间增长至 2341 行，下一个触发点 = 回放/Store Sink 进场）。
bus.rs:520 已留升级路径："泛化触发条件 = 第二个大扇出广播场景"——作者自知但未到阈值。

### C2. SessionRegistry 只进不出 → **ISSUE-0012**（✅ 已解决 2026-08）

`register()` 只 insert（lifecycle.rs:71），无 remove；每用户永久占 `(i32,u64,String)`≈40-80B。
**注意**：不能简单"离线删除"——epoch 需单调递增且不回收，否则重连回退 epoch 1 可能撞上
遗僵尸连接 `current_epoch==state.epoch`，复活 ISSUE-0009 漏洞。推荐方案 A：拆 `epochs`（永不删，8B/用户）
与 `names`（可淘汰）两表。

**修复（方案 A 已落地）**：`SessionRegistry` 拆为 `epochs`（永不删）+ `names`（可淘汰）两表；
`DangleExpired` 后调 `evict_name` 释放昵称；epoch 单调不回收保 ISSUE-0009 语义。
详见 ISSUE-0012 文末修复记录。

### C3. 手写 HTTP 客户端健壮性天花板 —— ✅ 已加固（2026-08，剩余项见下）

`http.rs`（手写 HTTP/1.1，约两百行）：Content-Length 无上限校验、只认 200（不跟重定向）、
注释自嘲"本实现只打自己的 mock API"。官方 API 行为变更（如加 CDN 302）即断鉴权/取谱面。
属"协议之外"的兼容面，不在开源范围（见 client-conformance.md）。

**加固已落地（2026-08）**：响应体 16MiB 上限（声明超限不读不缓冲直接拒绝 + 声明小实发多超限即断）
+ 30x 有限跟随（同 host 白名单，见下）——回归测试 6 连：`oversized_content_length_rejected_without_reading` /
`redirect_302_cross_host_rejected` / `redirect_302_followed_same_host` / `redirect_302_loop_exhausted` /
`redirect_302_without_location_rejected` / `trailing_bytes_beyond_content_length_discarded`。

**302 跟随升级（2026-08 二次清偿）**：从"显式拒绝"改为**同 host 有限跟随**（≤3 跳，
`resolve_same_host` 白名单：目标 host+port 必须与 base 一致，跨域拒绝）——上游加 CDN 反代时
鉴权/取谱面自动跟上；token 只随同 host 请求重发，绝不跨域外泄；Location 缺失/跳数耗尽/
非 200 终态均显式报错（可诊断，不静默）。**剩余项**：若实际上游 302 到**子域** CDN
（如 cdn.5wyxi.com），需把白名单从"同 host"扩为"可配置 allowlist"（走 config 项，现状拒绝对
故障更安全）。

**配置热重载（✅ 已落地）**：`config_poll_interval` 周期轮询 + `update_config` 广播（见
`server_config.example.yml`），C3 剩余仅上列子域 CDN allowlist 一项。

### C4. 谱面反作弊规则耦合（已标识的演进点，P2 触发时清偿）

`handle_record_fetched` 内的谱面匹配（P1，`record.chart vs self.chart.id`）把**反作弊判定规则**
硬编码进了房间状态机（impl-rooms-v1）。区分两种耦合：
- **数据耦合（必然，保留）**：判定需要 `self.chart`（本局谱面）——record 全字段 + 本局谱面的
  交集**只存在于回注点**，这是谱面匹配进不了观察者面（Moderator 无房间视野、`on_event(Played)`
  无 record_id/chart、`intercept` 禁长 IO）的根因；
- **规则耦合（当前程控，可清偿）**："chart 不一致 → 成绩无效"的判定逻辑本身。

**为什么当前程控是止损**：单一规则 + 零配置需求 + 契约测试钉死 + 失败路径已收敛为单一形态
（成绩无效 → 复用 `settle_record_failed` 结算，反作弊失败绝不卡房间）——此刻抽接口是预言式抽象
（原则 5）。**但"无多解"论据对未来不成立**：运营可能要求"宽松（仅 chart/fail-open）vs 严格
（chart+level+mod）vs 名单式"。

**清偿路径（P2 触发时）**：判定点接成契约级 policy 插座——`impl-rooms-v1` 保留默认实现
（现 chart 匹配规则），组合根可注入叠加规则：

```rust
// phira-api（弱演进）
pub trait RecordPolicy: Send + Sync {
    /// 回注点裁决：本局谱面 + 成绩 + 上报者 → 拒绝理由（None = 放行）。
    fn evaluate(&self, room_chart: Option<i32>, record: &Record, user_id: i32)
        -> Option<RoomError>;
}
```

**安全线（外置后必须由接口签名封死）**：policy 只允许"拒绝该成绩"（走既有 settle 结算），
**不允许其它副作用**——策略再烂，后果最多是某个成绩变成 abort，房间不变量与 GameEnd 收尾
不受影响。
**触发时机**：P2（mod/level/阈值类运营规则）需求到来时，与 `record.Mod` 数据口（上游有、
DTO 未接）一并落地。
**进度（2026-08-31 同步）**：跨房重放规则已由 **AntiCheatObserver（admin-api 阶段 3.6）**以
Moderator 观察者面落地 + R2 高频观测（3.6.1，纯 flag 不自动拦）——即 C4 的"拦截类"规则已走
观察者通道出账；`RecordPolicy` 回注点裁决插座仍未建，触发点 = P3 难度校验（mod/level，
DTO 未接）或多解规则真实出现。

> **P2 落地阻力实证（2026-08-31）**：本段"上游有 mod/level"为**未实证断言语**——实地核查
> 原版 `phira-mp-server/src/server.rs:27` `Record` 结构体**无** mod/level 字段；本地所有源码
> 均无该字段证据；线上官方 API（`/record/{id}`）需鉴权，无法直接取证。**当前添加
> `Record.mod`/`level` 会因字段名猜错（如官方叫 `charm`/`difficulty` 而非 `mod`/`level`）
> 而成为永死字段。** 故 P2 降级为勘误：**不添加**，留待拿到官方 `/record` 真实响应结构
> 或 P3 规则需求出现时再定字段名。加入后还需 `#[serde(default)]` + Option 缺省（与现有
> `chart: Option<i32>` 同款风格），并需验证字段名与实际 API 响应一致。
---

## D 级：防御缺口（有意取舍但仍属缺口）

| # | 缺口 | 现状 | 对照 |
|---|---|---|---|
| D1 | Chat 不限速（✅ 已解决 2026-08）——`rate_limit()` 白名单加入 `ClientCommand::Chat`（2/s，500ms 间隔），超限回 `TooManyRequests`；`rate_limit` 单元测试 + 移植 memory_guard 测试改用 Touches | `rate_limit()` 白名单只有 CreateRoom/JoinRoom/SelectChart/Played；注释"低频命令(Chat/Ready…)不限"是显式决定 | gooophira 聊天 2/s 令牌桶 |
| D2 | 版本握手不校验（✅ 已解决 2026-08）——`handle_connection` 握手后校验 `stream.version() != PROTOCOL_VERSION` 即释放准入并断开，回归测试 `handshake_rejects_wrong_version_then_accepts_v1` | stream.rs 读取版本但只记录展示（healthz），不拒绝不匹配 | gooophira `ver != protocolVersion` 即断 |
| D3 | protocol_hack 层缺失 | 无真客户端怪癖补偿机制（✅ **部分推进 2026-08-31**：conformance.rs 真 SDK 崩溃猎手 A1–A6 全绿 + **断言库雏形**（A2 负向注入：未入房用户绝收房间推送，三面覆盖）+ **漂移哨兵**（tools/check-client-drift.py + drift-sentinel workflow 手动/每周）——client-conformance.md 五步规划步骤 2/4 已动工） | gooophira/jphira 的 forceSyncInfo/fixClientRoomState（源码已验证怪癖机制，见 client-conformance.md） |

D2 在协议 v2 到来时会被动暴露；D3 的正确解法不是抄传闻，而是走 client-conformance.md 的一致性验证体系。

---

## 修复优先级建议

| 阶段 | 内容 | 判据 |
|---|---|---|
| **立即**（天级） | ✅ A1 ABA 修复（ISSUE-0011）· ✅ B3 Metrics 暴露 · ✅ D2 版本握手校验 · ✅ client-conformance 崩溃猎手测试（conformance.rs，真 SDK A1–A6） | 消灭全部已知正确性地雷 |
| **短期**（周级） | ✅ B2 i18n（EN 措辞决策遗留 ISSUE-0013）· ✅ 谱面反作弊（P0/P1 + 跨房重放/频率观测经 AntiCheatObserver 落地 = admin-api 阶段 3.6/3.6.1；P3 难度校验未做、`record.Mod`/level DTO 未接 → **C4**）· ✅ 观战聚合缓冲（B6+Tick 心跳）· ✅ D1 Chat 限速 · ✅ C2 Registry 拆表（ISSUE-0012） | 每项带契约测试落地 |
| **中期**（月级） | ✅ B1 Tick 通电（倒计时，提前完成）· ⬜ 一致性断言库 + 漂移哨兵（未建，见 client-conformance.md 五步规划；崩溃猎手 A1–A6 已先行）· ✅ 管理 API（阶段 1-3.6.1 全落地；⬜ 阶段 4 面板 = 前端消费方，决策不进仓库） | 玩家可感知 + 护城河成形 |
| **择机** | ✅ A2 回源出队（已提前完成）· ⬜ C1 server.rs 拆分（admin.rs 第一步已抽出；sink/shutdown 组块待下一触发点）· ⬜ 回放录制（Store 接口） | 绑定到相关功能进场时顺手做 |
| **远景** | Store 持久化 · 协议 v2 预案 · 多实例（仅当需求出现） | 文档立 flag，不动代码 |

## 总体判断

r0semi 的债务画像独特：**几乎没有"烂代码债"，全是"还没做完债"（B/C/D）与"优化引来的债"（A）**。
前者是单人项目时间问题，后者是所有高性能系统的宿命。修复优先级的关键是要**先暴露（B3 Metrics），
再决策（A2 是否值得重构）**——用数据引导投入，而不是凭猜测。
