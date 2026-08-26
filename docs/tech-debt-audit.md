# 技术债审计报告（r0semi-mp，2026-08）

> 本文档是 2026-08 对 r0semi-mp 自身代码的债务审计结论，方法为源码实证（非文档转述）。
> 竞品环境见 `competitor-review.md`；本报告聚焦 r0semi 自身的可改进项，
> 分四级，均含代码定位。两条新的实质性 bug 已按项目纪律落档：
> [ISSUE-0011](issues/0011-encode-cache-pointer-key-aba.md)（EncodeCache ABA）与
> [ISSUE-0012](issues/0012-session-registry-unbounded-growth.md)（SessionRegistry 增长）。

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

### A2. actor 内 await 回源 = 房间级队头阻塞

`handle_played` 中 `self.deps.api.fetch_record(id).await` 在房间 actor 内等待官方 API
（最长 5s，impl-rooms-v1/src/lib.rs handle_played）。期间该房间所有命令（含触摸帧）排队。

- 危害：5 人一局，一人交成绩卡全场数秒。
- 代码注释自行标注"仅阻塞该房间 actor（§4.9-2）"——已有认知，属权衡。
- 修法：回源移出 actor（先回"受理中"，响应经系统命令回流）。**建议待 B1 倒计时/ Tick 机制成熟后再做**（届时回流通道现成）。可观测性（B3 暴露 Metrics 后可看 fetch_record 延迟分布）应作为投入决策依据。

---

## B 级：功能欠账（插座已装，电器没买）

| # | 欠账 | 证据 | 影响 | 参照 |
|---|---|---|---|---|
| B1 | **Tick 是空壳**（✅ 已解决 2026-08——WaitForReady 60s 倒计时，契约测试 ready_countdown_tick 三场景） | lib.rs:851 "v1 无玩法倒计时…占位（§4.6）"；但 bus.rs 队列策略已分类 `Tick→DropIfFull` | 无倒计时/超时强开，对局或无限挂起 | gooophira ready 倒计时（60s 强制开赛，未准备 Aborted，18 测试） |
| B2 | **lang 字段躺契约里没用**（✅ 已解决 2026-08——server 出口按 lang 本地化，l10n 静态表零依赖） | `UserIdentity.lang` 存在（auth.rs）；错误 `"already uploaded"`/`"game is ongoing"` 英文硬编码 | 中文玩家看英文报错 | 原版 Fluent 三语（en/zh-CN/zh-TW，per-user 作用域） |
| B3 | **Metrics 收集了但不暴露**（✅ 已解决 2026-08） | bus.rs `Metrics::snapshot()`（含 calls/ok/business/internal），但 /healthz 仅 conn_count/rooms/version（server.rs 1121） | 可观测性数据进黑洞 | 一小时可暴露 |
| B4 | ISSUE-0007 game_time 缺失 | 重连恢复"玩家打到哪"无进度维度 | 断线恢复体验缺一维 | 原版 `NEG_INFINITY` 哨兵 + AtomicU32 |
| B5 | ISSUE-0010 CreateRoom 非幂等 | 响应丢失重试 → RoomIdOccupied + 孤儿房 | 弱网玩家建房失败率上升 | 协议级限制，已留档 |

B1 的修复与架构最贴合：Tick 插座已预埋，只需生命周期任务周期发 `Tick{now}` + 房间状态机加倒计时字段。B2/B3 是全场最低成本高收益项。
> **修复注记（2026-08）**：B1 已通电（WaitForReady 倒计时 + 超时驱逐复用 evict）；B2 已落地（对照原版 Fluent 三语语义，但用零依赖静态文案表实现于协议出口，lang 存 SendSlot 随会话生灭）；B3 同日完成（/healthz 暴露 Metrics）。

---

## C 级：结构性技术债

### C1. server.rs 上帝文件（1357 行）

SessionSink、EncodeCache、CommandLimiter、ConnectionAdmission、Backpressure、RoomListSink、
CompositeSink 全挤在一个文件。违背自身"薄缝"哲学。**拆分蓝图**（netty 已示范）：

```
server.rs → front-gate/(admission, proxy, limiter)
          + sink/(session_sink, encode_cache, room_list_sink)
          + shutdown/(grace, notify)
```

**触发时机**：不要为拆而拆，绑定到下一个必然功能（管理 API / 回放 Sink）进场时"顺便"抽出。
bus.rs:520 已留升级路径："泛化触发条件 = 第二个大扇出广播场景"——作者自知但未到阈值。

### C2. SessionRegistry 只进不出 → **ISSUE-0012**（✅ 已解决 2026-08）

`register()` 只 insert（lifecycle.rs:71），无 remove；每用户永久占 `(i32,u64,String)`≈40-80B。
**注意**：不能简单"离线删除"——epoch 需单调递增且不回收，否则重连回退 epoch 1 可能撞上
遗僵尸连接 `current_epoch==state.epoch`，复活 ISSUE-0009 漏洞。推荐方案 A：拆 `epochs`（永不删，8B/用户）
与 `names`（可淘汰）两表。

**修复（方案 A 已落地）**：`SessionRegistry` 拆为 `epochs`（永不删）+ `names`（可淘汰）两表；
`DangleExpired` 后调 `evict_name` 释放昵称；epoch 单调不回收保 ISSUE-0009 语义。
详见 ISSUE-0012 文末修复记录。

### C3. 手写 HTTP 客户端健壮性天花板

`http.rs`：Content-Length 无上限校验、只认 200（不跟重定向）、注释自嘲"本实现只打自己的 mock API"。
官方 API 行为变更（如加 CDN 302）即断鉴权/取谱面。属"协议之外"的兼容面，不在开源范围（见 client-conformance.md）。
当管理 API 或回放接入时，建议为它补充重定向解码与响应体上限。

---

## D 级：防御缺口（有意取舍但仍属缺口）

| # | 缺口 | 现状 | 对照 |
|---|---|---|---|
| D1 | Chat 不限速（✅ 已解决 2026-08）——`rate_limit()` 白名单加入 `ClientCommand::Chat`（2/s，500ms 间隔），超限回 `TooManyRequests`；`rate_limit` 单元测试 + 移植 memory_guard 测试改用 Touches | `rate_limit()` 白名单只有 CreateRoom/JoinRoom/SelectChart/Played；注释"低频命令(Chat/Ready…)不限"是显式决定 | gooophira 聊天 2/s 令牌桶 |
| D2 | 版本握手不校验（✅ 已解决 2026-08）——`handle_connection` 握手后校验 `stream.version() != PROTOCOL_VERSION` 即释放准入并断开，回归测试 `handshake_rejects_wrong_version_then_accepts_v1` | stream.rs 读取版本但只记录展示（healthz），不拒绝不匹配 | gooophira `ver != protocolVersion` 即断 |
| D3 | protocol_hack 层缺失 | 无真客户端怪癖补偿机制 | gooophira/jphira 的 forceSyncInfo/fixClientRoomState（但需源码验证，见 client-conformance.md） |

D2 在协议 v2 到来时会被动暴露；D3 的正确解法不是抄传闻，而是走 client-conformance.md 的一致性验证体系。

---

## 修复优先级建议

| 阶段 | 内容 | 判据 |
|---|---|---|
| **立即**（天级） | ✅ A1 ABA 修复（ISSUE-0011）· ✅ B3 Metrics 暴露 · ✅ D2 版本握手校验 · ⬜ client-conformance 崩溃猎手测试 | 消灭全部已知正确性地雷 |
| **短期**（周级） | 绿档剩余：B2 i18n · 谱面反作弊 · 观战聚合缓冲 · ✅ D1 Chat 限速 · ✅ C2 Registry 拆表（ISSUE-0012） | 每项带契约测试落地 |
| **中期**（月级） | ✅ B1 Tick 通电（倒计时，提前完成）· 一致性断言库 + 漂移哨兵 · 管理 API | 玩家可感知 + 护城河成形 |
| **择机** | A2 回源出队 · C1 server.rs 拆分 · 回放录制（Store 接口） | 绑定到相关功能进场时顺手做 |
| **远景** | Store 持久化 · 协议 v2 预案 · 多实例（仅当需求出现） | 文档立 flag，不动代码 |

## 总体判断

r0semi 的债务画像独特：**几乎没有"烂代码债"，全是"还没做完债"（B/C/D）与"优化引来的债"（A）**。
前者是单人项目时间问题，后者是所有高性能系统的宿命。修复优先级的关键是要**先暴露（B3 Metrics），
再决策（A2 是否值得重构）**——用数据引导投入，而不是凭猜测。
