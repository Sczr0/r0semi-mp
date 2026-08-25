# ISSUE-0007：原版 `game_time` 钩子（玩家最后触摸时间）未移植——`GetClientState` 断线恢复缺"玩家进度"维度

- 状态：**待解决**
- 发现日期：2026-08（对照原版 TeamFlos/phira-mp 源码逐行评审）
- 发现方式：clone 原版 `phira-mp-server`，对比 Touches/Judges 热路径处理与本重写 `impl-rooms-v1`
- 严重级：低（当前零影响——原版该字段亦"只写不读"，属预留能力缺失，非行为差异）
- 相关章节：ARCHITECTURE.md §6.5-23（重连恢复 GetClientState）、§6.5-16/17（触摸流热路径）、§4.6（时间事实命令化）

---

## 问题陈述

原版 `phira-mp`（TeamFlos，Apache-2.0）在每个用户会话上维护一个 `game_time` 字段，语义为**"该玩家最近一次上报触摸帧的谱面内时间（秒）"**——即玩家当前打歌进度。本重写版无对应物，且 `GetClientState`（§6.5-23 重连恢复）只返回房间快照（状态/用户列表/live/锁/循环/房主/ready），**不含任何"玩家打到哪"的进度维度**。

原版实现（三处，均无读取方）：
- `session.rs:393` 写入：收到 `Touches` 时 `user.game_time.store(frames.last().time.to_bits(), SeqCst)`——取本包最后一帧时间戳 = 当前进度点；f32 经 `to_bits()` 存 `AtomicU32`（标准库无 `AtomicF32` 时代的写法）
- `room.rs:226` 重置：`reset_game_time` 设 `f32::NEG_INFINITY.to_bits()`——哨兵值区分"本局未开打"（负无穷）与"已开打"（≥0）
- 重置时机：`RequestStart` 成功（`session.rs:602`）与全员 ready → `StartPlaying`（`room.rs:247`）

**全原版代码库 0 处 `load()` 读取**——死字段，作者预留给断线恢复/进度裁决但未实现消费方。

## 证据（原版 vs 本重写）

```rust
// 原版 session.rs:388-393（Touches 热路径）
ClientCommand::Touches { frames } => {
    get_room!(~ room);
    if room.is_live() {
        if let Some(frame) = frames.last() {
            user.game_time.store(frame.time.to_bits(), Ordering::SeqCst);
        }
        tokio::spawn(async move { room.broadcast_monitors(...).await; });
    }
    None
}

// 本重写 impl-rooms-v1/src/lib.rs:811（Touches 分支）
RoomCommand::Touches { frames } => {
    // live 时只转发给 monitor（§6.5-16）——无任何进度记录
    if self.live {
        let targets = Targets::Specific(self.monitors.keys().copied().collect());
        (None, vec![RoomEvent::RelayTouches { ..., frames }])
    } else { (None, Vec::new()) }
}

// 本重写 to_client_state（lib.rs:116-137）：返回字段 = id/state/live/locked/cycle/is_host/is_ready/users
// ——无"每用户 last_touch_time"维度
```

## 影响评估

- **当前零影响**：原版该字段亦无消费方；协议、转发语义、可观察行为均一致（Oracle 字节级对照不受影响）
- **预留能力缺口**：§6.5-23 断线恢复只能恢复"房间状态"，不能回答"该玩家断线前打到了谱面 X 秒"。未来功能无数据基础：
  - 断线重连后**续观战/续玩**需要断点时间
  - **"断线时进度 ≥X% 算完赛"**类结算规则需要断线瞬间进度
  - **防挂机**（game_time 停住 = 未在打）需要进度推进检测
- **移植成本低**：热路径命令已携带全部所需数据（`Touches.frames.last().time`），不新增协议字段

## 候选解决方案

| 方案 | 描述 | 代价 |
|---|---|---|
| A. 移植到 actor 状态（推荐，随断线恢复一起做） | `RoomV1` 增 `HashMap<i32, f32>`（user_id → last_touch_time）；handle Touches 分支更新（取 `frames.last().time`）；`handle_request_start` 成功时清零（负无穷或 Option）；`handle_get_client_state` 扩展返回给重连用户 | ~30-50 行 + 契约测试补用例；`ClientRoomState` 若加字段属**契约变更**，走 §5.6（枚举加字段 + 契约测试 + 版本检查） |
| B. 暂不移植（先记文档） | 保留本 issue 作为"断线恢复设计时的已知钩子"，等 §6.5-23 真正做进度恢复时一并设计 | 零代码；但信息会随本 issue 沉淀 |
| C. 照搬原版（不推荐） | 全局 `AtomicU32` 挂在会话上——**违反本重写 §4.6 命令化纪律**（进度事实来自命令、状态在 actor 内），且跨房间泄漏 | 与架构冲突 |

**倾向**：**B → A 的衔接**——本 issue 先落档；当 §6.5-23 断线恢复进入实现范围时按 A 移植，字段放 actor 状态而非全局。

## 验收标准

- **A**：`handle Touches` 后 `GetClientState` 能返回该用户最后触摸时间；`RequestStart` 后归零；契约测试断言（含空 frames 包不 panic、非 live 不记录）
- 若 `ClientRoomState` 加字段：契约测试 + `cargo-semver-checks` 通过；走 §5.6 流程
- `cargo test --workspace` 全绿；clippy/check-deps 通过
- **B**：无代码变更，仅本 issue 存档

## 关联

- §6.5-23（重连恢复 `GetClientState`）：唯一已有消费出口；当前返回房间快照、无进度维度
- §6.5-16（Touches 只转 monitor）：进度数据来源是热路径命令，顺带记录零成本
- §4.6（时间/连接事实必须命令化）：决定移植形态必须是 actor 状态（方案 A），禁止全局原子（方案 C）
- 与 ISSUE-0001（幽灵座位重放）同属"断线/重连语义"族；互不依赖
