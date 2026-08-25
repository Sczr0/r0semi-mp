# ISSUE-0009：旧 TCP 命令无 epoch 校验——§4.9-3 核心不变量未落地（双活连接竞态）

- 状态：**已解决（2026-08）**——命令侧 epoch 校验落地 + 回归测试，见文末
- 发现日期：2026-08
- 发现方式：原子性/幂等性全面审查（对照 §4.9-3 三条不变量逐条核对代码）——grep 全量核查 epoch 在 server.rs 的所有出现位置
- 严重级：**中**（竞态正确性缺口；需"同 id 二次鉴权 + 旧连接继续发包"同时成立才可达，正常客户端不触发，但违反文档明示核心不变量）
- 相关章节：ARCHITECTURE.md §4.9-3（会话纪元 / 旧连接失效）、§6.5-19（身份与重连）

---

## 问题陈述

§4.9-3 把"旧连接失效"列为输入侧闭环的三条不变量之一，文档明文承诺**两条**：

1. **关闭旧 TCP、取消旧会话任务**（替换会话时 epoch+1，其死亡事实随之消失）
2. **替换后旧 TCP 到达的命令以 epoch 校验拒绝**——否则同 id 双活连接的命令混进同一房间 channel，顺序语义被未定义交织

代码实际只落地了"事实侧"的 epoch 校验（`lifecycle::is_current` 过滤旧会话的断线/窗口到期事实），**客户端命令流完全没有 epoch 校验，且重连不关闭旧 TCP**。

## 证据（文档承诺 vs 代码实际）

```rust
// 文档 ARCHITECTURE.md:511-512（§4.9-3）
//   "替换会话时 epoch+1 且关闭旧 TCP、取消旧会话任务"
//   "替换后旧 TCP 到达的命令以 epoch 校验拒绝——否则同 id 双活连接的命令混进同一房间 channel"

// 代码 server.rs:1101 handle_frame（客户端命令路径，grep 核实）
//   epoch 在 server.rs 的全部出现位置：908/939/961/987（心跳监控/踢出/收尾）+ 1210-1212/1227（鉴权赋值）
//   ——客户端命令派发路径（handle_frame → bus.dispatch）零 epoch 校验，命令直通

// 代码 server.rs:1189 authenticate_flow（重连路径）
//   1210  registry.register(user_id, name) → epoch+1
//   1217-1218  ctx.sink.register(user_id, ...) → 仅替换投递槽位
//   ——没有关闭旧 TCP、没有取消旧会话任务（旧连接的 recv 任务继续存活、继续派发命令）

// 代码 lifecycle.rs:91 is_current
//   ——只校验生命周期事实（Disconnected/DangleExpired），不覆盖客户端命令流
```

## 影响评估

1. **同 id 双活连接**：重连后旧连接仍活着、仍能发 Chat/Ready/Touches 等命令，以同一 `user_id` 混进房间 channel——正是文档说要防的"顺序语义未定义交织"。
2. **AlreadyInRoom 的 check-then-act 竞态**：`bus.rs:290` 的 `contains_key` 预检（读锁、立即释放）与路由增量应用（actor 处理 UserJoined 后）之间存在 await 窗口；单连接假设下不可达，**双活连接下两个 JoinRoom 可同时通过** → 同一用户同时在两个房间 → 路由表 last-write-wins，后 join 的房间接管其命令。
3. 顺带：`AUTHED_CONNECTIONS` 按连接计数（每连接 fetch_add），同用户双活 = 占 2 个已鉴权名额（上限 1000），攻击者可用自己 token 多开占满——非本次核心问题，但双活修复后自然消失。

**可达性**：需客户端重连后旧 socket 仍存活并继续发包（正常客户端重连后自己会关旧 socket）；攻击者可控但收益低（只能让自己混乱）。**竞态正确性缺口，不是上线级故障。**

## 候选解决方案

| 方案 | 描述 | 代价 |
|---|---|---|
| **A（推荐）** | `SessionRegistry` 暴露 `current_epoch(user_id)`；`handle_frame` 派发前校验 `state.epoch == current_epoch`，不匹配则断开旧连接（或拒绝命令） | ~5-10 行 + core 测试；符合现有架构（epoch 已存在，只差一个读入口 + 一个校验点） |
| B | `authenticate_flow` 替换时主动关闭旧 TCP（server 侧维护 user_id → 旧连接句柄表，替换时 abort） | 改动大（需跨任务传递句柄）；且"命令拒绝"仍需要 A 的校验 |
| C | 维持现状，文档 §4.9-3 改为"已知限制" | 零代码；但不变量承诺悬空，幽灵座位同类问题可能复现 |

**倾向**：A——`is_current` 已存在（lifecycle.rs:91），把同一校验逻辑在客户端命令入口复用即可，与 §4.9-3"事实带 epoch、命令也带 epoch 校验"的原始设计一致。

## 验收标准

- **A**：集成测试构造"同 id 二次鉴权 + 旧连接发命令"场景：旧连接命令被拒（或断开）、新连接不受影响、路由表无第二次 UserJoined 增量
- 幽灵座位/重连族现有测试（core 集成测试）保持全绿
- `cargo test --workspace` 全绿；clippy/check-deps 通过
- 若改契约（如给 `CmdCtx` 加 epoch 字段）走 §5.6 + ADR

## 修复（2026-08，已解决）

- **`phira-core/src/lifecycle.rs`**：`SessionRegistry` 新增 `current_epoch(user_id) -> Option<u64>` 读入口（与 `is_current` 同源、同一把锁）
- **`phira-server/src/server.rs` `handle_frame`**：已鉴权命令入口加 epoch 校验——`current_epoch(user_id) != state.epoch` → 拒绝该命令 + `backpressure.force_close()`（借 kicker 1s 轮询拆掉旧连接、释放其已鉴权名额，与内存守卫踢出同机制）；不匹配的旧连接死亡事实（Disconnected）仍被 lifecycle `is_current` 忽略，新连接不受影响
- **回归测试**：`crates/phira-server/tests/stale_connection.rs`——A 鉴权建房（epoch 1）→ B 同用户顶替（epoch 2）→ A 的 Chat 被拒（B 收不到广播）→ B 的 Chat 正常广播回自己 → A 被 kicker 拆线
- 不新增协议字段、不改契约（校验在 server 连接层完成，`CmdCtx` 无变化）

## 验收标准（已满足）

- `cargo test --workspace` 全绿（178，含 stale_connection 回归）；连续多轮无偶发失败
- `cargo clippy --workspace --all-targets -- -D warnings` / `check-deps.py` / `check-adr.py` 通过
- 双活场景行为：旧连接命令一律拒绝；新连接命令/广播不受影响；旧连接 1-2s 内被拆线（已鉴权名额释放）

## 关联

- §4.9-3（输入侧竞态闭环）：本 issue 是三条不变量中唯一未闭环的一条（另两条——窗口边界、单一生产者——已落地）
- ISSUE-0001（幽灵座位重放）：同属"断线/重连语义"族，互为补充（0001 修表 miss，本 issue 修表命中后的双活）
- §6.5-19/23（身份与重连）：重连编排的完整性依赖本校验
