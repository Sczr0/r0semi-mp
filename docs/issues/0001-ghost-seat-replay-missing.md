# ISSUE-0001：幽灵座位竞态——文档承诺的"表 miss 挂起重放"未实现

- 状态：**已解决（2026-08）**——重放已实现（ADR-0007），见文末修复记录
- 发现日期：2026-08
- 发现方式：架构评审（对比 ARCHITECTURE.md §4.9-3 与 phira-core 实际代码）
- 严重级：低（后果 = 名额泄漏，非崩溃；重连可自愈）
- 相关章节：ARCHITECTURE.md §4.9-3（第四竞态·幽灵座位）、§4.9-4（路由规则）

---

## 问题陈述

ARCHITECTURE.md §4.9-3 **承诺**了对"幽灵座位"竞态的兜底：

> 第四竞态·幽灵座位（评审 §8 二-1）：入房时序是 actor 返回 UserJoined → bus 应用表增量 → 发响应；客户端入房后立刻断线（RST 即时可见）时，生命周期任务查表路由 UserDisconnected 可能撞上**增量未应用**的窗口（bus 忙时拉大）——表 miss → 事实被丢 → 无 dangle 窗口 → 幽灵座位卡死 WaitForReady。修法（措辞收敛，评审 §8 二-4）：**表写仅经 bus 分发步骤（§4.9-4），生命周期任务只读；表 miss 时挂起重放**——current_thread 单线程下无数据竞争，但 await 交错仍存在，重放兜底

**实际代码未实现"重放"**——生命周期任务查表 miss 时直接忽略事实。

## 证据

### 文档承诺 vs 代码实际

| | 位置 | 行为 |
|---|---|---|
| 文档承诺 | ARCHITECTURE.md §4.9-3 | 表 miss 时**挂起重放** |
| 代码实际 | `crates/phira-core/src/lifecycle.rs` `dispatch()` | `room_of` 返回 None → `debug!("not in any room, skipping lifecycle dispatch")` → **return（丢事实）** |
| 代码实际 | `crates/phira-core/src/bus.rs` `room_of()` | 纯读路由表 `routes.get(&user_id)`，无重试/挂起机制 |

```rust
// crates/phira-core/src/lifecycle.rs —— 实际行为（无重放）
async fn dispatch(&self, user_id: i32, cmd: RoomCommand) {
    let Some(room_id) = self.bus.room_of(user_id).await else {
        debug!("user={user_id} not in any room, skipping lifecycle dispatch");
        return;   // ← 事实被丢，文档承诺的"重放"不在此处
    };
    ...
}
```

## 竞态路径（何时触发）

1. 客户端 A 发 `JoinRoom` → bus `route()` → actor 返回 `UserJoined` 事件
2. bus `process_events()`：解析 targets → **应用路由增量**（`routes.insert(A)`）→ 发响应 → 投递（`sink.deliver` 是 await 点）
3. 客户端 A 在步骤 2 完成前断开（RST 即时可见；bus 忙时窗口拉大）
4. server 连接层发 `Disconnected{A, epoch}` → 生命周期任务 `room_of(A)` 查表
5. **表 miss（增量未应用）** → 事实被丢 → A 的座位留在房间、`absent` 无 A、无人驱逐

## 影响评估

- **不是崩溃**：`room_of` 返回 `Option`，miss 走 `debug` 分支，无 panic 面
- **后果是名额泄漏**：幽灵座位永久占坑（8 人房少一个名额），房间满员时新玩家无法加入
- **可自愈**：A 重连（同 token）→ 注册表新 epoch → `UserReconnected` → 房间认为 A 一直在（absent 无 A）→ 正常继续；**仅当 A 永不重连时泄漏持续**
- **窗口窄**：需"join 后立即断线 + bus 恰好卡在 process_events 的 await 点"同时发生

## 候选解决方案

| 方案 | 描述 | 代价 |
|---|---|---|
| A. 实现文档承诺的重放 | 生命周期任务 `room_of` miss 时挂起（如带超时的重试/延迟再查一次），命中后再派发 | ~20-40 行 + 集成测试；需防无限重试（用户确实不在任何房间的常态路径不能被拖慢） |
| B. 应用侧兜底 | impl 在 `handle_user_joined` 后自身检查连接活性（不可行——连接事实归 core，§4.6 分工） | 违反架构分工，否 |
| C. 修复文档 | 承认"表 miss 丢事实"为已知限制，降级"重放"为未来项 | 零代码，但竞态真实存在，只是概率低 |
| D. 增量应用前置 | process_events 先应用路由增量再解析 targets（交换步骤 1/2） | **会破坏"离开者仍收到自己的 LeaveRoom"不变量**（§4.9-4 先解析后应用），否 |

**倾向**：方案 A（兑现文档承诺，带超时重放 + 测试）或 C（若评估后认为成本收益不划算，明确降级并记录）。决策走 ADR。

## 验收标准（已全部满足）

- 新集成测试：`ghost_seat_replay_recovers_missed_route`——慢 JoinRoom actor（30ms 窗口）+ 并发断线事实，断言 `UserDisconnected` 经重放最终派发
- 常态路径回归：`replay_gives_up_when_user_never_in_room`——用户从未入房，重放耗尽后放弃、不误派发
- `cargo test --workspace` 全绿（155）；check-deps.py 通过

## 修复记录（2026-08）

- **实现**：`lifecycle.rs` 新增 `lookup_room_with_replay`——`ROUTE_REPLAY_ATTEMPTS=3`（1 立即 + 2 重查 × 20ms），表 miss 挂起重放，仍 miss 才放弃（正常路径代价 ≤ 40ms 一次性）
- **ADR**：ADR-0007（路由表 miss 挂起重放）——覆盖范围论证（真实窗口微秒-毫秒级，ISSUE-0004 修复后进一步收窄）；替代方案：精确同步（跨任务耦合，否）/ 无限重放（常态路径被拖住，否）
- **测试**：+2（幽灵座位重放命中 / 重放耗尽放弃）
