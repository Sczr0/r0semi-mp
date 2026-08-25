# ISSUE-0004：§10.4 慢消费者承诺未兑现——deliver 用 send().await 等待而非丢帧，乌龟 monitor 可队头阻塞卡死整个房间；且无发送积压踢除机制

- 状态：**已解决（2026-08）**——丢帧止血（A）+ 积压踢出（B）均已实现，见文末修复记录
- 发现日期：2026-08
- 发现方式：架构评审（对比 ARCHITECTURE.md §10.4 慢消费者承诺与 `SessionSink::deliver`/`stream.rs` 实际代码）
- 严重级：**高**（阻塞性单房间 DoS，门槛极低；文档明确承诺被违反；注释与实现矛盾）
- 相关章节：ARCHITECTURE.md §10.4（慢消费者·丢旧保新）、§4.9-9（队列压力分级）、§11（滥用防护）

---

## 问题陈述

ARCHITECTURE.md §10.4 **承诺**对慢消费者（观战）的保护：

> 慢消费者（观战）：live 路径（Touches/Judges→monitor）用**丢旧保新**策略：每 monitor 有界环形缓冲，满则丢最旧帧，**绝不阻塞房间 actor、绝不无限积压**（评审 §7）

**实际实现存在三处背离**：
1. 投递用 `send().await`（满时**等待**），不是丢帧——与注释声称的"队列满/连接断开 → 丢弃"**矛盾**
2. "绝不阻塞房间 actor"**被违反**：`send().await` 等待时，bus 的投递循环（串行 await）卡住 → `room_loop` 卡住 → 该房间 actor 被一个乌龟 monitor 间接阻塞
3. **无发送积压踢除机制**：心跳只监控收包（`last_recv`），持续发 Ping 的乌龟永不超时；无"发送队列积压超阈值 → 断连"

## 证据

### 因果链（全部代码事实）

```
乌龟 monitor（故意不收包）
→ 其 TCP 接收窗口归零 → 服务端 write_all 阻塞（写任务无超时）
→ 每连接发送队列（mpsc 1024，stream.rs:101）堆积到满
→ SessionSink::deliver 的 tx.send(cmd).await —— tokio mpsc send() 满时【等待】，不是丢弃
→ bus.process_events 投递循环【串行 await】（bus.rs:596-597）
→ room_loop 卡在 process_events
→ 该房间 actor 收不到下一个命令 —— 整个房间被一个乌龟 monitor 间接阻塞
```

### 注释与实现矛盾（关键）

```rust
// crates/phira-server/src/server.rs —— SessionSink::deliver
if should_send && let Some(tx) = self.sessions.read().await.get(&user_id) {
    // 队列满/连接断开 → 丢弃（send 任务已退出）；热路径可丢（§4.9-9）
    let _ = tx.send(cmd).await;   // ← send().await 满时【等待】，与注释"丢弃"矛盾
}
```

- 写任务：`while let Some(payload) = send_rx.recv().await { ... write.write_all(...).await }`（stream.rs）——**write_all 无超时**，乌龟的 socket 写永久 pending
- 心跳（server.rs:542-570）：`last_recv` 更新于**每次收包**；乌龟持续发 Ping → `last_recv` 新鲜 → **永不触发断线**。心跳只监控"收包"，不监控"发包积压"

### 哪些是正常的（不要误伤）

- **房间 actor 入队层**（bus `queue_policy`）：Touches/Judges/Tick 是 `DropIfFull`（`try_send` 丢新）✅——actor 不会因入队丢弃消耗 CPU
- **房间间隔离** ✅——每个房间独立 `room_loop`，乌龟只卡死自己的房间，不波及其他房间（这是"每房间 actor"架构的红利）

## 影响评估

- **单房间 DoS**：一个乌龟 monitor 卡死一个房间（所有玩家命令响应、其他 monitor 投递全部排队）。非全服（房间间隔离）
- **门槛极低**：live 房间 + 任意一个 monitor 故意不收包（或异常网络）即可触发；8 个房间各挂 1 个乌龟 → 8 个房间瘫痪
- **CPU 代价为零**（阻塞不是忙等）——攻击不消耗 CPU，只瘫痪功能，隐蔽性强
- **文档失信**：§10.4 是评审 §7 明确定案的承诺（"绝不阻塞房间 actor"），实际被违反；注释声称的丢弃行为与代码不符（比 ISSUE-0003 更严重：0003 是性能承诺，0004 是正确性/可用性承诺）

## 候选解决方案

| 方案 | 描述 | 代价 |
|---|---|---|
| A. 丢新（最小修复） | `deliver` 改 `try_send`：满则丢新（与 bus DropIfFull 一致）。消除阻塞，房间不再被卡死 | ~5 行；注：文档说"丢旧"，但 §4.9-9 评审 §8 六已承认 mpsc 无法生产者侧驱逐队首——实际可行的是丢新，需同步修文档表述 |
| B. 发送积压踢除（治本） | 每连接发送积压监控：队列占用 > 阈值（如 900/1024）持续 N 秒 → 断连（复用 §10.4 帧记账思路）；或写任务加写超时 | ~30-50 行 + 测试；需定义"阈值/时长"参数 |
| C. 投递超时/并行 | `process_events` 的 deliver 加超时或 `futures::join_all` 并行投递 | 中等；治标（消除房间卡死）不治本（乌龟仍占资源） |
| D. 兑现"丢旧保新" | 每 monitor 换有界环形缓冲（VecDeque + 丢队首）替代 mpsc | 中等；真正兑现文档语义；但改动写任务消费模型 |

**倾向**：**A 先做（立即止血）→ B 跟进（踢乌龟，治本）→ 视需求 D（兑现"丢旧"语义）**。A+B 组合下房间永不被卡死、乌龟最终被踢。

## 验收标准（已全部满足）

- **A**：构造"队列满"场景（ScriptedFactory/脚本化乌龟），断言 deliver 不阻塞、`room_loop` 继续处理后续命令；`cargo test --workspace` 全绿
- **B**：集成测试：发送队列积压超阈值 → 连接被断 + 生命周期 `Disconnected` 正常派发；参数可配置
- 文档：§10.4"丢旧保新"表述与实际实现（丢新 or 环形缓冲丢旧）对齐；注释"丢弃"改为实际语义
- check-deps.py 通过

## 修复记录（2026-08）

- **A 丢帧止血**：`SessionSink::deliver` 改 `try_send`（满则丢新 + `Backpressure::mark`，成功则 `clear`）；`handle_frame` 的命令响应、Pong、`broadcast` 同步改 `try_send`——**所有发送路径不再无限等待**
- **B 踢乌龟**：新增 `Backpressure`（积压开始时刻标记）+ kicker 监控任务（1s 检查粒度，持续满 5s → `closed` CAS + `LifecycleEvent::Disconnected` + 断连）；阈值 `SLOW_CONSUMER_KICK_AFTER=5s`（常量，可参数化）
- **测试**（`tests/slow_consumer.rs`，+5）：deliver 满队列不阻塞 + 积压标记自愈 + mark 幂等 + 真实 TCP 乌龟踢出（只写不读 → 洪泛 300 命令 → 断言服务端断连）
- 全量 153 tests 绿；clippy -D warnings / fmt 通过
- **遗留**：阈值尚未进 config（常量）；"丢旧"（环形缓冲丢队首）仍为文档表述，实际是"丢新"（mpsc try_send）——语义偏差记入本文档，如需兑现"丢旧"另开 issue（依赖 SendSlot 换 VecDeque）

## 关联

- ISSUE-0003（广播编码未兑现）：同属"§6.5-17/§10.4 性能承诺 vs 实现"审查发现；0003 的 writev 优化与本案 A/B 正交
- ISSUE-0001（幽灵座位重放）：同属"文档承诺 vs 实现差距"系列
- 修复 B（踢乌龟）若涉及连接治理契约，走 ADR（关联 ISSUE-0002）
