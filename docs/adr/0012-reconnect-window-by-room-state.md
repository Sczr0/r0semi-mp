# ADR-0012：对局中重连窗口分级（命令化查询房间状态）

- 状态：已接受（2026-08，期1 C-03）
- 相关章节：ARCHITECTURE.md §4.6（时间/连接事实命令化）、§6.5-21/22（重连窗口与 Playing 断线）
- 关联需求：竞品差距 C-03——gooophira `playing_reconnect_grace` 独立配置 / jphira 5 分钟挂起；本仓库此前单一全局 `dangle_window` 10s，对局掉线 10s 即弃赛

## 背景

原规则：Playing 中断线**立即驱逐、无重连窗口**（规则 22）；非 Playing 断线 10s 窗口（规则 21）。
玩家体感：对局中一次掉线（wifi 抖动 / 切后台 / 应用被杀重开）10s 即弃赛，体验最差的一项。

需求：`reconnect_window`（非对局，保持 10s）与 `playing_reconnect_window`（对局中，默认 60s 可配）双配置。

## 架构约束（本 ADR 必须回答的决策点）

- **core 生命周期不认识房间态**（§4.6 契约分层：core 只认识 `phira-api`，房间状态是 impl 的私有知识）。
- 时间/连接事实必须命令化（§4.6）：impl 禁止开后台任务/定时器——窗口到期事实必须由
  core 生命周期任务（单一生产者）派发 `UserDangleExpired`。

## 决策

### 1. 窗口决策 = 命令化查询（`GetClientState`），零契约新增

core 在收到 `Disconnected` 时、派发 `UserDisconnected` **之前**，调用既有系统命令
`RoomCommand::GetClientState { user_id }`（§6.5-23，重连恢复已用，`command_needs_response`
已有回话通道），按其响应分级：

| 查询结果 | 窗口 |
|---|---|
| `ClientState(Some(cs))` 且 `cs.state == Playing` | `playing_reconnect_window`（默认 60s） |
| 其余（不在房 / 查询失败 / 非对局状态） | `reconnect_window`（10s，原语义） |

- 语义 = "断线瞬间房间是否在对局"（查询在标记缺席前）。
- 不新增命令、不新增响应变体（`#[non_exhaustive]` 不动）、bus 的 3 处 match 不动、
  契约测试不动（`GetClientState` 已有用例）。
- 拒绝"impl 报告"（在 `UserDisconnected` 响应里带"是否曾在对局"）：会改变既有
  系统命令契约、且查询路径本就是现成的权威状态；拒绝"core 缓存房间态"：
  core 不该持有房间态副本（分层污染 + 双写失真）。

### 2. impl：Playing 断线统一标记缺席（原规则 22 取消）

`handle_user_disconnected` 不再区分 Playing/非 Playing——统一 `absent.insert(user_id)`。
驱逐仍只由 `UserDangleExpired`（core 到期派发）触发；窗口长短由 core 分级决定。

### 3. 配置与默认

- `reconnect_window`：非对局，默认 10s（不变）。
- `playing_reconnect_window`：对局中，默认 60s；`Config` 双字段 + yml 双键
  （`playing_reconnect_window` 缺省时 = `reconnect_window` 语义由组合根显式传值决定）。
- `LifecycleTask::with_playing_reconnect_window(window)` 注入（构造默认 = `dangle_window`，
  测试可只覆盖其一）。

## 影响

- **契约测试变更**（`phira-contract/src/rooms.rs` Playing 断线用例）：原"立即驱逐、
  无重连窗口"断言改为"标记缺席 + `UserDangleExpired` 驱逐 + 幂等"。这是**语义变更**
  （规则 22 取消），非只加只读——故本 ADR 记录，且契约测试同 commit 更新。
- impl 删 `on_playing_disconnect`（原 Playing 立即驱逐分支）。
- ARCHITECTURE.md 规则 12/21/22/24 与 §6 边界用例注释同步。
- 心跳断线判定（最后包后 10s）与重连窗口独立计时不变；最坏完成踢人
  = 心跳判定（~10s）+ 对局窗口（60s）≈ 70s，对局内最多挂机 1 分钟（可配）。

## 备选方案（否决）

1. **impl 报告"曾在对局"**：在 `UserDisconnected` 的返回里带房间状态位 → 改系统命令
   契约 + 需处理"报告时已非 Playing"的竞态（掉线瞬间后房间可能已结束）。
2. **core 缓存房间状态**：破坏分层（core 认识房间态）+ 双写一致性问题。
3. **单一窗口取长值**：简单但对非对局也延长，弱化占位清理，不满足"非对局仍 10s"验收。

## 验收

- e2e：对局中掉线按 `playing_reconnect_window` 处置（短窗口注入加速观察：
  非对局 400ms 到期不驱逐、对局窗口 3s 到期驱逐），非对局仍走 `reconnect_window`
  （既有 `disconnect_evicts_from_room` 钉 10s 语义）。
- 契约测试：Playing 断线不再立即驱逐（回归）。
