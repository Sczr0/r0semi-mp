# ADR-0008：每连接命令限速（滥用控制"快端"防线）

- 日期：2026-08
- 状态：已接受
- 相关章节：ARCHITECTURE.md §4.9-2（缓解措施 b）、§4.9-9（滥用控制）、§11（滥用防护）

## 背景

§4.9-2 承诺"v1 采用：每连接限速"，§4.9-9 要求"滥用控制优先用每连接限速，不让队列压力触发断连"——但 ISSUE-0006 发现全库无频率限制代码。鉴权后连接可高频发 `CreateRoom`（spawn actor + channel）/`JoinRoom`/`SelectChart`/`Played`（回源官方 API），靠队列满 `Reject` 断连"惩罚"而非"预防"。

## 决策

**每连接令牌桶（简化版）**：`CommandLimiter` 记录每个受限命令类别的"上次允许时刻"，距上次 ≥ interval 才放行。**只限"贵"命令**（资源成本驱动）：

| 命令 | 间隔 | 依据 |
|---|---|---|
| `CreateRoom` | 1s（1/s） | spawn actor + channel，最贵 |
| `JoinRoom` | 200ms（5/s） | 入房流程 + 广播 |
| `SelectChart`/`Played` | 200ms（5/s） | 回源官方 API（配额宝贵） |

超限 → 回 `TooManyRequests` Business 错误（**新增契约错误码**，phira-api `RoomErrorCode::TooManyRequests`）——客户端可见，**不触发队列 Reject 断连**（兑现 §4.9-9"优先限速"）。热路径（Touches/Judges）不限（靠 DropIfFull + 帧上限）；`Authenticate` 不限（核心流程，登录失败限速留 Observer 未来项）。

## 后果

- 正面："快端"滥用防线就位（与 ISSUE-0004 的"慢端"踢出成对）；`CreateRoom` 洪泛被 1/s 挡在源头；回源命令 5/s 保护官方 API 配额；限速是 core/session 层行为，不进 bus、不动房间契约。
- 负面：`TooManyRequests` 是新契约变体（§5.6 已满足：`RoomErrorCode` 追加 + `#[non_exhaustive]` 无关枚举 + 契约测试补断言）；间隔是 v1 常量（可参数化进 config）；**语义叠加**——同连接 1s 内重复建房先撞限速（TooManyRequests）而非 AlreadyInRoom（e2e 测试已按此调整等待窗口）。

## 替代方案

- 全命令统一限速——被拒：热路径 Touches 60Hz 远超任何合理全局限速，且已由 DropIfFull 兜底。
- 只依赖队列 `Reject` 断连——被拒：惩罚而非预防（ISSUE-0006 修复前状态），滥用者先打爆资源再被断。
- 登录失败限速（Observer，§11）——被拒：`Authenticate` 不经 RoomCommand 流，Observer 拦不到（§7.3 已注明）；留阶段 4。
