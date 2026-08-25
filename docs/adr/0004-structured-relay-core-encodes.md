# ADR-0004：结构化转发（方案 A）——编解码归 core

- 日期：2026-08
- 状态：已接受
- 相关章节：ARCHITECTURE.md §6.5-17、§4.4、§4.8

## 背景

观战转播是性能热点：原版给每个接收者克隆整包（N 次序列化 + N 次深拷贝）。热路径（Touches/Judges，60Hz）的转发必须避免逐接收者编码，同时 impl 不能碰协议编码（§4.3-3 红线：impl 只认识 api，协议编解码是 core 的叶子部件）。

## 决策

**结构化转发**（方案 A）：core 解码一次（校验）→ 命令侧 `Touches{frames}`（Arc 共享）→ actor 查 live、计算 `targets = Specific(monitor_ids)` → 返回结构化事件 `RelayTouches{targets, frames}` → **core 用它的编码器编码一次为共享缓冲** → 投递给所有 monitor。总编解码目标：每命令 1 解 + 1 编，每接收者 0 次。帧数据用 `Arc<Vec<TouchFrame>>` 浅共享（clone = 引用计数 +1）。

## 后果

- 正面：impl 永不碰协议编码；帧数据零深拷贝；顺序语义是契约的一部分（Touches 与其它命令同通道保序）。
- 负面：热路径也走 `handle`（8 玩家 × 60Hz ≈ 500 次/s/房，每次多一次分配可接受）；`RelayTouches/RelayJudges` 是改写产物（协议无此概念），须按设计对待。

> **现状注记（2026-08）**：本 ADR 记录**决策**本身（结构化转发、编解码归 core）——该决策已实施。但"core 编码一次、每接收者 0 次"的**实现未兑现**（实际每接收者各自 `event_to_server` + `encode_packet`），见 ISSUE-0003。帧数据 `Arc` 浅共享已兑现。

## 替代方案

- ForwardRaw（bytes 直传）——被拒：bytes 依赖进 api 红线（§4.3-1），且 impl 触碰编码。
- 热路径旁路（跳过 handle 直接转发）——被拒：破坏可观察顺序（Touches 可能晚于 Abort 到达 monitor），顺序语义是契约的一部分（评审 §8）。
