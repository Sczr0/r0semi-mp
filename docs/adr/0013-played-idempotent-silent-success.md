# ADR-0013：Played 重复上报口径拍板——静默成功（幂等）

- 状态：已接受（2026-08，期2 C-04）
- 相关章节：ARCHITECTURE.md §6.5-10（上报成绩）、§6.5-27（重复入房/重复操作）、§4.9-2 规则 2（A2 两段式受理）
- 关联需求：竞品差距 C-04——gooophira `Played` 重试静默幂等（competitor-review.md:74），本仓库此前 `AlreadyUploaded` 显式错误；对齐 ISSUE-0010（CreateRoom 非幂等处置）的幂等哲学

## 背景

A2 两段式（§4.9-2 规则 2）：`Played` 第 1 段受理（幂等预检 + 登记 in-flight，立即回 `Ok`），
第 2 段 `RecordFetched` 回源回注（入 `results` + 广播 `Played` 事件）。

第 1 段幂等预检现状（`impl-rooms-v1/src/lib.rs:723-740`）：`results` / `aborted` /
`inflight` 任一命中 → 回 `AlreadyUploaded` 显式错误。

**问题**：弱网/抖动下客户端重试 `Played`（首次响应丢失），会看到"已上传"红字——
玩家困惑，且与 gooophira 静默幂等行为不一致（gooophira 重试直接成功）。

## 决策

### 1. Played 重复上报 → **静默成功（幂等）**

`handle_played` 幂等预检命中（`results` / `aborted` / `inflight` 任一）时，
返回 `(Some(RoomResponse::Ok), Vec::new())`——与首次受理同响应，且不产生事件、
不重复回源。语义 = "成绩以首条为准"：重复上报只是幂等重放，服务端状态不变。

- 对齐 gooophira：`Played` 重试静默幂等。
- 对齐 ISSUE-0010：该 issue 判定 `CreateRoom` 协议无幂等键、**无法服务端根治**，
  故走"客户端唯一 id + 文档指引"。而 `Played` 有天然幂等锚点（`user_id` 房内唯一、
  成绩以首条为准），服务端**可以**根治——两条线合流为同一哲学：
  **"重复执行同一操作必须安全"：能服务端幂等的服务端做（Played），
  不能的协议级限制给客户端指引（CreateRoom）**。
- 可观测性：重复上报仍在 debug/trace 留痕（不静默吞没），运维可辨重试与攻击。

### 2. Abort 对"已入账成绩"→ **维持 AlreadyUploaded 显式错误（不改）**

`handle_abort`（`impl-rooms-v1/src/lib.rs:856-867`）对 `results` / `inflight` 命中
仍回 `AlreadyUploaded`。这是**不同语义**：不是"重复上报"，而是"成绩已计、不可撤销"——
`Abort` 想撤销已入账成绩，属协议违规操作，应显式拒绝（gooophira 对已计成绩 abort 亦拒绝）。
本 ADR 只改 `Played` 幂等口径，`Abort` 维持错误留档。

## 影响

- `impl-rooms-v1/src/lib.rs`：`handle_played` 幂等预检命中 → `(Ok, vec![])`。
- 契约测试 `phira-contract/src/rooms.rs`：536-538（受理段重复上报）、587-589
  （被结算 aborted 后重试上报）断言由 `AlreadyUploaded` 改为 `Ok` + 无事件。
- `AlreadyUploaded` 错误码保留（`handle_abort` 仍用），契约测试补注释说明边界。

## 备选方案（否决）

1. **维持 AlreadyUploaded 显式错误**：弱网重试玩家仍见红字，与 gooophira 不一致，
   不符合"竞品对齐 + 弱网重试不惩罚"验收动机。
2. **重复上报静默 + 仍错误区分"首次 vs 重试"**：协议无客户端请求号，
   无法区分"首次失败"与"重试"（无幂等键），徒增复杂度且不可靠。

## 验收

- 契约测试：重复上报（受理段 / aborted 后）→ `Ok` + 无事件；`Abort` 已入账仍
  `AlreadyUploaded`。
- `cargo test --workspace` 全绿（含 conformance / e2e）。
- `tools/check-adr.py` 通过（编号 0013 连续）。
