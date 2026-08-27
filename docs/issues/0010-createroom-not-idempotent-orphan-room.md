# ISSUE-0010：CreateRoom 非幂等——响应丢失重试得 RoomIdOccupied，且可能留下孤儿房间

- 状态：**已处置（2026-08-27）**——方案 D→C 落地（deployment.md §9 客户端指引 + 本 issue 记档）；协议级根因留待演进，见文末处置记录
- 发现日期：2026-08
- 发现方式：幂等性全面审查（"重复执行同一操作是否安全"逐个命令核对）——CreateRoom 是唯一无客户端幂等键的入房类命令
- 严重级：**低**（需"建房响应丢失 + 客户端用同 id 重试"同时成立才触发；孤儿房间占内存极小；协议级限制，原版同病）
- 相关章节：ARCHITECTURE.md §6.5-27（重复入房）、§6.5-6（空房自毁）、§4.9-3（重连恢复）

---

## 问题陈述

`ClientCommand::CreateRoom { id }`（协议载荷只有 room id，无客户端生成的幂等键）。若**响应在网络上丢失**，客户端无法区分"房间没建成"与"房间建成但响应丢了"：

- 重试同 id → `RoomIdOccupied`（bus.rs:298-299）——客户端被迫换 id；
- 换 id 重试成功后，**旧房间成为孤儿**：host 已在重连中恢复座位（`UserReconnected` → `impl-rooms-v1` lib.rs:726 `absent.remove`），`users` 非空 → `evict` 的空房自毁判定（lib.rs:195 `users.is_empty()`）永不触发 → 房间永久存活，客户端却以为它不存在。

孤儿房间的负面面：常驻内存（actor + 1024 槽 channel，~几 KB）、占据 `/rooms` 公开列表条目（RoomListSink 无 TTL）。

## 证据

```rust
// 协议层（phira-api/src/proto.rs）：CreateRoom 载荷只有 id——无幂等键（tag 5）
// 原版 phira-mp 同协议，同样无幂等键（协议级限制，非本实现引入）

// bus.rs:298-299：同 id 重试 → RoomIdOccupied
if rooms.contains_key(id) {
    return Err(business(RoomErrorCode::RoomIdOccupied, "room id occupied"));
}

// impl-rooms-v1/src/lib.rs:195：空房自毁只看 users.is_empty()
if self.users.is_empty() {
    events.push(RoomEvent::RoomClosed { .. });
}
// lib.rs:726：重连恢复座位（absent.remove）——host 回到旧房间，自毁永不触发
RoomCommand::UserReconnected { user_id, .. } => { self.absent.remove(&user_id); }
```

**场景推演**：CreateRoom(abc) → 响应丢失 → 客户端同 id 重试 → RoomIdOccupied → 客户端换 CreateRoom(def) 成功 → 用户实际在 abc（host、座位已恢复）与 def（新建）两个房间中，路由表 last-write-wins 指向 def；abc 永久存活，无人会对其发 LeaveRoom（客户端不知道自己在里面）。

## 影响评估

- **低**：触发需"响应丢失 + 客户端重试"；孤儿房间 ~几 KB 内存 + 公开列表可见，无安全影响（孤儿房不可加入？——**可加入**：abc 在 SelectChart 状态、未锁，其它玩家可 JoinRoom 进入；若 host 不再回来，房间有玩家时又回到正常生命周期。孤儿的主要危害是"宿主用户认知不一致"）
- **不可根治于本项目**：协议无幂等键，客户端重试语义由客户端决定——社区客户端可自行保证（见候选 C）

## 候选解决方案

| 方案 | 描述 | 代价 |
|---|---|---|
| A. 协议级幂等键 | 官方协议加客户端生成的 request_id（重试携带同 id → 服务端返回已建房间状态而非 Occupied） | 需官方协议演进 + 客户端配合，**不在本项目**（记入未来项） |
| B. 全员 absent 超时自毁 | 房间内全员 absent 持续超过 N 分钟 → 自毁（区别于 10s 重连窗口的驱逐语义） | 需契约演进（新系统命令或扩展 `UserDangleExpired` 语义）+ ADR + 契约测试补用例；风险：慢速断线重连被误杀（N 需远大于重连窗口） |
| C. 客户端策略 | 建房用唯一 id（uuid 后缀）避免同 id 重试；文档指引 | 零服务器代码；依赖客户端自觉 |
| D. 记档（本 issue） | 保留为已知限制，等 B 或协议演进时一并处理 | 零代码 |

**倾向**：**D → C 衔接**——本 issue 先落档；部署文档加一条"客户端建房建议用唯一 id"指引（零代码）；B 留作未来运维选项（若孤儿房间在实测中真出现）。

## 验收标准

- **D**：无代码变更，仅本 issue 存档 + deployment.md 客户端指引
- **B**（若实施）：契约测试补"全员 absent 超时 → RoomClosed"用例；`cargo test --workspace` 全绿；ADR 记录
- 无论哪个方案：`/rooms` 列表不得因孤儿房间出现任何 panic 或记账失衡

## 关联

- §6.5-27（重复入房）：AlreadyInRoom 去重存在，但那是"去重"，不是"幂等重放"——本 issue 是它缺失的补集
- §6.5-6（空房自毁）：自毁条件只看 `users.is_empty()`，absent 恢复座位后条件不成立——本 issue 的根因所在
- §4.9-3（重连恢复）：`UserReconnected` 恢复座位是孤儿形成的必要条件（host 重连回来了）
- ISSUE-0009（旧 TCP 无 epoch 校验）：同属"重连语义"族；0009 修复后本 issue 的双活部分不会更糟，两者独立

---

## 处置记录（2026-08-27，方案 D→C 衔接落地）

按本 issue 倾向执行：**D（记档）→ C（部署指引）**，零代码变更：

- `docs/deployment.md` §9 新增 FAQ 条目："房间 ID 已被占用"的成因（响应丢失 + 同 id 重试）、
  **建房 id 建议带唯一后缀**指引、孤儿房风险指针回本文。
- B 方案（全员 absent 超时自毁）维持"留作未来运维选项"不变——当前无实测孤儿房案例驱动。

验收标准 D 满足：无代码变更、issue 存档 + 部署文档客户端指引齐备。
