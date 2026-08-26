# ISSUE-0012：SessionRegistry 只进不出——长寿命进程慢性增长（且不可简单删除，需拆原子表）

- 状态：**已解决（2026-08）**——方案 A（拆 epochs/names 两表）已落地，见文末修复记录
- 发现日期：2026-08
- 发现方式：竞品横评审计中核查生命周期注册表生命周期（对照 §4.9-3 不变量与 §10.4 记账平衡精神，grep 全量核验 registry 的增删路径）
- 严重级：**低**（持久进程慢性增长；量级虽小，但违反项目自身"记账平衡"的文档承诺，且与 ISSUE-0009 的 epoch 不变量耦合需谨慎处理）
- 相关章节：ARCHITECTURE.md §4.9-3（会话纪元 / 注册表）、§6.5-19（身份）、ISSUE-0009（epoch 校验，已解决）

---

## 问题陈述

`SessionRegistry` 记录 `user_id → (epoch, name)`，`register()` 只插入、从不删除：

```rust
// lifecycle.rs:47 定义 / :71 register 插入
pub struct SessionRegistry {
    inner: Mutex<HashMap<i32, (u64, String)>>,
}
pub fn register(&self, user_id: i32, name: String) -> u64 {
    let m = self.inner.lock()...;
    let epoch = m.get(&user_id).map_or(0, |(e, _)| *e) + 1;
    m.insert(user_id, (epoch, name));   // 只 insert，无 remove
    epoch
}
```

grep 全量核验：`inner.remove` / `m.remove` 在 lifecycle.rs 中**零出现**（server.rs 的 remove 针对的是 sessions/IP 限额表，非 registry）。每个自进程启动以来见过的不同 user_id 永久占 `(i32, u64, String)` ≈ 40–80B。长跑公开服（用户流转高）→ 无界缓慢增长。

## 证据（文档承诺 vs 代码实际）

```rust
// 生命周期事实侧（lifecycle.rs dispatch）：UserDangleExpired 处理后不触碰 registry
//   ——既不删条目，也不标记"可回收"。用户彻底离线后，其 (epoch, name) 永久驻留。

// 注释（lifecycle.rs:50-52）交代了 name 的用途：
//   "昵称存这里：CreateRoom/JoinRoom 派发时需要（§6.6 表 2）"
//   —— name 只在"在线用户被派发进房"这一瞬才是必需。
```

## 影响评估

- **内存**：单个条目 40–80B；千用户社区服务器数年 ≈ 数十 KB～数百 KB，量级小时不致命，但与"内存最小化"P0 与 §10.4 记账平衡精神相悖，属慢性侵蚀；
- **行为依赖（关键）**：epoch 必须**单调递增**且**不回收**——否则重连用户 epoch 回退，可能撞上 IDESSUE-0009 修复所依赖的 `current_epoch != state.epoch` 判定：
  - 现设计下 `current_epoch` 对"从未见过或已删除"的用户返回 None → 与 state.epoch 不匹配 → 拒（安全）；
  - 但若**删除条目**后该用户重连得 epoch 1，而此时恰好有**旧进程时期遗留的僵尸 TCP 连接**（其 state.epoch 也是 1）仍存活 → `1==1` 被放行 → **ISSUE-0009 漏洞复活**。
  - 因此绝不能"在用户离线时整条删除"。

## 候选解决方案

| 方案 | 描述 | 代价 |
|---|---|---|
| **A（推荐）** | 拆分两表：`epochs: HashMap<i32, u64>`（**永不删除**，8B/用户，量级可忽略）+ `names: HashMap<i32, String>`（可淘汰：用户彻底离线且不在任何房间时移除；dispatch 需要 name 时由 authenticate 重新注入） | 改动小；保住 epoch 单调不变量，只释放 name 字符串的多数驻留 |
| B | 维持单表，接受增长，仅把删除时机设为"进程定义外的运维重启" | 零代码；但要改文档把"无界增长"从"未记录的限制"升格为"已知设计"——违背 §10.4 记账平衡承诺 |
| C | 整个 registry 在其不变量上重建为 `(epoch 计数器 AtomU64 per user)` + name 走事件载荷传递 | 触碰契约（CreateRoom/JoinRoom 派发需 name 的来源），走 §5.6，改动大 |

**倾向**：A——纯 core 内实现、零契约变更；`epochs` 表即使不删也只是 8B/用户，真正释放的是 name 字符串。

## 验收标准

- 核心测试：模拟"用户断线超时→彻底离线"后，验证 names 表条目被淘汰而 epochs 表保留；同用户再次连接 epoch 继续 +1（不回退）
- 回归测试：构造 ISSUE-0009 场景（旧僵尸连接 epoch=1 + 用户被淘汰后重连），断言僵尸仍被拒——证明单调 epoch 不变量在删除后仍成立
- `cargo test --workspace` 全绿；clippy/check-deps 通过；若改契约走 §5.6

## 关联

- ISSUE-0009（已解决）：本 issue 的删除逻辑不能破坏其 epoch 校验语义，是本方案 A 引入"永不删除 epochs 表"的直接原因
- §4.9-3（会话纪元三不变量）：第 1 条"register 每次 +1 并替换"隐含递增假设；本 issue 是它记忆留存部分的资源管理补充
- 竞品对照：gooophira 的 User/状态由 GC 管理与 CPU 资源调优，无此长驻表概念；r0semi 手工管理需显式平衡

## 修复记录（2026-08）

- **方案 A 落地**：`SessionRegistry` 由单表 `inner: Mutex<HashMap<i32,(u64,String)>>` 拆分为 `epochs`（永不删除，8B/用户，单调不变量）与 `names`（可淘汰，昵称字符串）两张 `Mutex<HashMap>`。
- **`register`（user_id, name）**：epoch 写 `epochs`（+1 单调保留），name 写 `names`（覆盖注入）——两者独立锁，无嵌套死锁风险。
- **`name_of`**：改读 `names` 表，返回 `None` 表示已被淘汰；`impl` 侧 `unwrap_or_default()` 兜底——但需要昵称的 `CreateRoom`/`JoinRoom` 只发生在在线会话（此时 name 已由 authenticate 注入），语义不变。
- **`current_epoch`/`is_current`**：改读 `epochs` 表（永不删），ISSUE-0009 的校验读入口不受淘汰影响。
- **新增 `evict_name(user_id)`**：只删 `names`，不动 `epochs`——在 `LifecycleTask::handle` 的 `DangleExpired` 分支（用户彻底离线、重连窗口到期）后调用，释放昵称字符串驻留。
- **回归测试**（`phira_core::tests::lifecycle`）：`evict_name_keeps_monotonic_epoch`——验证昵称淘汰后 epoch 保留、重连 +1 不回退、昵称重注；`register_keeps_name` 既有测试适配双表。
- `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --all -- --check` 全绿。
