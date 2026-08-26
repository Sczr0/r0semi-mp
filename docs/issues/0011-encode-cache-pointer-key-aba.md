# ISSUE-0011：EncodeCache 以裸指针地址为缓存键——ABA 地址复用可致观战者收到陈旧帧

- 状态：**已解决（2026-08）**——方案 A（条目钉住源 Arc）已落地，见文末修复记录
- 发现日期：2026-08
- 发现方式：竞品横评审计中审查热路径编码缓存实现（ADR-0009 / ISSUE-0003 方案 2 的落地代码），对"缓存键 = Arc 指针地址"这一设计的分配器语义推演
- 严重级：**中**（数据正确性缺陷：单次事件触发概率低，但长时运行统计上必然发生；后果是观战者收到上一局/历史批次的陈旧触摸帧——静默、无 panic、极难排查）
- 相关章节：ARCHITECTURE.md §6.5-17（广播编码一次共享）、ADR-0009（Encode-once 热路径）、ISSUE-0003（已解决的"广播编码一次"）

---

## 问题陈述

`SessionSink::deliver` 对热路径事件（Touches/Judges）做"编码一次、多 monitor 共享"，缓存键取的是事件载荷 `Arc` 的**裸地址**：

```rust
// server.rs:672-676
let key = Arc::as_ptr(frames) as usize;
Outbound::Encoded(self.encode_cache.get_or_encode(key, || { ... }))
```

而缓存条目**只持有编码结果** `Arc<Vec<u8>>`，不持有源帧的 `Arc`：

```rust
// server.rs:513-547
pub struct EncodeCache {
    inner: Mutex<std::collections::HashMap<usize, Arc<Vec<u8>>>>,
    capacity: usize,   // 默认 64
}
```

这构成教科书式 **ABA 危害**：

1. 事件 E1 的 `Arc<Vec<TouchFrame>>` 完成投递后引用计数归零 → 堆内存释放；
2. 缓存条目 `(E1_addr, bytes_1)` **仍然存活**（容量 64，仅满则整体清空）；
3. 后续某个新事件 E2 分配帧批次时，分配器把**同一块内存**（同尺寸类，free-list LIFO 下相当常见）分给 E2；
4. E2 投递时 `key == E1_addr` 命中死条目 → **monitor 收到的是 E1 的旧字节**。

## 证据（注释假设 vs 分配器现实）

```rust
// server.rs:540 的注释断言:
inner.clear(); // 满则清（简单淘汰：旧帧指针不复用，留着只会是死条目）

// —— "旧帧指针不复用"这一前提不成立。
// Rust 全局分配器不承诺地址唯一性；同尺寸类的释放块被立即复用是常见行为。
// 触发条件 = "旧 Arc 全部释放" ∧ "新批次落在同一地址" ∧ "旧条目未被 clear 驱逐"
// 三者独立概率均不高，但服务器以小时计连续运行 + 触摸批次高频分配，统计上必然命中。
```

关键不对称性加剧隐蔽性：`OnJoinRoom`/领域事件的正常投递不受影响（不走缓存），只有**多观战者场景的热路径**受影响——而观战者恰是最难报告"画面偶尔回放几秒前动作"的用户群。

## 影响评估

- **正确性**：观战者收到与当前对局无关的历史触摸帧（表现为观战画面瞬间"回放"/"瞬移"），且无任何错误信号；
- **可达性**：需要 ≥1 个观战者在线（live 才转发）+ 地址碰撞；频率低但随运行时长线性累积必然发生；
- **排查成本**：无日志、无 panic、现象与网络抖动相似——按"出现后定位难度"评级应为中高。

## 候选解决方案

| 方案 | 描述 | 代价 |
|---|---|---|
| **A（推荐）** | 条目同时克隆一份**源帧 Arc**：`struct Entry { _pin: Arc<Vec<TouchFrame>>, bytes: Arc<Vec<u8>> }`——条目存活期间源地址被强引用钉住，不可能被复用 | 改动 ~10 行；代价是缓存最多钉住 64 个历史帧批次（有界、量级 MB 以下，与现有 §10.4 记账精神一致） |
| B | 键升级为 `(address, generation)`：全局 AtomicU64 代际计数包装进帧载荷 | 需改 `RoomEvent::RelayTouches` 载荷结构或引入包装类型——触碰契约，走 §5.6 |
| C | 放弃跨事件缓存，改为"单次广播轮内显式传参去重"（cache 作为 deliver 参数而非全局态） | 语义最干净（encode-once 本来就只需要轮内去重），但改动调用路径较大 |

**倾向**：A——最小改动、不动契约、不变量简单（"键所指内存被钉住"可直接写进注释与测试）。

## 验收标准

- 单元测试：构造两批不同内容的帧，模拟"第一批 drop 后第二批分配"场景（可通过先 drop 再分配相同尺寸 Vec 引导分配器复用），断言第二次 `get_or_encode` 不返回第一批的编码字节
- 现有热路径/slow_consumer/memory_guard 测试全绿；`cargo test --workspace` 全绿
- 注释更新：删除"旧帧指针不复用"的错误论断，替换为新不变量的准确表述

## 关联

- ADR-0009 / ISSUE-0003：本组件的来源设计（其"编码一次"目标不受影响，仅键策略有误）
- §10.4 内存守卫：方案 A 新增的有界驻留需与 in-flight 记账口径核对（钉住的批次是否计入？建议不计入——它们不可达回收路径，属常驻小内存）
- 竞品对照：gooophira 同为"编码一次"，但其帧生命周期由 GC 管理，天然无此问题——这是手写缓存在无 GC 语言中的典型陷阱

## 修复记录（2026-08）

- **方案 A 落地**：`EncodeCache` 条目由 `Arc<Vec<u8>>` 升级为 `EncodeEntry { _pin, bytes }`——`_pin` 是源 `Arc` 的克隆（`Box<dyn Any + Send + Sync>` 擦除类型，统一覆盖 `Touches` 的 `Arc<Vec<TouchFrame>>` 与 `Judges` 的 `Arc<Vec<JudgeEvent>>`）。条目存活期间源地址被强引用钉住，分配器不可能复用 → 杜绝 ABA。
- **`get_or_encode` 签名**：`(key, pin, encode)` 三参——`pin`（源 Arc 克隆）在 miss 时存入 `_pin`，hit 时直接丢弃（避开热路径重复 clone 的浪费，仅 miss 路径产生一次原子计数的副作用）。
- **清理错误断言**：删除注释「旧帧指针不复用，留着只会是死条目」——该前提不成立（Rust 分配器不承诺地址唯一性），替换为新不变量「键所指内存被 `_pin` 钉住」的准确表述。
- **内存口径**：`_pin` 钉住的帧批次**不计入** in-flight 记账（不可达回收路径，属常驻小内存；容量 64 有界、量级 MB 以下，与 §10.4 记账精神一致）。
- **回归测试**（`phira_server::server::tests` 4 个）：`encode_cache_pins_source_arc`（断言源 `Arc` 强引用 = 2，证明被钉住）、`encode_cache_hit_returns_cached_bytes`（同 key 命中共享，pin 丢弃）、`encode_cache_distinct_keys_isolate`（不同 key 互不污染）、`encode_cache_same_addr_reuses_pinned_entry`（同地址复用人工复现，命中旧条目）；slow_consumer 既有测试适配三参签名。
- `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --all -- --check` 全绿。
