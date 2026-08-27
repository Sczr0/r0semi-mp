# CPU 瓶颈定位（1500 人狂按键场景，§10.1.1 load 29 的本地复现）

> 状态：2026-08 定位完成（静态放大证据链）；优化待拍板（候选见 §4）。
> 复现工具：`crates/phira-server/tests/bench_broadcast.rs`（`#[ignore]` 手动跑）。

## 1. 场景与复现

```bash
# 基准刻度：N 客户端同房，全 16Hz Touches
R0SEMI_BENCH_N=300 R0SEMI_BENCH_SECS=8 cargo test -p phira-server --test bench_broadcast -- --ignored --nocapture
```

实测（本机）：50 客户端 → 691 触摸帧/秒（≈50×16 预期 ✓）；300 客户端全流程通过（22.5s，
其中 ~21s 是串行连接握手——生产每 IP 未鉴权并发上限 5 是 bench 需串行连入的原因，§10.4）。
房内触摸帧/秒 = 输入负载；**CPU 成本 = 每帧 × 房内人数（扇出）**。

## 2. 放大结构（静态证据链，数学即结论）

单条 `Touches` 命令的下游成本（1500 人同房）：

```
1 命令 → 房间 actor → 1 个 RelayTouches 事件（targets=All）
  → bus.process_events：遍历 routes 1500 条过滤 room_id（O(N)）
  → deliveries 列表 push 1500 × ev.clone()
  → 1500 次 sink.deliver(user, ev)：
      ├─ phira_core::convert::event_to_server —— 同一事件对每个接收者重跑一次转换！
      ├─ sessions.read().await（tokio RwLock 读锁 ×1500）
      ├─ EncodeCache.get_or_encode（Arc::as_ptr hash 查/编码；§ADR-0009 已共享编码一次）
      └─ 记账原子 queue_bytes.fetch_add ×2 + try_send（安全锁 A，§10.4）
```

**放大数**：1500 人 × 16Hz = 24k cmd/s 输入 → **36M 下游投递/秒**。
每投递 ≈ 30-80ns（转换 + 读锁 + 原子 + send）→ **1.1-2.9 CPU 秒/秒**。双核
load 29 与这个量级吻合——**扇出放大就是瓶颈本体**，单点优化（编码一次等 ADR-0009）
已被放大率吃光。

## 3. 实测采样（2026-08-28，Windows ETW：samply record + Firefox profiler UI 导出）

**样本 15162，模块归属**（frameTable → nativeSymbol → lib）：

| 模块 | 占比 | 代表函数 | 语义 |
|---|---|---|---|
| ntdll.dll | 70% | `NtWaitForAlertByThreadId` 6690 / `NtRemoveIoCompletionEx` 3122 / `NtRemoveIoCompletion` 531 | **IOCP 完成端口循环 + 等待**—— 每包一次完成唤醒 |
| ntoskrnl.exe | 11% | 内核 | 系统调用底 |
| 用户态算法（未符号化 fun_*） | <5% | 合计 ~700 | 房间 deliver/转换 **不是大头** |
| 用户态可见 | 1.4% | atomic_load / CAS / RtlLookupEntryHashTable | 记账/队列 |

**结论修正**：静态假设（§2）预测用户态放大（event_to_server 重转）为瓶颈——**实测推翻**：
Windows 上 1500 连接双向高频小包，CPU 主体是**每包一次 IO 完成端口唤醒/系统调用**
（`NtRemoveIoCompletionEx` 系列 + 写侧每帧 2 次 `write_all` syscall）。
优化重心应放在**系统调用面**（批处理合并 IO），而非用户态算法去重。

### 3.1 锁竞争矩阵（Linux perf，2026-08）

| 锁 | 帧样本 | 归因 | 处置 |
|---|---|---|---|
| tokio time driver（InnerState） | 18.0M | **bench 客户端 sleep 假象**（1500×16Hz 与服务器同 runtime 混采） | 排除 |
| **Metrics.record**（Mutex<HashMap<&str, CommandStats>>） | 9.5M | 我们的代码：dispatch 每命令一次锁 | ✅ **已无锁化**（下述）：热路径（touches/judges）豁免为单原子计数，慢路径保留 |
| SessionRegistry.epochs | 7.0M | **探针实锤为假**：R0SEMI_EPOCHS_PROBE=1 实测（bench N=100）调用 5705 次、慢锁(>50µs)仅 2 次、总等待 1149µs（0.02% CPU）——7M 帧是 tokio fp 采样在 async 任务边界的**串帧伪影** | 排除（探针保留为诊断工具） |
| mpsc Waitlist semaphore | 1.5M | tokio 内部（1500 写任务等待） | 标准代价 |
| io registration / names | <1M | 连接建立期/低频 | —— |

**Metrics 无锁化**（2026-08 落地）：`bus.rs` Metrics 加 `hot: AtomicU64`——
Touches/Judges 只 `fetch_add(1, Relaxed)`（触摸流无错误语义、f64 moving-avg
无运营价值）；其余命令走原明细（低频无争）。`snapshot` 合成
`touches.judges.hot` 条目（count 保留，明细零）。契约测试不涉及触摸流明细 ✓。
**复采验证（flamegraph workflow 第二轮，2026-08）✅**：
- Metrics 锁帧 0.31% → **0.00%**；CommandStats 泛型帧 0.44% → 0.03%；lock_contended
  总帧 0.81% → 0.50%（-38%）——热路径无锁化生效，与预期一致；
- epochs 锁帧仍稳定 ~0.30%（不随 Metrics 消失而变）+ 探针实锤 0.02% CPU → **串帧
  伪影定性成立**（与 SessionRegistry 无关，tokio 内部栈拼接；0.3% 低价值收笔）；
- 剩余大头不变：sendto 27% / recvfrom 21%（**读侧合读是下一只明确优化对象**——
  带 pending 缓冲的设计已论证可防垃圾流，见 §6 备选）；
- 记账原子 ~4.4%（SeqCst 属安全锁 A 边界，§Relaxed 结论维持：不做）。
**已落地（同轮）**：写侧批处理（`stream.rs` WRITE_BATCH_MAX=64：`recv_many`
攒帧一次 `write_all`，低流量延迟不增）——回归绿（memory_guard 账目平衡/healthz）。
**量化复核路径（已自动化）**：`.github/workflows/flamegraph.yml` 手动触发——GitHub
Actions（Linux）编译 bench + `perf record` 采样 + FlameGraph 生成 SVG/collapsed 栈
artifact。Windows 非管理员 ETW 采样 + samply save-only 无符号（函数名是地址、lib 归属
缺失）；Linux perf 对 debug 构建天然带符号（ELF + debug info，无 PDB 问题）——这是
**同一 benchmark 的权威复核面**。两次 Run 各下载 artifact，collapsed.txt 直接数值
对比（写批处理前后效果在此验证）。

**备选后续**：读侧长度前缀逐字节 `read_u8`（每字节 poll）可改为带用户缓冲的合读
（需 pending 缓冲，防吞后续帧载荷——本轮未动）；`tokio` multi_thread 单 IO driver
为架构级约束（1500 连接高频双向 → driver 单线程），缓解靠减少 IO 事件数。

## 4. 优化候选（按 ROI 排序，均需拍板后实施）

| # | 方案 | 收益机制 | 代价/风险 | 通道 |
|---|---|---|---|---|
| 1 | **转换一次、投递复用**：core 在 process_events 里对每个事件转一次 `ServerCommand` 序列，`deliver` 传已转换结果 | 砍掉 99.93% 的转换重跑（1500× → 1×） | 碰 `EventSink` 契约（§5.6：加 fast-path 或改签名）；API 主版本影响 | 契约变更 |
| 2 | **房间→成员直取**：`Targets::All` 不再遍历全量 routes，维护 per-room 成员集 | O(N)=1500 遍历 → O(1) 直读（每次事件省一趟全表扫描） | SessionRegistry/RoomListSink 已有同类结构可复用；状态一致性要钉契约测试 | 组合根/核心内改 |
| 3 | **记账原子调度**：`SeqCst` → `Relaxed`（账目只需最终一致，无需全序） | 每投递 2-4 次原子从 ~15-20ns → ~5ns | ADR-0010 安全语义要重论证；账目平衡测试已现成 | ADR |
| 4 | **触摸旁路广播**（§4.9-9 已声明热路径可丢）：Touches 不进 actor 状态机，读路径直接 fan-out | 砍掉 actor 串行 + 状态流转（命令数减半级别） | 信令面重大改造（读路径旁路 actor）；丢帧语义要契约化 | ADR + 契约测试 |

**建议**：#1+#2 打包（同属"投递面去重放大"，共享契约测试改动）为一个 ADR；
#3 独立小改；#4 留作触摸流进一步优化的储备（当前放大主要在生产转换+遍历上）。

## 5. 验收刻度

- bench 固定 N=300 基准记录**服务端 CPU 时间**（Windows 实测受限，Linux perf 复核）；
- 优化后同样本秒数下「房内触摸帧/秒」不变（输入守恒）+ 服务端 CPU 占比显著下降；
- 契约测试全绿 + memory_guard 账目平衡（#3 的重点回归面）。