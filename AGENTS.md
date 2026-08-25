# AGENTS.md —— r0semi-mp 项目纪律（给 AI Agent / 协作者）

> 本文件是**工作纪律摘要**，不是架构文档。架构细节见 `docs/ARCHITECTURE.md`（1106 行，权威规范）。
> 目标：让"每次会话都是新协作者"的 AI 在 5 分钟内掌握：什么能信、什么不能信、怎么改不出错。

## 项目一句话

Phira 联机房间服务器 `phira-mp`（Rust）的重写：**内存最小（目标 RSS 7-15MB）+ 子系统可整体替换（契约分层）+ 协议完全兼容**。Rust workspace，5 个 crate，165 测试全绿。

## 文档可信度分级（两轮检查结论，2026-08）

| 文档 | 可信度 | 说明 |
|---|---|---|
| `docs/ARCHITECTURE.md` 的**契约层**（§4.4 类型、§6.6 转换表、依赖方向） | ✅ 与代码一致 | 骨架是诚实的 |
| `docs/ARCHITECTURE.md` 的**性能/防护承诺**（§6.5-17 编码一次、§10.4 丢旧保新、§4.9-2 每连接限速） | ✅ **已兑现（2026-08）** | 编码一次共享（ADR-0009/EncodeCache）、丢新不阻塞+kicker（ISSUE-0004）、每连接限速（ADR-0008/CommandLimiter）均已落地 |
| `docs/ARCHITECTURE.md` 的**选型描述**（§4.8-3 peek） | ✅ 已同步 | §4.8-3/§11.1 已改写为"独立端口 http_port + /rooms + /healthz"（ISSUE-0005 已解决） |
| `docs/adr/` | ✅ **已有 0001-0009** | 含新增 ADR-0007（重放）/0008（限速）/0009（编码一次）（ISSUE-0002 已解决） |
| 代码注释 | ✅ 可信 | 大量注释自带章节引用，且记录真实决策（如 peek 放弃原因） |

**总纪律：以代码为准。发现"文档说了、代码没有"→ 记入 `docs/issues/`（现有 0001-0007，0001-0006 已解决、0007 待解决，格式见下）。**

## 修改纪律（改代码前必读）

1. **依赖方向（物理强制）**：`phira-api`（契约，零 tokio）← `phira-core`（柜台）← `impl-*`（只认识 api，**连 core 都不许认识**）← `phira-server`（组合根，唯一认识所有人）。**新增 crate 必须同步更新 `tools/check-deps.py` 的 ALLOW**，否则 CI 红
2. **契约变更走 §5.6**：改 `phira-api`（RoomCommand/RoomEvent/trait）→ 枚举加变体必须 `#[non_exhaustive]` + 契约测试补用例；破坏性变更走 ADR + api 主版本（`cargo-semver-checks` CI 盯）
3. **新增系统命令要碰 4 处**（例：`RoomCommand::Foo`）：
   - `phira-api/src/rooms.rs`：枚举变体（带 `missing_docs` 文档！该 crate `missing_docs=deny`）
   - `phira-core/src/bus.rs`：**3 个 match 各加一行**——`queue_policy`（丢/等/拒）、`command_name`（Metrics 键）、`command_needs_response`（有无回话）
   - `impl-rooms-v1/src/lib.rs`：handle 分支
   - `phira-contract/src/rooms.rs`：契约测试用例（**改完必须全绿**）
4. **时间/连接事实必须命令化**（§4.6）：impl 内**禁止**开后台任务/定时器/线程。断线、超时、重连全是命令（`Tick`/`UserDisconnected`/`UserDangleExpired`），由 core 生命周期任务单一生产者派发
5. **lint 红线**：全 workspace `forbid(unsafe_code)`；`phira-api` 额外 `missing_docs=deny`；`phira-core` 禁 `unwrap/expect`（柜台不 panic）；clippy `pedantic` 全量 + `-D warnings`
6. **测试**：`cargo test --workspace` 必须全绿；契约测试是"任何 impl 必须通过"的安全网；**模糊测试**（`tests/fuzz.rs` 解码器 + `tests/fuzz_frames.rs` 真实 TCP 垃圾流）保证解码器吃任意字节不 panic；**压力测试**（`tests/pressure.rs`，`#[ignore]` 手动跑：`-- --ignored`——本地回环实测 ~1.5-2.3Gbps 灌流 0 panic 0 内存膨胀）；Oracle 字节级对照在独立工程 `C:/git/r0semi-mp-oracle`（不在 workspace）
7. **错误走 Err 不走 panic**：业务拒绝用 `RoomError::Business`（客户端可见），内部故障用 `Internal`（通用文案 + 日志）。错误率只统计 `Internal`
8. **安全锁（§10.4/§11，ADR-0010）**：全局在途字节 64MiB + 每连接 send 队列 8MiB（超限踢）+ 已鉴权连接上限 1000——**改投递/写路径时必须保持记账平衡**（投递 charge ↔ 写任务 fetch_sub ↔ Drop guard 兜底），否则内存守卫失真；上限为常量可参数化

## 陷阱清单（历史记录：0001-0006 已全部解决并回写文档——现在可以信文档）

> 下表为历史陷阱，各条均已解决：修复细节见 `docs/issues/` 各文末修复记录（ADR-0007/0008/0009）。
> **当前唯一未决**：ISSUE-0007（game_time 钩子——断线恢复缺"玩家进度"维度，移植形态见该 issue）。

| # | 陷阱 | 现状 |
|---|---|---|
| 1 | "表 miss 挂起重放"（§4.9-3 幽灵座位） | ✅ **已修复（2026-08）**——`lookup_room_with_replay`（3×20ms 重放）；ADR-0007（ISSUE-0001 已解决） |
| 2 | ADR 文件 | ✅ **已修复（2026-08）**——0001-0009 已落 `docs/adr/`（含 0007 重放/0008 限速/0009 编码一次）+ `tools/check-adr.py` CI 第 3b 闸门（编号连续）（ISSUE-0002 已解决） |
| 3 | "一次编码 Bytes 共享"（§6.5-17） | ✅ **已修复（2026-08）**——`EncodeCache` 热路径编码一次（帧 Arc 指针缓存）+ `Outbound::Encoded` 直写；ADR-0009（ISSUE-0003 已解决） |
| 4 | "丢旧保新、绝不阻塞房间"（§10.4） | ✅ **已修复（2026-08）**——`try_send` 丢新不阻塞 + `Backpressure` 积压标记 + kicker 5s 踢乌龟；阈值未进 config；"丢旧"仍为文档表述（实际丢新）（ISSUE-0004 已解决） |
| 5 | peek 嗅探（§4.8-3/§11.1 方案 B） | ✅ **已修复（2026-08）**——文档 3 处同步（peek 已放弃）；`http_port` 提供 `/rooms` + `/healthz`（ISSUE-0005 已解决） |
| 6 | 每连接限速（§4.9-2/§4.9-9） | ✅ **已修复（2026-08）**——`CommandLimiter` 只限"贵"命令（CreateRoom 1/s，JoinRoom/SelectChart/Played 5/s），超限回 `TooManyRequests`；ADR-0008（ISSUE-0006 已解决） |

## 常用命令

```bash
cargo build --workspace            # 编译
cargo test --workspace             # 全量测试（含契约测试）
cargo clippy --workspace --all-targets -- -D warnings   # lint（CI 第 2 闸门）
python3 tools/check-deps.py        # 依赖方向（CI 第 3 闸门；Windows 本地需 python）
cargo fmt --all -- --check         # 格式
```

CI 六道闸门：fmt → clippy → check-deps → test → cargo-deny 许可 → cargo-semver-checks（盯 phira-api）。

## 新增 issue 的格式（docs/issues/）

```
# ISSUE-00XX：<标题>
- 状态：待解决
- 发现日期：YYYY-MM
- 发现方式：<如何发现>
- 严重级：低/中/高
- 相关章节：ARCHITECTURE.md §X
## 问题陈述 / ## 证据（文档承诺 vs 代码实际）/ ## 影响评估
## 候选解决方案 / ## 验收标准 / ## 关联
```

## 架构速记（商店比喻）

- **phira-api** = 货架规格（契约，谁都不认识）
- **phira-core** = 柜台（会话/总线/路由表/生命周期，只认识 api）
- **impl-rooms-v1** = 第一个货物（房间实现，只认识 api）
- **phira-server/main.rs** = 老板（组合根，唯一接线处）
- **换实现 = 组合根换工厂 + 契约测试全绿**（不是改核心）
- **并发模型**：每房间一个 actor + mpsc 串行 + 生命周期单一生产者；`&mut self` 无锁
