# ISSUE-0006：§4.9-2/§4.9-9 "每连接限速"承诺未实现——滥用控制缺"快端"防线

- 状态：**已解决（2026-08）**——每连接限速已实现（ADR-0008），见文末修复记录
- 发现日期：2026-08（第二轮文档-实现一致性检查）
- 发现方式：全库搜索 `ratelimit/rate_limit/per_connection/token_bucket/限速` 零结果，对照 §4.9-2/§4.9-9
- 严重级：中（滥用防护承诺缺失；与 ISSUE-0004 慢消费者形成"快慢双端"缺口）
- 相关章节：ARCHITECTURE.md §4.9-2（队头阻塞缓解）、§4.9-9（队列压力分级·滥用控制）、§11（滥用防护可选观察者）

---

## 问题陈述

ARCHITECTURE.md 两处**承诺 v1 采用"每连接限速"**作为滥用控制手段：

1. §4.9-2："缓解（**v1 采用**）：(a) 热路径可丢（规则 9）；(b) **每连接限速**；(c) 结算突发可预期"
2. §4.9-9："**滥用控制优先用每连接限速**（热路径 ~60-70Hz 上限），不让队列压力触发断连"

**实际实现：每连接限速完全缺失**——全库无 rate limit / token bucket / 每连接频率限制代码。(a) 热路径 DropIfFull ✅ 已实现，(b) 无，(c) 属设计预期。

## 证据

```bash
$ grep -rniE "ratelimit|rate_limit|per_connection|token_bucket|限速" crates/ --include="*.rs"
# 零结果（唯一"限速"相关是帧大小上限 packet_limit，非频率限制）
```

现状的滥用防护只有：
- **帧大小分级**（§10.4：鉴权前 4KiB / 鉴权后 1MiB）——防"大帧"，不防"高频"
- **队列压力分级**（§4.9-9）：客户端命令满则 `Reject`（断连）——**是惩罚不是限制**
- **连接准入**（§11）：未鉴权上限 + 每 IP 限额——只防未鉴权
- 鉴权后、连接内的**命令频率**无任何限制

## 影响评估

- **"快端"滥用路径开放**：鉴权后玩家可高频发非热路径命令（`Chat`/`CreateRoom`/`JoinRoom`/`SelectChart` 等，`QueuePolicy::Reject` 类别）刷房间队列——满则触发断连（玩家体验）或反复建房（资源消耗）；`CreateRoom` 每次 spawn actor + channel（§4.9-9）
- **与 ISSUE-0004 形成"快慢双端"缺口**：0004 = 慢消费者（乌龟 monitor 不收包）阻塞房间；0006 = 快消费者（高频命令）刷爆队列——两端都无频率防线
- **文档失信**："v1 采用""优先用"是明确承诺，非"可加"表述（对比 §10.2 memory-check 的"CI 可加"是可选）
- **严重级评估为中**：非阻塞性（Reject 断连兜底了最坏情况），但背离文档承诺 + 防滥用能力弱于设计

## 候选解决方案

| 方案 | 描述 | 代价 |
|---|---|---|
| A. 兑现每连接限速 | session 层令牌桶：每连接 N 命令/秒（文档建议热路径 ~60-70Hz 上限）；超限可降级处理（丢包/延迟/断连分级） | ~50-100 行 + 测试；参数进 config |
| B. 只限"贵"命令（轻量） | 仅对高成本命令限速：`CreateRoom`/`JoinRoom`（spawn actor）/回源类（`Played`/`SelectChart`）；热路径 Touches 靠 DropIfFull 已够 | ~30 行 + 测试；符合"成本计量"思路（此前 RU 讨论的轻量版） |
| C. 修文档（降级） | 承认每连接限速未实现，§4.9-2/§4.9-9 改为"依赖队列压力分级 + 断连惩罚" | 零代码；但滥用防线弱于设计意图 |

**倾向**：**B 优先**（限"贵"命令最贴合风险——高频 CreateRoom/回源才是资源威胁；高频 Chat 之类成本低、Reject 断连可接受）→ 完整 A 视需要。与 ISSUE-0004 的修复（发送积压踢除）正交，可同轮做。

## 验收标准（已全部满足）

- **B**：`CreateRoom`(1/s)/`JoinRoom`/`Played`/`SelectChart`(5/s) 每连接频率上限生效（集成测试：连发 CreateRoom → 第二个收 `too many requests`，窗口恢复后可建房）；热路径 Touches 不受影响（专项测试：连发不报错）
- 新错误码 `TooManyRequests` 走 §5.6 + ADR-0008（phira-api 契约追加 + 穷举测试补断言）
- `cargo test --workspace` 全绿（159）；check-deps.py 通过

## 修复记录（2026-08）

- **实现**：`server.rs` 新增 `CommandLimiter`（令牌桶简化版：每命令"上次允许时刻"）+ `rate_limit` 白名单（只限"贵"命令：CreateRoom 1/s、JoinRoom/SelectChart/Played 5/s）；`handle_frame` 已鉴权分支派发前检查，超限回 `TooManyRequests` Business 错误（不触发队列 Reject 断连，兑现 §4.9-9"优先限速"）
- **契约**：phira-api `RoomErrorCode::TooManyRequests`（§5.6 流程：枚举追加 + units 穷举测试补断言）
- **ADR**：ADR-0008（每连接命令限速）——含间隔表、语义叠加说明（同连接 1s 内重复建房先撞限速而非 AlreadyInRoom）
- **测试**：+4（limiter 间隔语义 / 命令独立 / 集成 CreateRoom 限速 + 窗口恢复 / 热路径不受限）；e2e `duplicate_join_rejected` 加 1.1s 等待（避免被限速拦截，保留 §6.5-27 语义验证）
- **遗留**：间隔常量可参数化进 config；`Authenticate` 限速（登录失败）留 Observer 阶段 4（§7.3：拦截不到鉴权流）

## 关联

- ISSUE-0004（慢消费者阻塞房间）："快慢双端"缺口的另一端；修复正交
- 与"RU（请求单元）按成本限流"讨论的轻量版对应——只对"贵"命令计量，不做全套加权
- ISSUE-0002（ADR）：若限速错误码进契约，需 ADR 记录
