# ISSUE-0003：§6.5-17 方案 A 未兑现——"core 编码一次 Bytes 共享"实际是"每接收者各自转换+编码"

- 状态：**已解决（2026-08）**——方案 2（热路径编码一次共享）已实现（ADR-0009），见文末修复记录
- 发现日期：2026-08
- 发现方式：架构评审（对比 ARCHITECTURE.md §6.5-17 方案 A 与 phira-server 实际代码）
- 严重级：中（性能承诺未兑现 + 文档失真；`Arc<Vec<TouchFrame>>` 帧浅共享已兑现）
- 相关章节：ARCHITECTURE.md §6.5-17（观战转播·方案 A）、§4.8-2（Bytes 选型）、§4.4（RelayTouches/RelayJudges）、§4.3-1（api 零 Bytes 红线）

---

## 问题陈述

ARCHITECTURE.md §6.5-17 承诺观战转播（性能热点）的编码成本：

> **热路径机制（方案 A：结构化转发，编解码归 core）**：core 解码一次（校验）→ 命令侧 `Touches{frames}`/`Judges{judges}` → actor 查 live、计算 `targets = Specific(monitor_ids)` → 返回结构化事件 `RelayTouches`/`RelayJudges` → **core 用它的编码器把 ServerCommand 编码一次**为 `Bytes` → 共享给所有 monitor。总编解码：**每命令 1 解 + 1 编，每接收者 0 次**；impl 永不碰协议编码

**实际代码未实现"编码一次共享"**——事件→ServerCommand 转换和协议编码都在**每接收者**路径上重复执行。

## 证据

### 文档承诺 vs 代码实际

| | 文档承诺（§6.5-17 方案 A） | 代码实际 |
|---|---|---|
| 编码次数 | 每命令 1 解 + 1 编，**每接收者 0 次** | **每接收者 1 次 `event_to_server` + 1 次 `encode_packet`** |
| 共享单位 | core 编码一次为 `Bytes` 共享给所有 monitor | mpsc 传**未编码的 `ServerCommand` 结构体** |
| 写路径 | Bytes 直接投递 | 每连接 `Vec<u8>` buffer + 两次 `write_all` |

### 实际数据流（读码确认）

```
actor → Vec<RoomEvent>（RelayTouches { frames: Arc<Vec<TouchFrame>> }）
→ bus.process_events → sink.deliver(user_id, &event)          【每接收者一次】
→ SessionSink::deliver → event_to_server(event.clone())       【每接收者转换一次】
→ mpsc 传 ServerCommand 结构体（未编码）
→ 各 session 写任务 → encode_packet(&payload, &mut buffer)    【每接收者编码一次】
→ write_all(len_buf) + write_all(buffer)                      【两次写 syscall】
```

关键代码：

```rust
// crates/phira-server/src/server.rs —— SessionSink::deliver（每接收者全量转换）
async fn deliver(&self, user_id: i32, event: &RoomEvent) {
    let commands = phira_core::convert::event_to_server(event.clone());  // 每接收者转换
    ...
    let _ = tx.send(cmd).await;   // mpsc 传结构体，非 Bytes
}

// crates/phira-server/src/stream.rs —— 写任务（每连接各自编码）
while let Some(payload) = send_rx.recv().await {
    buffer.clear();
    encode_packet(&payload, &mut buffer);          // 每接收者编码
    write.write_all(&len_buf[..n]).await?;         // ① 长度前缀
    write.write_all(&buffer).await?;               // ② 载荷
}
```

### 已兑现的部分（不要否定优化成果）

- `RelayTouches`/`RelayJudges` 的 `frames: Arc<Vec<TouchFrame>>` / `judges: Arc<Vec<JudgeEvent>>`——**帧数据浅共享**（clone = 原子引用计数 +1，不复制帧内容）✅
- 10 个 monitor 看 60Hz 触摸流：省的是 9/10 的帧数据深拷贝

**未兑现的部分**：`event_to_server`（转换）+ `encode_packet`（编码）仍每接收者各做一遍——编码层面等价于原版的"每接收者克隆整包"（只是把深拷贝换成了重复编码）。

## 影响评估

- **不是正确性问题**：每接收者独立编码结果字节一致（同一 payload → 同一字节流），只是 CPU 浪费
- **CPU 浪费量**：live 房间 monitor 数 × 60Hz ×（1 次转换 + 1 次编码）的重复。单 monitor 编码 ~微秒级，8 玩家 × 5 monitor 的 live 房间峰值 ~300 次/s 重复编码——个位数百分比 CPU，**文档 P1"CPU 合理即可"下可容忍，但违背了文档自己的性能承诺**
- **文档失真**：§6.5-17 方案 A 被当作已定案设计（评审 §8 一-1），实际未落地——与 ISSUE-0001/0002 同类"文档说了、代码没有"
- **TCP 广播硬成本（谁都无法绕过）**：内核不知道"N 个 socket 共享同一份数据"，每个 socket 发送都要用户态→内核 socket buffer 拷贝一次。优化上限 = "1 次编码 + N 次拷贝"，消灭不了 N 次拷贝

## 候选解决方案

| 方案 | 描述 | 代价 |
|---|---|---|
| A. 兑现方案 A（契约变更） | EventSink 形状改为可传共享编码结果（`Bytes` 或"编码一次 + Arc 共享"）；bus 层编码一次、mpsc 传共享缓冲 | 动契约（§5.6 + ADR + api 主版本）；SessionSink/bus 重构；需处理"不同接收者 targets 不同"的编码缓存粒度 |
| B. writev 合并写 | `write_vectored(&[len_buf, buffer])` 两次 syscall → 一次 | ~10 行，零契约变更，立竿见影（独立于 A，可先做） |
| C. io_uring | 每连接提交队列批量写 | 复杂度高；收益在 syscall 次数；current_thread 运行时下收益有限 |
| D. msg_zerocopy | `sendmsg(MSG_ZEROCOPY)` 内核引用用户页 | pin + 异步错误通知；小包场景收益存疑；**不推荐 v1** |
| E. 保持现状 + 修文档 | 承认"每接收者编码"为现状，§6.5-17 方案 A 降级为未来优化项 | 零代码；但性能承诺失真仍在 |

**倾向**：**先 B（writev，便宜且独立）→ 再评估 A（兑现方案 A 需要契约变更，走 ADR；收益 = 省 N-1 次转换+编码）**。E 是兜底选项（若评估后认为 CPU 收益不划算，明确降级并修文档，与 ISSUE-0001 方案 C 同类）。

## 验收标准（已满足核心；writev 另项）

- **核心（方案 2）**：热路径同一帧 N 个 monitor 只 1 次 `encode_packet`（EncodeCache 按帧 Arc 指针缓存）；写任务对 Encoded 直写共享载荷；Oracle 字节流不变（编码结果与逐接收者编码字节一致——同一 payload 同一字节流）
- **契约**：phira-api 仅加 `BinaryWriter::write_raw`（编解码工具方法，非协议语义）；EventSink 签名不变、RoomListSink 无感
- `cargo test --workspace` 全绿（165）；check-deps.py 通过
- **未做（另项）**：writev 合并写（ISSUE-0003 候选方案 B，与本次正交）；转换去重（ADR-0009：暂缓）

## 修复记录（2026-08）

- **`Outbound` 消息类型**（stream.rs）：发送通道 = `Command(ServerCommand)` | `Encoded(Arc<Vec<u8>>)`；`Outbound` 实现 `BinaryData`——Encoded 分支经 `write_raw` 直写缓存字节，Stream 通用路径不变（客户端模式 frames.rs 测试无感）
- **`EncodeCache`（独立组件，server.rs）**：热路径按帧 Arc 指针缓存编码载荷（容量 64，满则清）——同一帧多 monitor 命中同一缓存，每命令 1 编、每接收者 0 次（方案 A 核心兑现）
- **改动面**：stream.rs（Outbound）+ server.rs（EncodeCache/SessionSink deliver/各发送方 Outbound 适配）+ binary.rs（write_raw）+ slow_consumer 测试 channel 类型适配
- **测试**：+2（EncodeCache 同 key 一次编码 / 满则清空重编）
- **ADR-0009**：方案 2 决策 + 泛化触发条件（第二个大扇出场景才提升到方案 3，原则 5）

## 关联

- ISSUE-0001（幽灵座位重放）：同属"文档承诺 vs 实现差距"审查发现
- ISSUE-0002（ADR 目录空置）：方案 A 的契约变更需要 ADR 记录，先补 ADR 体系
