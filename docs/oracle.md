# Oracle 对照（§9 测试策略）

**目标**：以原版 phira-mp 的编码为"标准答案"（Oracle），验证本实现的协议编解码**逐字节一致**——协议逆向的最终正确性保险。

## 工程位置

独立工程（不在 workspace，避免依赖方向检查干扰）：

```
C:/git/r0semi-mp-oracle/
├── Cargo.toml   # 依赖 phira-mp-common（原版）+ phira-api（本实现）
└── src/main.rs  # 对比矩阵：64 个用例
```

## 运行

```bash
cd C:/git/r0semi-mp-oracle && cargo run --release
```

## 结果（2026-08 首次全量）

```
===== Oracle 结果 =====
一致: 64  差异: 0  解码失败: 0
PASS: 与原版逐字节一致
```

| 类型 | 用例数 | 覆盖 |
|---|---|---|
| ClientCommand | 16 | 全变体（Unit / 带字段 / Arc<Vec> 载荷） |
| Message | 16 | 全变体 |
| ServerCommand | 20+ | 全变体 + SResult Ok/Err 两态 |
| RoomState | 4 | SelectChart(Some/None) / WaitingForReady / Playing |
| UserInfo / ClientRoomState / JoinRoomResponse | 3 | 含嵌套（RoomId、Option、HashMap、Vec<UserInfo>） |

## 双向验证

每个用例做两件事：

1. **encode 对比**：同一种子数据，原版 `encode_packet` 与本实现 `encode_packet` 逐字节相等
2. **decode roundtrip**：原版字节 → 本实现 `decode_packet` 必须成功，且重新 encode 仍等于原版字节（"我们的解码器能读原版的字节"）

## 结论

本实现手写的 BinaryData impl（tag=变体索引、ULEB128 长度、Varchar/RoomId/CompactPos、String/Vec/HashMap/Option/Result/Arc 编码）与原版 proc-macro 生成的编码**完全一致**——协议逆向无"自以为懂了"的偏差。

## 注记

1. **HashMap 多元素顺序**：无序容器的迭代顺序由 hash 种子决定（两边实现不同 → 字节不同），**语义上无顺序概念**（客户端 decode 回 HashMap 不受影响）。Oracle 用单元素种子验证编码格式，多元素顺序不计入字节对比。
2. **契约演进**：本实现相对原版的协议演进（如阶段 2 的 `CreateRoom/JoinRoom` 携带昵称——注意那是 `RoomCommand`（core 层）不是 `ClientCommand`（协议层）；协议层 `ClientCommand` 与原版完全一致）不影响编码字节。
3. **CI 集成**：Oracle 依赖本地原版源码（`C:/git/phira-mp` path 依赖），无法进 GitHub Actions；定位为**本地工具**，协议层改动后手动重跑。
