# ISSUE-0005：§4.8-3 peek 嗅探被实现推翻（改独立端口），文档 3 处未更新；§11.1 方案 B `/healthz` 未实现

- 状态：**已解决（2026-08）**——文档同步 + /healthz 实现，见文末修复记录
- 发现日期：2026-08（第二轮文档-实现一致性检查）
- 发现方式：对比 ARCHITECTURE.md §4.8-3/§11.1/§10.4 与 `phira-server/src/server.rs` 实际实现
- 严重级：中（选型定案被实测推翻是合理决策，但文档 3 处失真；`/healthz` 承诺未兑现）
- 相关章节：ARCHITECTURE.md §4.8-3（选型定案三）、§11.1（测活方案 B）、§10.4（半开连接）

---

## 问题陈述

ARCHITECTURE.md §4.8-3 把"HTTP 嗅探用 `socket.peek`"列为**选型定案**（"敲第一行代码前定死"），§11.1 方案 B 与 §10.4 也引用 peek 方案。**实际实现已放弃 peek，改用独立端口**——文档 3 处未同步更新。

同时，§11.1 方案 B 承诺的 `/healthz` JSON 健康检查（uptime/connections/rooms）**未实现**——管理 HTTP 端点只有 `/rooms` 房间列表。

## 证据

### 文档（3 处仍写 peek）

| 位置 | 原文 |
|---|---|
| §4.8-3（487 行） | "3. **HTTP 嗅探用 `socket.peek`**：测活（§11.1 方案 B）在 accept 后 peek 前几字节分流——`0x01` 走 MP 协议，`b"GET "`/`b"HEAD"` 走 HTTP 分支；peek 不消费数据、不污染 MP 状态机" |
| §11.1 方案 B（934 行） | "端口复用：accept 后 `socket.peek(&mut buf).await` 窥探前几个字节（peek 不消费数据）" |
| §10.4（895 行） | "握手超时（**peek** 等首字节 ≤5s）" |

### 代码实际

```rust
// crates/phira-server/src/server.rs:503（注释即决策记录）
// 注：HTTP 管理端点走独立端口（`http_port`），不混入 MP 入口（peek 分流在
// Windows/current_thread 下不稳定，2026-08 实测 5s 延迟 + 后续卡死）
```

- 实际：`http_port: Option<u16>` 独立监听端口，仅提供 `/rooms`（server.rs:129/623/659）
- `socket.peek` 仅存在于握手读首字节的语义描述（stream.rs:41 注释"peek/读首字节"），实际实现是 `read_u8`（消费式）
- **§11.1 方案 B 的 `/healthz`（`{"status":"ok","version":...,"uptime_s":...,"connections":...,"rooms":...}`）未实现**

## 定性分析

- **放弃 peek 是合理技术决策**：文档 §4.8-3 的"实测"精神本身就要求实现验证（Windows/current_thread 下 5s 延迟 + 卡死 → 主动推翻，代码注释记录了原因——符合项目"诚实注记"传统）
- **文档欠账**：§4.8-3 标注"定案"，§11.1 方案 B 描述为"推荐实现"，但两者都未标注"已实测放弃"状态
- **`/healthz` 缺口**：方案 A（鉴权前 Ping→Pong）已实现 ✅（免费测活），方案 B 的深度健康 JSON 未做——如果运维需要（K8s probe / 监控大盘），当前只有 `/rooms` 可用

## 候选解决方案

| 方案 | 描述 | 代价 |
|---|---|---|
| A. 修文档（最小） | §4.8-3/§11.1/§10.4 标注"peek 实测放弃，改独立端口 `http_port`（§运营）"；§11.1 方案 B 标注"未实现，如需可后续按 §运营 HTTP 端点扩展" | 零代码；文档与实现对齐 |
| B. 补 `/healthz`（兑现方案 B 部分承诺） | 在 `http_port` 监听器加 `/healthz` 端点：`{"status":"ok","version","uptime_s","connections","rooms"}`；数据源 = Metrics（§3.2，文档承诺"深度健康信息白送"）+ 连接数/房间数 | ~50-100 行（文档自己的估算）+ 测试 |
| C. A + B | 修文档 + 补 `/healthz` | 组合 |

**倾向**：**A 必须做**（文档失真，与 0001/0002/0003 同类）；**B 按需**（若部署需要健康检查 JSON 就做，否则 A 即可并记 `/healthz` 为未来项）。验收不依赖方案 B 是否实现——文档状态必须真实。

## 验收标准（已全部满足）

- **A**：§4.8-3/§11.1/§10.4 三处文档更新：peek 标注"实测放弃（2026-08，Windows/current_thread 5s 延迟）"，替代方案 = 独立端口 `http_port`（§运营）
- **B（已做）**：`GET /healthz` 返回 `{"status","version","uptime_s","connections","rooms"}`；`/rooms` 回归；测试覆盖
- `cargo test --workspace` 全绿（163）；check-deps.py 通过

## 修复记录（2026-08）

- **文档**：§4.8-3（选型定案三）/§11.1（方案 B）/§10.4（半开连接行）三处同步——peek 标注"2026-08 实测放弃（Windows/current_thread 5s 延迟 + 卡死）"，方案 B 改为"独立端口 `http_port` 实现"（ISSUE-0004 修复后投递不阻塞，进一步佐证放弃 peek 的正确性）
- **实现 /healthz**（方案 B 兑现）：`http_serve` 加 `/healthz` 路由——`status/version/uptime_s/connections/rooms`；数据源 = `SessionSink::conn_count` + 房间列表 + 进程启动 `OnceLock`，**不依赖官方 API**（验收标准成立）；`/` 端点列表补 `/healthz`
- **测试**：+4（healthz JSON 字段 / rooms 回归 / 404 / 根端点列表）
- **遗留**：`/healthz` 无独立限速（公开部署建议反代限速，文档已注明）；方案 C（独立健康端口）与 B 重叠，不另做

## 关联

- ISSUE-0003（广播编码未兑现）：同属"§性能/选型承诺 vs 实现"审查系列
- 与本 issue 同轮的 ISSUE-0006（每连接限速缺失）——两轮检查累计 6 项
