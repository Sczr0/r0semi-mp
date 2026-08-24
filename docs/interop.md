# 互通测试（§9 双实现互连互通）

**目标**：用原版 `phira-mp-client` 库（真 Phira 客户端集成的是同一份逻辑：编码、心跳、状态机、消息队列）连本服务器，验证**协议双向 + 服务器行为**完整兼容——比自造帧的 e2e 更接近真客户端。

## 工程位置

```
C:/git/r0semi-mp-interop/
├── Cargo.toml   # 依赖 phira-mp-client（原版客户端库）+ phira-mp-common
└── src/main.rs  # 双用户全流程：鉴权→建房→加入→广播→聊天→选图→开局→结算→心跳
```

## 一键运行

```bash
bash tools/interop.sh
```

（等价手动：`python /tmp/mock_api.py` → `R0SEMI_MP_API_BASE=http://127.0.0.1:19000 ./target/debug/r0semi-mp-server` → `cd C:/git/r0semi-mp-interop && cargo run`）

## 流程与结果（2026-08 全通）

| 步骤 | 断言 | 结果 |
|---|---|---|
| user1 鉴权（回源 mock /me） | `me() == Some` | OK |
| user1 建房 | `room_state == SelectChart(None)` | OK |
| user2 鉴权 + 加入 | `room_id == interop1` | OK |
| user1 收 JoinRoom 广播 | messages 含 `JoinRoom{user:2}` | OK |
| 聊天双向 | user2 收 `Chat{user:1}` | OK |
| user1 选图（回源 /chart） | — | OK |
| RequestStart → WaitForReady | — | OK |
| 全员 Ready → StartPlaying | **双端**收到 `StartPlaying` | OK |
| user2 Played（回源 /record 校验） | — | OK |
| 心跳 | `ping()` 延迟 | OK |

## 过程中发现并修复

- mock API 缺 `/record/{id}` 分支 → Played 回源校验（规则 10）解析 `{}` 失败 → 补 `/record/1` 返回合法 Record
- 客户端库 `blocking_*` 查询 API（tokio RwLock `blocking_read`）不能在 runtime 线程调用 → 互通客户端经 `spawn_blocking` 桥接（与真客户端集成方式一致）
- Windows 下服务器监听 `[::]`（IPv6 双栈），客户端连 `[::1]`（IPv4 `127.0.0.1` 被拒）

## 与 Oracle 的关系

- **Oracle**（`r0semi-mp-oracle`）：纯编解码字节级对照（64 用例）
- **互通**（本工程）：完整服务器行为 + 原版客户端逻辑——两层合起来 = 协议与行为双重兼容证明

## 注记

- 真 Phira 游戏 App 联调（最后一步）：需要 Phira 客户端 + 服务器公网可达 + 真账号 token；本互通测试是它的本地预演（同协议、同客户端逻辑，差异仅在 App UI 与网络路径）
