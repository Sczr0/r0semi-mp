# ISSUE-0008：默认配置下服务器启动即退出——`Server::run` 的 select 短路（已修复）

- 状态：**已解决（2026-08）**——修复 + 回归测试，见文末
- 发现日期：2026-08
- 发现方式：flooder 压测工具本地验证时——连接服务器全部超时，排查发现进程启动即退出
- 严重级：**高**（上线即崩：默认配置 `http_port=None` 时服务器无法运行）
- 相关章节：ARCHITECTURE.md §4.5（组合根）、§11（优雅停机）

---

## 问题陈述

`Server::run` 用 `tokio::select!` 同时等待 `http_accept_loop` / `shutdown` / `accept_loop`——**select 任一分支完成即整个 run 返回**。`http_port` 未配置（默认）时 `http_accept_loop(None)` **立即返回** → select 短路 → `run()` 返回 Ok → main 返回 → **进程退出**。

**默认配置（无 http_port）下服务器启动后立刻退出，端口根本没有服务**。

## 证据

```rust
// 修复前 server.rs run()
tokio::select! {
    () = http_accept_loop(http_listener, Arc::clone(&ctx)) => {}  // None → 立即返回 → select 短路！
    () = shutdown => { ... }
    () = accept_loop(listener, Arc::clone(&ctx)) => {}
}
```

实测：`r0semi-mp-server` 启动打印 "listening" 后进程消失；`netstat` 无监听端口；连接超时。

**为什么测试没抓到**：e2e/frames 测试直接调 `handle_connection`（不经 `Server::run` 的 select）——**`Server::run` 本身无集成测试**（测试覆盖盲区）。

## 附带发现（同一排查中）

**Windows IPv6 双栈问题**：默认监听 `Ipv6Addr::UNSPECIFIED`（`[::]`）在 Windows 是 **V6ONLY**（只收 IPv6）——IPv4 玩家（游戏客户端主流）连不上。config.rs 注释写"默认 0.0.0.0"但代码是 `[::]`（**注释-代码不一致**）。修复：默认改 `0.0.0.0:12346`（IPv4；双栈需 socket2 `V6ONLY=false`，v1 用 IPv4 足够）。

## 影响评估

- **上线即崩**：默认配置（README 指引的 12346 端口、无 http_port）无法运行——所有默认部署直接失败
- 配置了 `http_port` 的部署不受影响（http_accept_loop 有监听器会持续运行）——**但这是碰巧，不是设计保证**
- IPv6 问题影响 Windows 部署的 IPv4 玩家连通性

## 修复

- **`Server::run`**：accept 循环放 `tokio::spawn` 后台任务，**shutdown 是唯一退出路径**；停机后 abort accept（停止新连接）
- **默认监听**：`Ipv4Addr::UNSPECIFIED`（0.0.0.0）+ config.rs 注释对齐
- **回归测试**：`tests/server_run.rs`——`timeout` 包 `run()` 断言持续运行（修复前立即返回）+ 连接探测验证 accept 活着

## 验收标准（已满足）

- `cargo test --workspace` 全绿（177，含 server_run 回归）
- 实测：`r0semi-mp-server` 默认配置启动后 `netstat` 显示 `0.0.0.0:12346` LISTENING，IPv4 连接成功
- flooder 压测实测：random 705Mbps / proto 1813 连接 / reconnect 2237 连接，**0 panic**（服务器在真实接受并防御）

## 关联

- 本 issue 由 flooder 工具开发时的本地验证发现——**独立压测工具的价值：暴露测试盲区**
- 与 §4.5（组合根）/§11（优雅停机）相关：run 的生命周期是组合根的运行契约
