# ISSUE-0014：conformance 测试 worker_threads=2 下随机整体僵死（tokio 唤醒丢失）——备查

- 状态：**待解决（低危，备查——已绕过未深挖）**
- 发现日期：2026-08
- 发现方式：conformance.rs（真 SDK 崩溃猎手）调试期实测——`worker_threads = 2` 下 a1_a5/a6 必挂/随机挂 60s+；根因未定位，仅以 `worker_threads = 4` 绕过
- 严重级：低（仅测试 harness 运行时参数；生产服务器不受影响）
- 相关章节：client-conformance.md §已落地；crates/phira-server/tests/conformance.rs 头部坑位注记

## 问题陈述

conformance 测试在 `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` 下：
- a1_a5/a6（双 SDK 客户端 + 完整服务器组合）随机**整体僵死**：guest 的 JoinRoom 帧永远出不了 SDK 发送队列，
  且 SDK 自家 7s 超时 / 测试外圈 10s 超时全部失效；
- 挂起进程实测 CPU 总量 ~0.016s、8 线程全 park——**无代码在跑、无死循环**，是调度层"任务全睡没人叫"。

## 证据（对照实验，2026-08 实测）

| 组合 | worker_threads | 结果 |
|---|---|---|
| 真 SDK ↔ r0semi 完整 `spawn_server`（a1_a5/a6） | 2 | ❌ 必挂（5/5 挂；偶发快速失败） |
| 真 SDK ↔ r0semi 完整 `spawn_server` | 3/4/5/8 | ✅ 0.37s 全绿、5 连跑稳定 |
| 真 SDK ↔ 最小协议正确服务器（双客户端） | 2 | ✅ 正常（发送/超时/join 全通） |
| 真 SDK ↔ 裸 TCP（读版本字节后停读） | 2 | ✅ 超时正常回 |
| e2e（手写客户端 ↔ 完整服务器） | current_thread / 4 | ✅ 稳定 |

即：2 线程下"完整服务器（任务数多）+ 双 SDK 客户端"触发，最小服务器不触发——疑似 tokio 多线程运行时
在任务数/唤醒时序特定组合下的唤醒丢失（Windows）。排除：非协议 bug（e2e 同构稳定）、非死循环、非 busy-spin。

## 影响评估

- 测试稳定性：2 线程下 CI 必红/时红——已用 4 线程绕过（conformance.rs 现已 4，与 e2e 对齐）；
- 生产：服务器用 tokio 多线程运行时正常运行（压测/e2e/长时间运行均无此现象），与测试 harness 的任务形状不同；
- 风险：若未来有人改回 2 线程或加更多并发测试，可能复现——留档避免重踩。

## 候选解决方案

- **方案 A（当前已采用）**：工作者线程数取 4，注释记录本 issue；
- **方案 B（深挖）**：用 tokio 官方诊断（`tokio-console`/`dump`）或升级 tokio 后复测，看是否上游 bug/已在某版本修；
  收益低（生产无此问题），建议仅在本 issue 里挂"升级 tokio 后复测"的待办。

## 验收标准

- conformance 在 CI 上连续多跑绿（当前 4 线程已满足）；
- 若未来升级 tokio 依赖：顺手复测 2 线程，若已修复则回写本 issue 关闭。

## 关联

- conformance.rs（崩溃猎手，client-behavior-review §5 A1–A6）
- client-conformance.md §已落地（坑位注记的文档锚点）