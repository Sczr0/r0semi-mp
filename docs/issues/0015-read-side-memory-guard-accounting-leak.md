# ISSUE-0015：读侧内存守卫超限路径记账泄漏（fail-closed 不还账）

- 状态：已修复（2026-08-30，随覆盖率补全工作落地）
- 发现日期：2026-08-30
- 发现方式：覆盖率为补全防护路径测试改写 `charge_memory` 语义时审读
- 严重级：中（攻击面相关）
- 相关章节：ARCHITECTURE.md §10.4（安全锁 A 记账平衡承诺）、docs/performance-cpu.md §6

## 问题陈述

`charge_memory` 原实现：

```rust
IN_FLIGHT_BYTES.fetch_add(bytes, Ordering::SeqCst) + bytes <= MEMORY_GUARD_LIMIT
```

先加后判，**失败时已加的字节不归还**。投递侧（SessionSink::deliver）失败后显式
`fetch_sub + release_memory` 自我补救；但**读侧**（stream.rs 读循环）失败分支是
`bail!("read-side memory guard exceeded")` 直接断连——`ReadCharge` 守卫只在成功路径
创建，失败路径的 `fetch_add` 永远留在全局账上。

## 证据（文档承诺 vs 代码实际）

- 承诺：§10.4「投递 charge ↔ 写任务 fetch_sub ↔ Drop guard 兜底——任何退出路径账目必然平衡」
- 实际：读侧超限 → 断开 → 全局账永久虚增该帧字节。攻击者用声明大帧触发一次超限，
  全局水位即被抬高一次（效应累积但方向相反：水位越高越容易触发超限）；长跑进程
  的有效守卫上限会单调萎缩到 0。

## 影响评估

- 生产默认 64MiB 上限 + 每连接 2MiB 帧：需 ~32 个并发连接同时声明大帧才可能首次触发；
  此后水位永久 +32MiB… 攻击者可控累积，最终任何大帧投递都被拒（丢新 + 断最重连接），
  触发**服务降级而非内存膨胀**——不会 OOM，但守卫失真，超限行为与文档语义（账目平衡）不符。
- 单次触发即留痕，运维可从 /healthz 的 in_flight 观测到水位不回落。

## 修复（2026-08-30）

`charge_memory` 改为原子语义：先加、后验、**超限回减**（失败 = 无净变化），
投递侧不再重复回滚全局账（其 `queue_bytes`（每连接账）回滚保留）：

```rust
pub(crate) fn charge_memory(bytes: usize) -> bool {
    IN_FLIGHT_BYTES.fetch_add(bytes, Ordering::SeqCst);
    let ok = IN_FLIGHT_BYTES.load(Ordering::SeqCst) <= memory_guard_limit();
    if !ok {
        IN_FLIGHT_BYTES.fetch_sub(bytes, Ordering::SeqCst);
    }
    ok
}
```

同时把上限抽成 `memory_guard_limit()`（默认常量 64MiB；`#[cfg(test)]` 覆盖开关
`set_memory_guard_limit_for_test` + `TEST_MEMORY_GUARD_MUTEX` 串行化），
兑现 AGENTS.md §8「上限为常量可参数化」的文档承诺。

## 验收标准（已达成）

1. 读侧 fail-closed 路径可用测试真实触发：`stream::tests::recv_loop_bails_when_global_guard_exceeded`
   ——覆盖水位压到 64B，发 200B 帧 → bail 断连，且断言 `in_flight_bytes() == 0`（无残留虚增）
2. 投递侧全局超限：`server::tests::deliver_global_limit_refunds_and_kicks_heaviest`
   ——断最重连接 + 轻连接不受伤 + 全局账回滚 + 被拒帧不入队
3. 既有 memory_guard 集成测试（投递侧记账增长/回落）继续全绿

## 关联

- 随覆盖率补全工作一起落地（同一批测试：slow_kick/admission/boot/admin_routes/
  reset_flood/http 错误分支/authed-cap/契约边角场景）
- 与 ISSUE-0011（EncodeCache 钉住）同族：记账/缓存守卫的失败路径可测试性