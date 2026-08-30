# 贡献指南（CONTRIBUTING）

> 本文档面向**人类协作者**。AI 协作者请先读 [AGENTS.md](AGENTS.md)（工作纪律摘要）与
> [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)（权威规范，1106 行），那里有 5 分钟上手所需的一切。

## 项目在做什么

r0semi-mp 是 Phira 联机房间服务器 `phira-mp`（TeamFlos，Rust）的**内存最小化重写**：
目标为：RSS 7–15MB、子系统可整体替换（契约分层）、协议完全兼容（真 Phira 客户端可直接连接）。

Rust workspace，5 个 crate

| crate | 角色 |
|---|---|
| `phira-api` | 契约层：协议编解码 / 命令字典 / 房间 trait |
| `phira-contract` | 契约测试套件 |
| `phira-core` | 柜台：会话 / 总线 / 路由 / 生命周期 |
| `impl-rooms-v1` | v1房间实现 |
| `phira-server` | 组合根：唯一接线处，持有二进制 |

## 开发环境与验证

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
python3 tools/check-deps.py
```

## 提 PR 前必读

- **依赖方向红线**：`impl-*` 只认识 phira-api，连 core 都不许认识；新增 crate 必须同步更新
  `tools/check-deps.py` 的 ALLOW，否则 CI 红。
- **契约变更走 §5.6**：枚举加变体必须 `#[non_exhaustive]` + 契约测试补用例；破坏性变更走
  ADR + api 主版本。
- **新增系统命令碰 4 处**：`phira-api/src/rooms.rs`（枚举 + 文档）、`phira-core/src/bus.rs`
  （3 个 match 各一行）、`impl-rooms-v1/src/lib.rs`（handle 分支）、`phira-contract`（契约用例）。
- **时间/连接事实必须命令化**（§4.6）：impl 内禁止开后台任务/定时器/线程。
- **错误走 Err 不走 panic**：业务拒绝用 `RoomError::Business`，内部故障用 `Internal`。
- **安全锁记账平衡**（§10.4/ADR-0010）：改投递/写路径时必须保持 charge ↔ fetch_sub ↔
  Drop guard 记账守恒。
- **lint 红线**：全 workspace `forbid(unsafe_code)`；`phira-api` `missing_docs=deny`；
  clippy `pedantic` 全量 + `-D warnings`。
- **许可结构**：`binary.rs` / `stream.rs` / `proto.rs` 是 Apache-2.0 移植文件（SPDX 头），
  修改时保留头与 NOTICE 记录；新增整文件移植需同步更新 NOTICE。

## 许可与换牌约定

感谢为本项目贡献代码！通过提交任何形式的贡献（Pull Request、补丁、文档、测试用例等），您同意以下约定：

1. 您的贡献将按项目**现行许可**（当前为 AGPL-3.0-only，见 [LICENSE](LICENSE)）发布；
   您保留对贡献的版权，归属记录将保留在 [NOTICE](NOTICE) 中。
2. 您授予本项目及其维护者对您贡献的**全球的、永久的、不可撤销的、免版税的**的使用、修改、复制与
   再分发权利（范围以项目许可为准）。
3. 您同意：维护者有权在**提前公告**（不少于 30 天，公告于仓库 README 与 GitHub
   Discussions）后，将整个代码库（含您的贡献）变更许可至以下清单内的任一许可：
   **AGPL-3.0-or-later、GPL-3.0-or-later、MPL-2.0、Apache-2.0、MIT**
   （均为 OSI 认可的开源许可）。
   清单外的许可按第 5 条扩展流程增补。
   请注意，由于本项目及其维护者已获得贡献者的授权，因此无需征询贡献者的同意，也无需向贡献者支付任何补偿。
4. 您保证您有权作出上述授权（如您受雇于他人，请先获得雇主同意）。
5. 上述清单可由维护者扩展：扩展须经同款公告（不少于 30 天），公告期内未提出异议的
   贡献者视为同意扩展；异议者可要求其既有贡献维持变更前许可，或由项目移除。