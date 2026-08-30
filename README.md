# r0semi-mp

一个 **协议兼容** 的 Phira 联机房间服务器（[`phira-mp`](https://github.com/TeamFlos/phira-mp)，TeamFlos，Rust）重写版本。

与原版相比，本版本的不同之处：

- **极致的内存控制**：相比其他版本大幅降低内存开销，目标设定为 RSS 7–15MB 且已达成（实测稳态仅 4.3–5.2MB）。
- **零死锁并发架构**：重构核心调度模型，采用每房间独立的 **Actor 模型**，根除了多重锁竞争与死锁风险。
- **高内聚低耦合**：严格的契约分层与模块化设计，使开发者在替换或扩展各子系统时无需费心处理代码间的复杂耦合。
- **安全与自动化**：注重安全性防线与高测试覆盖率，并通过 GitHub Actions 实现自动化测试与夜间构建（Nightly）发布。

你可以在 [Releases / Nightly](https://github.com/Sczr0/r0semi-mp/releases/tag/nightly) 下载由 Action 自动编译的最新版本，请注意，该版本为 Linux 的 musl 版本，如有需要，请另行编译。

未尽事宜，你可以通过 [![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Sczr0/r0semi-mp) 获取本项目的更多架构与设计细节。

## 快速开始

需要 Rust 1.98+（低于 Rust 1.98 版本未经测试，不保证可用性）。

```bash
cargo build --release
cp server_config.example.yml server_config.yml   # 按需修改
cargo run --release -p phira-server
```

或使用 Docker Compose：

```bash
docker compose up -d
```

默认监听 `0.0.0.0:12346`。可用 `R0SEMI_MP_CONFIG` 环境变量指定其他位置的配置文件；所有配置项均有默认值，详见 [server_config.example.yml](server_config.example.yml)。

> [!WARNING]
> **Docker 部署提示**  
> Docker 部署方式目前未经充分测试，在完成整体开发之前，不保证该部署方式的绝对可用性。建议优先使用源码编译或 Nightly 二进制运行。

## 验证

```bash
cargo test --workspace              # 测试套件（含契约测试）
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
python3 tools/check-deps.py         # 依赖方向物理闸门
```

## Workspace 结构

| Crate | 角色职责 |
|---|---|
| `phira-api` | 契约层：协议编解码、命令/事件字典、房间 Trait（零 Tokio 依赖） |
| `phira-contract` | 契约测试套件 |
| `phira-core` | 柜台：会话、总线、路由、生命周期单一生产者 |
| `impl-rooms-v1` | v1 房间 Actor 状态机实现 |
| `phira-server` | 组合根：装配所有依赖，持有启动入口二进制 |

- 完整架构设计、ADR 决策记录与 Issue 追踪：详见 `docs/`。
- 协作者纪律（含 AI 协作者）：详见 [AGENTS.md](AGENTS.md)。
- 贡献方式与贡献者许可条款：详见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 许可与合规

本项目采用双重许可结构：

- **项目整体**：采用 **AGPL-3.0-only** 许可证，详见 [LICENSE](LICENSE)。
- **移植模块**：三个从原版（Apache-2.0）移植的文件保留原作者版权并在文件头声明 `SPDX-License-Identifier: Apache-2.0`：
  - `crates/phira-api/src/binary.rs`
  - `crates/phira-server/src/stream.rs`
  - `crates/phira-api/src/proto.rs`  
  Apache-2.0 许可证全文见 [LICENSE.Apache-2.0](LICENSE.Apache-2.0)。

原创文件中夹杂的少量原版片段与文档引用均已在行内标注；完整归属清单见 [NOTICE](NOTICE)。  
*注：`phira-mp` 协议本身（字段名、Tag、时序等事实性协议数据）不受本许可限制。*

## 特别鸣谢

- [Phira](https://github.com/TeamFlos/phira) - 官方客户端（GPL-3.0）
- [phira-mp](https://github.com/TeamFlos/phira-mp) - 官方参考服务端（Apache-2.0）
- [gooophira-mp](https://github.com/Pimeng/gooophira-mp) - Go 语言实现参考（AGPL-3.0）