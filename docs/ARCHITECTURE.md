# r0semi-mp 架构设计文档

> 本文档描述 **r0semi-mp**（Phira 多人联机房间服务器 phira-mp 的重写）项目的架构设计：
> 我们要做什么、为什么这么设计、架构的好处、文件夹结构、以及如何从一开始就约束好。
>
> 版本：v3（第四轮评审后） · 状态：**冻结待执行**（自声明：只修单、抽 ADR、跑编译检查，不再新增章节——评审 §8） · 语言：简体中文

---

## 1. 我们要做什么

### 1.1 项目背景

Phira 是 Phigros（鸽游音游）的开源社区平台。官方联机房间服务器 `phira-mp` 是 TeamFlos 名下的 Rust 实现（约 3000 行），实现了一套纯 TCP 二进制协议（版本握手 + ULEB128 帧 + 命令枚举），支撑开房 / 选图 / 准备 / 联机游玩 / 观战转播等玩法。

我们决定**重写**这个服务器（下称 r0semi-mp），不照抄原版内部结构，只复用协议语义。

### 1.2 目标（按优先级）

| 优先级 | 目标 | 含义 |
|---|---|---|
| **P0** | 常驻内存最小化 | 服务端机器挤着一大堆服务，内存是稀缺资源。RSS 目标是原版（含 reqwest/rustls 的 ~30-50MB）的 1/3 到 1/2 |
| **P0** | 子系统可整体替换 | 房间管理、封禁、鉴权等任意子系统出现更好实现时，只换实现、不碰核心、不改其他模块 |
| **P1** | CPU 合理即可 | "不炸"就行，不做无意义的 CPU 极致优化 |
| **P1** | 协议完全兼容 | 真 Phira 客户端可直接连接 |
| P2 | 模块解耦、可单测、未来协作者入职成本低 | 工程质量要求，由架构本身保证（单人语境下"并行开发"无意义，评审 §8） |

> **灰度已从需求表移除**：它不是本项目需求，是未来运维选项（§3.2）——单人单实例下安全网 = 契约测试 + Oracle + 快重启，部署级灰度 = **入口粒度**（新入口）、零项目代码。

### 1.3 非目标（明确不做）

- 不做完整游戏服务端（排行榜、谱面库、论坛等）——那是官方 `phi-ch-server` 的事
- 不做运行时代码热替换（HMR）——服务端子系统替换不需要热卸载，**重启即换，成本最低**
- 不追求 CPU 极致——内存才是硬指标
- 不依赖数据库（v1 全部内存态，可后续通过可替换的 `Store` 接口扩展）

### 1.4 一句话

> **一个内存最省、核心最小、任何子系统都能整体替换的 Phira 房间服务器。**（灰度已降级为未来运维选项，§3.2）

### 1.5 三个最难的已决策问题（不是红利，是被显式设计过的）

> 本项目最大的风险曾是：**把最难的三个问题当作"已经解决的红利"来陈述**（评审 §8 总结）。以下三项全部是显式设计决策——每个都有问题陈述、取舍与文档位置。读文档时遇到它们，请按"设计"对待，不是按"免费赠品"。

| 问题 | 问题本质 | 决策 | 位置 |
|---|---|---|---|
| **并发模型** | `&self + Send + Sync` 的形状对实现形态（单例+锁 / actor）是决定性猜测 | 每房间一个 actor、命令串行、`&mut self` 无锁；断线事实由用户生命周期任务单一生产者按序派发 | §4.9 |
| **广播寻址** | core 要"广播"却不知道发给谁；影子状态必然漂移 | 事件自带 `room_id + targets`（impl 计算投递集，core 只投递）；路由表只存 user→room_id 元数据 | §4.4 / §4.9-5 |
| **灰度路由** | 命令不携带 room_id，且 L4 LB 也看不见 room_id（它在协议载荷里，connect 时刻只有四元组） | 已决策：进程内 A/B 不做；灰度已降级为非需求（§1.2/§3.2）。未来运维选项 = **入口粒度**（v2 新入口 + 引导新房主）；百分比级分流在无 redirect 命令的协议下不可实现 | §3.2 |

---

## 2. 为什么设计这个架构

### 2.1 原版的问题

原版 `phira-mp` 代码质量不错，但存在结构性缺陷：

1. **硬编码耦合**：鉴权、谱面、成绩全部硬编码调 `https://phira.5wyxi.com`，想换鉴权源必须改核心代码
2. **模块互相认识**：`session.rs` 直接调用 `room.rs`、`room.rs` 直接调用用户，换实现=改核心
3. **无法灰度**：换实现只能全量替换 + 重启，没有 A/B、没有回滚通道
4. **广播低效**：给每个接收者克隆整包（性能热点）
5. **依赖过重**：reqwest + rustls 一个 HTTP 客户端吃掉大量常驻内存

### 2.2 理论依据：时空可组合性

架构思想源自论文 **《A Programming Paradigm for Spatiotemporal Composability》**（cordiverse/paper，Cordis / Koishi 作者 Shigma 等）：

- **空间可组合性（Spatial Composability）**：模块声明自己的依赖（coeffect），提供方被替换时，依赖方按契约重新激活
- **时间可组合性（Temporal Composability）**：模块卸载时，其副作用被完整还原

本项目取其实质、舍其运行时成本：

| 论文机制 | 本项目的落地 | 取舍理由 |
|---|---|---|
| 空间可组合性（provide/consume 契约） | trait 契约 + 组合根 + 依赖方向矩阵 | **保留**——这是"子系统可换"的形式化基础 |
| 时间可组合性（运行时逆操作追踪） | 不做；Rust 的 RAII + 重启即换替代 | 服务端子系统替换不需要运行时热卸载 |
| 反应式 coeffect 通知 | 事件总线（广播） | 保留，但只用于事件，不用于命令 |

**核心结论**：空间可组合性在 Rust 里用"trait 契约 + 编译期选择"实现，成本为零；时间可组合性用重启替代，成本最低。

### 2.3 核心原则（五条，缺一不可）

**原则 1：契约先行（Contract First）**
契约分**两层**，诚实对待它们的来源（评审 §5）：
- **协议层**（ClientCommand/ServerCommand/Message，§6.3）：协议的直接投影，**无猜测成分**
- **内部契约层**（RoomCommand/CmdCtx/RoomEvent/Targets/系统命令）：**改写产物**——去上下文、加系统命令、发明协议中不存在的 Event 概念。**这部分就是设计，必须按设计对待**：纳入评审、可演进、有版本（§5.6），不能拿"协议投影"当免检通行证

两种类型都必须第一天定义在独立的契约 crate 里。

**原则 2：薄缝（Thin Seam）**
模块之间的接口只做"形状被契约类型与**已选定的并发模型**共同钉死的最小 trait"（§4.4/§4.9），不做预测性的丰富接口（15 个方法、能力声明、插件框架）。**过早抽象是"猜第二个实现的接口"，必然猜错**；薄缝是"类型长出来的插口"这句话只适用于协议投影部分——**内部设计部分（RoomActor 的形态、事件携带 targets）是被选中而非被钉死的，按设计评审（评审 §1/§5）**。并发模型必须先定义，否则 `&self + Send + Sync` 这个形状本身就是在猜。

**原则 3：组合根（Composition Root）**
只有 `main.rs` 认识所有模块。核心和实现互相不认识，各自只认识契约。耦合被集中到一处，其它人保持无知。

**原则 4：依赖方向（Dependency Direction）**
耦合的本质是"谁 import 谁"，不是"有没有接口"。依赖方向矩阵由 crate 边界 + CI 脚本强制，物理上不可违反。

**原则 5：抽象时机（Abstract When Second）**
类型第一天就位；**接口等第二个实现出现时**才成型。第一版就是"一个契约 + 一个实现"，不许提前抽象。

> **诚实注记（评审 §8）**：crate 拆分、契约测试套件等**面向第二个实现的成本，第一天确实预付了**。这是刻意的取舍：依赖方向必须靠 crate 边界才能被机器强制（`check-deps.py` 用 cargo metadata 只能查 crate 级，mod 边界无法检查），**接口设计可以等，防火墙不能等**。"先用 mod 边界跑通再拆"的反方案，代价是将来手术式抽取。该取舍记入 ADR（附录 A 模板）。

### 2.4 沟通用的比喻（商店模型）

> **一个柜台（core），几排货架（api 契约），货物（impl）只管符合规格、听柜台中转，老板（main.rs）唯一决定谁上架。换货物 = 老板换一批符合同规格的新货，柜台、顾客、其它货物全无感。**

- 老板只在开店前接线，接的全是"货物到柜台"的线
- 柜台是营业时唯一的电话交换机（点对点命令）和广播站（事件扇出）
- 货物之间永远没有直连线

---

## 3. 架构的好处

### 3.1 可整体替换（核心收益）

任何子系统（房间、封禁、鉴权、聊天策略）都满足"符合契约 → 即插即用"：

```rust
// 换实现 = 换工厂构造（概念演示；真实换实现 = 新 impl crate + 契约测试通过 + 部署级灰度，§3.2）
let rooms = RoomsV2::new(config.rooms.clone(), deps);   // 从 v1 换到 v2
```

其他模块、客户端、协议——全部无感。V2 上线前必须通过同一套契约测试（见 5.3）。

### 3.2 灰度发布（已降级：未来运维选项，非本项目需求）

**灰度不是本项目需求**（§1.2）：单人单实例下，安全网 = 契约测试（§5.3）+ Oracle 对照（§9）+ 秒级重启——换实现 = 组合根换工厂 + 测试全绿 + 重启，不需要灰度。

**为什么项目里不写灰度代码（决策记录，评审 §3）**：

1. **路由键缺失**：协议命令（LeaveRoom/Ready/Played…）不携带 room_id，房间隐含在会话中——进程内分流需要 user→impl 影子映射（中间件持有业务状态，必然漂移）
2. **影子流量代价**：系统命令双写、v2 事件丢弃、HTTP 回源翻倍（官方 API 限流）——收益不抵复杂度
3. **符合原则 5**：灰度设施等"第二个实现出现"再说；v1 没有第二实现

**若未来多实例运营真需要（零项目代码的运维选项，评审 §8 修正）**：
- **入口粒度**：Phira 是服务器列表模型——房间隶属于服务器入口，加入某房必须连到该入口。v2 = **新增一个服务器入口**（独立地址）；灰度 = 引导一部分新房主在 v2 入口建房；回滚 = 下架入口（秒级）；错误率对比 = 两个入口各自的 Metrics。"同房间同进程"自动成立，零额外机器
- **为什么不做 LB 一致性哈希（收回第一轮建议）**：L4 LB 在 connect 时刻只有四元组，room_id 首次出现在 CreateRoom/JoinRoom 帧内（协议交换之后）——LB 看不见哈希键；按连接分流又违反"同房间同进程"；跨进程房间转发需要 redirect/迁移机制（协议没有）——那是伪装成 LB 的第三个服务器
- **上限声明**：百分比级机械分流在该协议下没有廉价实现，**入口级是上限**；若未来真需要百分比级，前提是协议加 redirect 命令（记入未来项，不在本项目）。**现实中的降级路径**：入口对命中灰度组的 `CreateRoom` 返回业务错误并提示新入口地址（错误提示引导——任何旧客户端可用，体验粗糙）；自动重连需官方协议加 redirect 且客户端配合——**客户端是官方的、玩家用官方客户端，单靠社区服主做不了，必须官方协议演进**

**保留的进程内设施**：`Metrics`（原子计数器，评审 §8：不再叫 Interceptor——它不是中间件）——v1 不引入"中间件"概念，只是总线内的原子计数器集合（每命令类型 成功/失败/延迟），健康检查（§11.1）与换实现后的验证共用；错误率只统计 `RoomError::Internal`——业务拒绝（房满/越权）是预期行为，混入会扭曲对比（评审 §8）。中间件包装等第二个实现出现再考虑（原则 5）

### 3.3 内存最小化（P0 落地手段）

- Rust（无 GC、无运行时，异步任务无栈）
- 广播零拷贝：`bytes::Bytes` 序列化一次共享给所有接收者（§4.8-2）
- HTTP 客户端用**轻量异步**实现（ureq-3 async 或最小化 hyper），**禁止阻塞客户端进 async 上下文**（评审 §6，详见 §4.9-7）；TLS 用 rustls 单栈（https 回源躲不掉，见 §10.1）
- tokio `current_thread`（或 ≤2 worker）+ 小栈任务
- 分配器可选 mimalloc 减碎片
- 目标：几百连接常驻 RSS ~7-15MB（预算明细见 §10）

### 3.4 其它收益

- **未来协作者入职成本低**：模块互不认识，新人只需读契约 + 一个 impl（单人语境下"并行开发"无意义，评审 §8）
- **可测试性**：契约测试对 trait 泛型编写，任何实现一键验证（见 §9）
- **编译期安全**：crate 边界 + `forbid(unsafe_code)` + 依赖白名单，架构违约无法合并
- **生态对齐**：可复用原版 `phira-mp-common` 协议层（Apache-2.0），省 40% 工作量

### 3.5 寻址：域名后接端口的解法（不是协议限制，是寻址习惯）

**定性**：TCP 需要显式端口、协议明文无 TLS、无 IANA 默认端口——`domain:port` 是客户端寻址习惯，不是协议限制（协议层只有版本握手，不管寻址）。

**三层解法（按成本）**：

| 层 | 方案 | 成本 | 生效条件 |
|---|---|---|---|
| 约定 | 默认端口 **12346**（原版默认值，README 可查；评审 §8 六：此前"社区事实标准"无出处），广告省略端口 | 零 | 客户端有缺省端口行为（需核实官方客户端） |
| DNS | **SRV 记录** `_phira._tcp.<domain>` → host:port——**SRV 是客户端解析行为，不是协议的一部分**（协议层只有版本握手）；`phira-mp-client` 的 `resolver.rs` 已实现"端口优先"两级逻辑（已读码确认）；**官方游戏客户端是否支持 = 附录 D 待核实项**（评审 §8 五） | 需 DNS 控制权 | 新版客户端（旧客户端不认识 SRV） |
| 生态 | **服务器列表**（Phira 是服务器列表模型）：玩家从列表选择入口，不手输地址 | 需注册 | 官方列表机制 |

**本项目动作**：服务器默认跑 12346；文档指引 SRV 配置；`phira-mp-client` 复用 resolver 做测活/测试工具。端口问题在"列表模型"下自然消失——它与灰度分流（§3.2）共用同一个**入口模型**：寻址 = 入口被找到的方式，灰度 = 入口被替换的方式。

**两级寻址的具体形态**：`phira-mp-client` 的 `resolver.rs` 已是"端口优先、无端口才查 SRV"的两级逻辑——显式端口直连、裸域名查 `_phira._tcp.<domain>`。新旧客户端可共存（服务器侧零改动，只监听端口）：

```
老客户端（不认识 SRV）：玩家输 re0.r0semi.net:3939  → 直连
新客户端（支持 SRV）：  玩家输 re0.r0semi.net       → _phira._tcp.re0.r0semi.net → 3939
```
（示例用你的实际端口 3939；文档默认 12346 是原版默认——端口是部署选择，两者不冲突，评审 §8 六）

DNS 配置（SRV 目标必须是 A/AAAA 记录，勿用 CNAME 链）：

```
_phira._tcp.re0.r0semi.net.  IN  SRV  0 5 3939  re0.r0semi.net.
```

裸 IP 不支持 SRV（resolver 显式拒绝，必须带端口）。

---

## 4. 文件夹结构

### 4.1 完整结构

```
r0semi-mp/
├── Cargo.toml                  # [workspace] + workspace.lints + workspace.dependencies
├── rust-toolchain.toml         # 钉死工具链版本（可复现构建）
├── deny.toml                   # cargo-deny：第三方依赖许可审查
├── docs/
│   ├── ARCHITECTURE.md         # 本文档
│   └── adr/0001-*.md           # 架构决策记录（为什么这么定）
├── tools/
│   └── check-deps.py           # 依赖方向检查脚本（给 CI 跑）
└── crates/
    ├── phira-contract/           # 契约测试套件库（评审 §4：virtual manifest 根目录的 tests 不属于任何 package）
    │   └── src/rooms.rs          #   泛型套件 room_contract_suite<F: RoomFactory>，只依赖 api；各 impl 的 tests/ 引用之
    ├── phira-api/              # 【契约】只有类型 + 薄缝 trait。禁止依赖任何内部 crate
    │   ├── Cargo.toml          #   依赖：仅 thiserror/half 等轻量库，零 tokio（评审 §8 六：ForwardRaw 删除后 bytes 不再进 api；uuid 属 core 会话层）
    │   └── src/
    │       ├── rooms.rs        #   RoomCommand / CmdCtx / RoomEvent / RoomFactory / RoomActor / RoomDeps
    │       ├── auth.rs         #   AuthHandler（token → 身份，评审 §4）
    │       ├── mod.rs
    │       └── lib.rs          #   #![forbid(unsafe_code)] #![deny(missing_docs)]
    ├── phira-core/             # 【柜台】会话 + 总线 + 配置。只依赖 api
    │   └── src/
    │       ├── bus.rs          #   命令路由 + 事件广播 + 拦截链
    │       ├── session.rs      #   连接生命周期、心跳、协议帧
    │       ├── config.rs       #   配置加载与热重载
    │       └── lib.rs
    ├── impl-rooms-v1/          # 【第一个货物】房间实现（照原版语义）
    └── phira-server/           # 【老板】bin crate，组合根，唯一认识所有人的地方（crate 名保留 phira-*；**二进制输出名 r0semi-mp-server**，Cargo.toml 设 `[[bin]] name`）
        └── src/main.rs

（未来目标结构，不在 Day 1：`impl-mod-memory/` 封禁实现，阶段 4，§14——评审 §8）

**命名定案**（评审后）：仓库 `r0semi-mp`、二进制 `r0semi-mp-server`、crate 保留 `phira-*`——契约 crate 名描述"它讲 phira 协议"而非"它属于哪个项目"，将来换语言时 `phira-api` 作为可移植规格书保留 phira 前缀有语义价值（方案 b）。原版 crate（`phira-mp-common`/`phira-mp-client`）是复用对象，名字不动。
```

### 4.2 命名规则（命名即约束）

| 前缀 | 角色 | 该认识谁 |
|---|---|---|
| `phira-api` | 契约 | 谁都不认识（只依赖 std/基础库） |
| `phira-core` | 柜台 | 只认识 api |
| `impl-*` | 可换货物 | 只认识 api |
| `phira-server` | 老板（bin） | 认识所有人 |

### 4.3 依赖方向矩阵（硬约束）

| 谁 → 依赖谁 | phira-api | phira-core | impl-* | phira-server |
|---|---|---|---|---|
| phira-api | - | ✗ | ✗ | ✗ |
| phira-core | ✅ | - | ✗ | ✗ |
| impl-* | ✅ | ✗ | ✗ | ✗ |
| phira-server | ✅ | ✅ | ✅ | - |

四条铁律：

1. **phira-api** 只依赖 std 和极少数基础库（thiserror、half），**零 tokio、零运行时**（uuid 属 core 会话层，评审 §8 五-3）
2. **phira-core** 只认识 **phira-api**
3. **impl-\*** 只认识 **phira-api**——**连 core 都不许认识**（impl 不 import core，全靠薄缝接口交互，事件通过返回值带出）
4. **phira-server** 是唯一允许认识所有人的 crate

### 4.4 薄缝的完整形态（契约 crate 的核心）

```rust
// crates/phira-api/src/rooms.rs
pub type TimeMs = u64;   // 单调毫秒；impl 唯一时钟源是 Tick（§4.9，测试可伪造）

pub enum Origin { Client { user_id: i32 }, System }

pub struct CmdCtx {
    pub origin: Origin,
    pub room_id: RoomId,   // 路由目标（core 盖章，§4.9）
}

pub enum RoomCommand {
    // —— 客户端命令（与 §6.3 全量对齐；room_id 在 CmdCtx。评审 §8：此前缺 LeaveRoom/RequestStart/Touches/Judges）——
    CreateRoom { id: RoomId },   // 自带 room id（路由目标是新建房间，§4.9-4）
    JoinRoom { id: RoomId, monitor: bool },   // 自带 room id（路由目标是目标房间）——"唯一自带"是错的，评审 §8
    LeaveRoom,
    Chat { message: Varchar<200> },
    SelectChart { id: i32 }, RequestStart, Ready, CancelReady, Abort,
    Played { id: i32 },
    LockRoom { lock: bool }, CycleRoom { cycle: bool },
    Touches { frames: Arc<Vec<TouchFrame>> },   // 热路径入口（§6.5-17）
    Judges { judges: Arc<Vec<JudgeEvent>> },

    // —— 系统命令（柜台驱动，见 §4.6/§4.9）——
    Tick { now: TimeMs },
    UserDisconnected { user_id: i32, epoch: u64 },   // epoch：会话纪元（§4.9-3，评审 §8）
    UserReconnected { user_id: i32, epoch: u64 },
    UserDangleExpired { user_id: i32 },   // 不携带 epoch：生命周期任务仅在确认当前纪元无活会话后派发（§4.9-3 窗口边界），语义已关联（评审 §8 四-3）
    GetClientState { user_id: i32 },   // 重连恢复用（§6.5-23）
    UpdateConfig { config: Arc<RoomConfig> },  // 配置热重载（§4.9-8，评审 §8）
}

/// 内部错误是结构化的：Business（业务拒绝：房满/越权，预期行为）与 Internal（内部故障）分开——
/// 错误率/灰度只统计 Internal，业务拒绝混入会扭曲对比（评审 §8）；
/// 协议层的 Err(String) 由 core 从 RoomError 生成（Business 透传文案，Internal 返回通用文案 + 日志）
pub enum RoomError {
    Business { code: RoomErrorCode, msg: String },
    Internal { msg: String },
}
pub enum RoomResponse {
    Ok,
    Failure(RoomError),
    JoinRoom(JoinRoomResponse),
    ClientState(Option<ClientRoomState>),   // GetClientState 用（§6.5-23）
    // 其余命令的成功响应即 Ok（协议 Result 变体由 core 按命令映射）
}
// 响应关联不变量（评审 §8 一）：每命令一次 handle、一次响应，channel FIFO + 分发配对保证按序对应——
// core 按"自己刚分发的命令"把 RoomResponse 映射到对应 ServerCommand::X(Result)；
// 因此 Failure 无需携带命令判别（若未来允许乱序响应，才需加 CmdKind 字段）

/// 事件分类学（评审 §8 二/四）：领域事件投递目标恒为房内 All（已核实无全服广播），不再携带 targets——
/// 恒 All 是死重量兼错误面；仅转发指令携带 targets（Specific）；core 信号仅 core。
/// | 类别 | 投递目标 | 路由增量 |
/// | 领域事件 | 房内 All（成员 + 观察者） | RoomCreated/UserJoined 增、UserLeft 删 |
/// | 转发指令（RelayTouches/RelayJudges） | 仅 monitor——不进观察者通道（480/s 触摸字节无语义） | 无 |
/// | core 信号（RoomClosed） | 仅 core | 删房间 |
pub enum RoomEvent {
    // —— 领域事件（与 §6.3 Message 一一对应，评审 §8 一：全量穷举，无省略号）——
    Chat        { room_id: RoomId, user: i32, content: String },
    RoomCreated { room_id: RoomId, host: i32 },        // Message::CreateRoom + 路由增量(host→room)
    UserJoined  { room_id: RoomId, user: UserInfo },   // Message::JoinRoom + 路由增量
    UserLeft    { room_id: RoomId, user: i32 },        // Message::LeaveRoom + 路由增量（含驱逐：无独立协议对应物）
    NewHost     { room_id: RoomId, new_host: i32, old_host: i32 },   // Message::NewHost + ChangeHost(双向，表 2)
    SelectChart { room_id: RoomId, user: i32, name: String, id: i32 },
    GameStart   { room_id: RoomId, user: i32 },
    Ready       { room_id: RoomId, user: i32 },
    CancelReady { room_id: RoomId, user: i32 },
    CancelGame  { room_id: RoomId, user: i32 },
    StartPlaying{ room_id: RoomId },
    Played      { room_id: RoomId, user: i32, score: i32, accuracy: f32, full_combo: bool },
    GameEnd     { room_id: RoomId },
    Abort       { room_id: RoomId, user: i32 },
    LockRoom    { room_id: RoomId, lock: bool },
    CycleRoom   { room_id: RoomId, cycle: bool },
    // —— 转发指令（结构化；core 编码一次、共享 Bytes，§6.5-17；仅此类携带 targets）——
    RelayTouches { room_id: RoomId, targets: Targets, player: i32, frames: Arc<Vec<TouchFrame>> },
    RelayJudges  { room_id: RoomId, targets: Targets, player: i32, judges: Arc<Vec<JudgeEvent>> },
    // —— core 信号 ——
    RoomClosed  { room_id: RoomId },   // 空房自毁：core 排空 channel、drop sender、拆任务（§4.9-9）
}
// UserDangled 已删（评审 §8 二-2）：驱逐的广播就是 UserLeft，无独立协议对应物；观察者如需区分可在 v2 给 UserLeft 加 reason

/// 并发模型：每房间一个 actor，命令经该房间的 channel 串行进入（§4.9）
/// deps 由工厂持有（组合根注入一次），create 不再收第二份（评审 §8：双注入只留一个）
pub trait RoomFactory: Send + Sync {
    fn create(&self, room_id: RoomId) -> Box<dyn RoomActor>;
}
/// 对象安全：经 async-trait / trait-variant 实现（§4.7 规则），供 core 以 Box<dyn RoomActor> 持有
#[async_trait]   // 与 §4.7 一致（评审 §8 四-1：两处形态统一）
pub trait RoomActor: Send {
    /// 回话：多数系统命令无回话；GetClientState 例外——并入生命周期任务后经 oneshot 转发回鉴权编排（§6.5-23，评审 §8 二-5）
    async fn handle(&mut self, ctx: CmdCtx, cmd: RoomCommand) -> (Option<RoomResponse>, Vec<RoomEvent>);
}

/// 外部依赖全部经构造器注入（§4.9，契约测试可 mock）
pub struct RoomDeps {
    pub api: Arc<dyn ApiClient>,      // 回源：chart/record（规则 10/15）
    pub rng: Arc<dyn RandomSource>,   // 房主随机选择（规则 5）
    // 时间不注入 —— impl 唯一时钟源是 Tick
}

/// 回源 HTTP 契约（评审 §8 三）：**每次请求必须自带超时（如 5-10s）**——无超时的挂起 = 房间永久冻结 +
/// 生命周期事实在 bus 侧无限等待 + 该房玩家被"丢弃断连"（外部观察即集体掉线）；超时/网络错归 Internal
#[async_trait]
pub trait ApiClient: Send + Sync {
    async fn fetch_chart(&self, id: i32) -> Result<Chart, ApiError>;    // 超时 ≤10s
    async fn fetch_record(&self, id: i32) -> Result<Record, ApiError>;  // 超时 ≤10s
}
pub enum ApiError { Internal { msg: String } }   // 归 RoomError::Internal / AuthError::Internal

// —— 鉴权契约（评审 §8：此前只有文件名；它是重连编排的枢纽，§6.5-19/23）——
#[async_trait]   // §4.7 规则：api 中所有 async trait 一律对象安全（core 必然以 dyn 持有）
pub trait AuthHandler: Send + Sync {
    /// 每次调用必须自带超时（如 5s，评审 §8 三）：鉴权挂起会卡死会话建立
    async fn authenticate(&self, token: &str) -> Result<UserIdentity, AuthError>;
}
pub enum AuthError {
    Business { code: AuthErrorCode, msg: String },  // token 无效（客户端可见）
    Internal { msg: String },                        // 官方 API 不可达 → §12 降级策略
}
pub struct UserIdentity { pub user_id: i32, pub name: String, pub lang: String }
// core 编排：token → AuthHandler → 用户注册表（core）→ 旧会话替换（epoch+1）→ GetClientState 恢复房间
```

### 4.5 组合根示例（老板的活，就这几行）

```rust
// crates/phira-server/src/main.rs —— 唯一认识所有人的地方
#[tokio::main]                        // 评审 §1：sync main 里 .await 编译不过
async fn main() -> Result<()> {
    let config = Config::load()?;

    // 老板接线：决定谁上架 + 注入外部依赖（§4.9）
    // 单一 HTTP 实例，auth 与 chart/record 共享（评审 §8 五-1：此前注释说共享、代码 new 了两个）
    let http = Arc::new(HttpApiClient::new(config.api.clone()));
    let deps = RoomDeps {
        api: Arc::clone(&http),
        rng: Arc::new(ThreadRng::default()),                    // 生产随机源
    };
    let rooms = RoomsV1::new(config.rooms.clone(), deps);       // 第一个货物（工厂，持有 deps）

    // v1 生产实现直接放组合根，第二实现出现再抽 impl-auth crate（原则 5 对自己生效）
    let auth = Arc::new(HttpAuth::new(Arc::clone(&http)));

    let bus = Bus::new(rooms, auth);
    bus.attach(Metrics::new());                                 // 计数器：每命令错误率（§3.2）
    bus.watch_config(config.clone());                           // 配置热重载 → UpdateConfig（§4.9-8）

    Server::new(config, bus).run().await?;                      // 柜台开业
    Ok(())
}
// 换实现 = 组合根换工厂（§3.2：灰度已降级为运维选项，项目内零灰度代码）
```

### 4.6 工程细节一：后台时间事件走同一薄缝（禁止 impl 内开后台线程）

**场景**：掉线 10s 超时踢出、打歌倒计时结算——不是客户端命令触发的，是后台时间到了触发的。

**规则**：impl 内部禁止开后台线程/任务去"偷偷广播"或直接改共享状态。**时间与连接事实也必须变成命令，走统一的薄缝**（§4.4）。

分工：

- **core（柜台）拥有时间与连接生命周期**：session 任务检测断线；**用户生命周期任务（单一生产者）**按序派发 `UserDisconnected{user, epoch}` →（`UserReconnected{user, epoch}` | `UserDangleExpired{user}`）；定时器按固定节拍（如 500ms）派发 `Tick { now: TimeMs }` 给活跃房间
- **impl（货物）拥有游戏语义**：`UserDisconnected` 标记缺席、`UserReconnected` 恢复座位、`UserDangleExpired` 执行驱逐（踢人/迁移房主/广播）；打歌倒计时由 `Tick` 推进。**impl 不再自己计时**——计时归 core 生命周期任务（§4.9）
- 系统命令没有要回话的客户端 → 返回值是 `Option<RoomResponse>`（§4.4 已改）

```rust
// impl 内部的状态推进：每房间一个 actor 实例，&mut self 独占状态，无锁（§4.9）
pub struct RoomV1 { absent: HashSet<i32>, /* ... */ }
#[async_trait]   // §4.7：对象安全，供 core 以 Box<dyn RoomActor> 持有
impl RoomActor for RoomV1 {
    async fn handle(&mut self, ctx: CmdCtx, cmd: RoomCommand) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        match cmd {
            RoomCommand::UserDisconnected { user_id, .. } => {
                // 标记缺席（计时在 core 生命周期任务，impl 只记状态）
                self.absent.insert(user_id);
            }
            RoomCommand::UserReconnected { user_id, .. } => {
                self.absent.remove(&user_id);   // 窗口内重连：座位保留
            }
            RoomCommand::UserDangleExpired { user_id } => {
                // 执行驱逐：移除座位、广播 LeaveRoom、若为房主则迁移（规则 5/21）
                self.absent.remove(&user_id);
                self.evict(user_id, &ctx.room_id)   // 产出带 targets 的事件
            }
            RoomCommand::Tick { now } => {
                // 玩法倒计时：到期产出 GameEnd 等事件
                // （dangle 不在此结算 —— 已归 core 生命周期任务）
            }
            // ...
        }
    }
}
```

**为什么禁止 impl 内开后台任务**：

1. **状态机纯度**——`handle` 是唯一入口，契约测试可穷举；后台线程不可测、不可替换、不可灰度
2. **可替换性**——后台任务会持有 impl 内部状态引用，换实现时泄漏
3. **竞态温床**——原版 `dangle` 的 `tokio::spawn` + Weak 引用升级就是这类问题

**优化路径（v2 再考虑，且不动契约）**：bus 层跳过空/无活跃玩家的房间的 `Tick` 派发——纯 bus 优化，不需要改薄缝（**评审 §8：给 `RoomActor` 加 `next_deadline()` 方法本身是一次破坏性契约变更**，须走 §5.6 + ADR + 主版本）。v1 用粗粒度 Tick 就够（几百房间 × 2Hz 的唤醒量可忽略）。

### 4.7 工程细节二：async fn in trait 与对象安全（随并发模型修订）

Rust 1.75+ 支持原生 `async fn` in trait，但 **RPITIT（返回位置 impl Trait）不满足对象安全**。原设计用"单例 Handler + 组合根枚举分发"回避 dyn；**并发模型改为每房间 actor 后（§4.9），core 的房间表是 `HashMap<RoomId, Sender<Envelope>>`——actor 跑在自己的任务里，Box 在任务手上（评审 §8：core 持有的只能是 channel sender，销毁即 drop sender）**，dyn 成为必然容器，枚举分发无法覆盖。

**结论（随并发模型修订）**：薄缝 trait 直接以对象安全形式定义：

```rust
// api/rooms.rs —— 薄缝的最终形态（对象安全）
#[async_trait]   // 或 #[trait_variant::make(RoomActorDyn)]，二选一
pub trait RoomActor: Send {
    async fn handle(&mut self, ctx: CmdCtx, cmd: RoomCommand) -> (Option<RoomResponse>, Vec<RoomEvent>);
}
```

- `async-trait`：成熟稳定；每次调用一次 Box 分配 + 一次虚调用——本场景每命令一次，远可接受
- `trait-variant`：生成对象安全伴生 trait，语义更接近原生 async
- **规则（评审 §8）**：**api 中所有 async trait 一律以对象安全形式声明**（async-trait 或 trait-variant）——core 只认识 api、必然以 `Arc<dyn …>` / `Vec<dyn …>` 持有；本规则对未来新增 trait 自动生效（`AuthHandler`/`Moderator` 已按此声明），不靠每次记得
- 热路径 `Touches`（60Hz）也走 `handle`：8 玩家 × 60Hz ≈ 500 次/s/房，每次多一次分配仍可忽略；若未来实测吃紧再考虑旁路——**旁路会改变可观察顺序（Touches 可能晚于 Abort 到达 monitor），顺序语义是契约的一部分（评审 §8），因此 v1 不开旁路，Touches 与其它命令同通道保序**

**零成本替代（可选，v2 再考虑）**：core 泛型化 `Server<F: RoomFactory>` + 组合根枚举 `enum AnyActor { V1(RoomV1), V2(RoomV2) }`，无 dyn 无宏；代价是 core 带泛型参数、V1/V2 需同表共存时枚举胶水。v1 不做。

### 4.8 工程细节三：三个选型定案（敲第一行代码前定死）

1. **f16 用 `half` crate**：`phira-api` 依赖 `half`（纯数学转换、no_std、零运行时依赖），处理协议要求的半精度坐标 `CompactPos`（§6.2）。符合 api 的"零 tokio、轻依赖"红线。
2. **共享广播缓冲用 `bytes::Bytes`**：观战转播的零拷贝广播（§6.5-17）统一用 `bytes::Bytes`——O(1) 切片 + 引用计数克隆，与 tokio 写路径天然契合。比 `Arc<[u8]>` 更优（后者切片无法零拷贝）。**core 的传输层（session 写路径）签名用 `Bytes`** 而不是裸 `Vec<u8>`（Bytes 属 core 依赖，不进 api——评审 §8 六：ForwardRaw 删除后 api 无 Bytes 消费点）
3. **HTTP 嗅探用 `socket.peek`**：测活（§11.1 方案 B）在 accept 后 peek 前几字节分流——`0x01` 走 MP 协议，`b"GET "`/`b"HEAD"` 走 HTTP 分支；peek 不消费数据、不污染 MP 状态机；循环等到 ≥1 字节再判（首字节即可区分协议）。

### 4.9 并发模型与外部依赖注入（编码前必须定死，评审 §1/§5）

**并发模型：每房间一个 actor，命令串行进入。**

```
┌─ session 任务 ──────┐        ┌───────────────────────────────┐
│ 收包→解码            ├──┐     │ bus：路由表(user→room_id 元数据) │
└─────────────────────┘  │     │  + 每房间 mpsc channel         │
┌─ 用户生命周期任务 ─────┼──┼──► │  + 每房间一个 actor 任务        │
│ 断线/重连/超时(单生产者) │  │     │ RoomActor::handle 串行执行     │
└─────────────────────┘  ├──►  └───────────────┬───────────────┘
┌─ 定时器(500ms) ────────┘                      ▼
│ 派发 Tick{now:TimeMs}     每个房间一次 handle：决策+副作用一体
└─────────────────────     （HTTP 回源也在其中，见规则 2）
```

**规则**：

1. **每房间串行**：同一房间的命令（客户端命令、系统命令、Tick）都经该房间的 channel FIFO 进入，`&mut self` 独占状态——无需内部锁，`handle` 内可安全 `.await`（HTTP 回源）
2. **队头阻塞边界（已读码核实，评审 §8）**：**原版不冻结同房其它命令**——`get_room!` 克隆 Arc 后 guard 立即释放、`reqwest` 在锁外、`room.state.write()` 在 HTTP 之后，且各会话跑在独立 task。actor 模型的每房串行是**相对原版的行为回退**（不是"可接受代价"）：结算校验期间该房命令全部排队，8 条成绩串行回源时尾玩家等 8×RTT。**缓解（v1 采用）**：(a) 热路径可丢（规则 9）；(b) 每连接限速；(c) 结算突发可预期（同房同曲同时收尾，Played 集中在歌曲结束后）。若实测仍有问题，v2 拆两段（actor 派发 `VerifyRecord` 副作用 → core HTTP 任务 → `RecordVerified` 回注）。**禁止**用共享全局锁串起所有房间
3. **顺序保证（评审 §8 补全输入侧）**：派发序由单一生产者保证，但**输入侧竞态必须闭环**：
   - **窗口边界**：`UserDangleExpired` 派发前，生命周期任务先查权威会话状态（该 user_id 当前是否有活会话）——重连通知的入队序 ≠ 墙钟序，9.999s 的重连可能排在 10.000s 的定时器后；盲发会踢掉刚重连的用户
   - **会话纪元**：生命周期事实的单位是 `(user_id, epoch)`；替换会话时 epoch+1 且**关闭旧 TCP、取消旧会话任务**（其死亡事实随之消失）
   - **旧连接失效**：替换后旧 TCP 到达的命令以 epoch 校验拒绝——否则同 id 双活连接的命令混进同一房间 channel，顺序语义被未定义交织
   - **第四竞态·幽灵座位（评审 §8 二-1）**：入房时序是 actor 返回 UserJoined → bus 应用表增量 → 发响应；客户端入房后立刻断线（RST 即时可见）时，生命周期任务查表路由 `UserDisconnected` 可能撞上**增量未应用**的窗口（bus 忙时拉大）——表 miss → 事实被丢 → 无 dangle 窗口 → 幽灵座位卡死 WaitForReady。修法（措辞收敛，评审 §8 二-4）：**表写仅经 bus 分发步骤（§4.9-4），生命周期任务只读；表 miss 时挂起重放**——current_thread 单线程下无数据竞争，但 await 交错仍存在，重放兜底
   - **GetClientState 并入生命周期任务（评审 §8 二-2）**：它与 UserReconnected 来自不同生产者会乱序——**所有 per-user 系统命令（含 GetClientState）并入单一生产者**，顺序问题从源头消除；若 `ClientRoomState` 不含 actor 内部状态（absent 等）则无序本无害，但单一生产者更干净
   以上均为 **core 行为**，测试位置是 **phira-core 集成测试 + 脚本化假 actor**（评审 §8 六：契约套件对 factory 参数化、直驱 actor，测不到 epoch 校验/表先于响应/流水线——此前写"各配一条契约测试"是错的）
4. **路由表只存元数据 + 更新先于响应**：core 维护 `user → room_id`（仅 id，由事件维护），用于命令路由；**不复制任何房间状态**（评审 §2）。**顺序不变量（评审 §8）**：bus 在同一处理步骤内**先解析事件 targets（All 反解）→ 再应用事件携带的路由增量 → 再发送响应 oneshot**——"先解析后应用"保证离开者仍收到自己的 LeaveRoom（原版先广播后移除；UserJoined 对称情形同理，**进 Oracle 核实清单**）；"先应用后响应"保证流水线客户端 `JoinRoom → SelectChart` 不会收到"你不在房间里"。core 编排测试覆盖该流水线
   **路由规则（评审 §8 二-3）**：`CreateRoom`/`JoinRoom` 靠载荷里的 id 路由（用户还不在目标房间，表查不到）；其余客户端命令靠表路由，**表 miss → 回"不在房间"错误**；系统命令按 room_id 直接路由
5. **事件自带路由**：`RoomEvent` 携带 `room_id + targets`，`targets ∈ {All, Specific}`（§4.4）——`All` 由路由表反解（core 侧，成员 = 表内映射），`Specific` 由 impl 计算（角色在 actor 内，如 monitor 列表）。core 只执行投递
6. **外部依赖注入**（评审 §5，契约测试的兑现前提）：
   - **时钟**：`Tick { now: TimeMs }`（u64 毫秒）——`Instant` 测试不可伪造，`TimeMs` 可任意构造；impl 唯一时钟源是 Tick
   - **HTTP**：`RoomDeps.api: Arc<dyn ApiClient>` 经构造器注入，契约测试注入 fake（规则 10/15）
   - **随机**：`RoomDeps.rng: Arc<dyn RandomSource>` 注入，房主选择可测（规则 5）
7. **运行时**：tokio `current_thread` 即可（actor 全异步、无阻塞调用；HTTP 用非阻塞客户端——**禁止 ureq-2 类阻塞客户端进 async 上下文**，评审 §6）
8. **配置更新走薄缝**（评审 §8）：`config.rs` 监听配置文件，变更后派发 `RoomCommand::UpdateConfig { config: Arc<RoomConfig> }` 给所有房间；impl 在下次 `handle` 时应用（如 monitor 白名单，规则 4）。**配置不是构造期快照**——`RoomsV1::new(config)` 之后配置仍可变
9. **房间生命周期与队列压力（评审 §8）**：创建有 `factory.create`（出生证明），销毁走 `RoomEvent::RoomClosed`（死亡证明）——actor 判定空房（规则 6）时返回该事件，core 排空 channel、drop sender、拆任务、清理路由表。**channel 有界（如 1024），满时按消息类处理**：
   - **热路径可丢**：`Touches`/`Judges` **满则丢新**（发送端 `try_send` 拒绝整个新包，评审 §8 六：mpsc 不支持生产者侧驱逐队首；丢新与"同通道保序"相容——队内顺序不变，触摸流每帧独立、下一帧自愈；与 §10.4 monitor 环形缓冲的丢旧保新同哲学、不同实现。若不满足可自建队列，v2 再议）
   - **Tick 可丢**：自带 `now`，下一拍自愈
   - **生命周期事实不可丢**：bus 等待
   - **其它客户端命令**：丢弃并断连（滥用防护）
   **滥用控制优先用每连接限速**（热路径 ~60-70Hz 上限），不让队列压力触发断连。**分发并发边界**：bus 对每个房间的投递独立进行（不跨房间串行 `await`）——一个房间拥塞不得拖延其它房间的 Tick/生命周期事实。**RoomClosed 排空（评审 §8 二-4）**：core 见 RoomClosed 即停路由——新命令回 room-closed 错误；排空期间到达的命令一律 `Failure(Business)`。禁止"加入成功然后房间被拆"的状态

**两个 10s 的归属**（随评审修订）：
- 断线判定 10s（心跳）→ core session 任务
- 重连窗口 10s → core **用户生命周期任务**（与连接事实同源，单一生产者）；impl 只处理驱逐后果，不再自己计时
- 玩法倒计时（打歌）→ impl，Tick 驱动（真正属于游戏语义的部分）

---

## 5. 如何从一开始就约束好

> 约束不能靠自觉，要靠**机器**：编译期、CI、测试三层卡死，文档层供人理解。

### 5.1 第 1 层：编译期（结构层）

Rust 的 **crate 边界就是依赖防火墙**——`phira-api` 的 Cargo.toml 没写 `phira-core` 依赖，`use phira_core::...` 就编译不过，**物理上不可能违反**。

```toml
# 根 Cargo.toml
[workspace.lints.rust]
unsafe_code = "forbid"                 # 全 workspace 禁 unsafe

[workspace.lints.clippy]
all = { level = "warn", priority = -1 }
```

```toml
# crates/phira-api/Cargo.toml —— 契约 crate 单独加严
[lints.rust]
missing_docs = "deny"                  # 契约必须文档化
```

### 5.2 第 2 层：CI 依赖方向检查（机器检查）

lints 管不了"impl 偷偷依赖 core"（能编译过），靠脚本卡。CI 里跑 `cargo metadata` 读真实依赖图，与白名单比对：

```python
# tools/check-deps.py（伪代码）
ALLOW = {
    "phira-api":       [],                    # 不依赖任何内部 crate
    "phira-core":      ["phira-api"],
    "phira-contract":  ["phira-api"],         # 契约测试套件库（只依赖 api）
    "impl-rooms-v1":   ["phira-api"],         # + dev: [phira-contract]（接入测试）
    "impl-mod-memory": ["phira-api"],         # 阶段 4 再开（crate 尚未创建，创建时启用本行，评审 §8 六）
    "phira-server":    ["phira-api", "phira-core", "impl-rooms-v1", "impl-mod-memory"],
}
# 遍历所有 crate 的依赖边，不在 ALLOW 中 → 退出码 1
# 评审 §8：必须区分 normal/dev 依赖边——dev 边仅允许 impl → phira-contract → api 这条链，
# 否则要么 CI 误红（假边）、要么 dev 边完全漏检（真违规）
```

任何人试图让 `impl-rooms-v1` 依赖 `phira-core`，PR 直接红。**这是"一开始就约束好"的物理保证。** 新增 crate 时，先更新本表再合并（否则 CI 卡死）。

### 5.3 第 3 层：契约测试（行为层）

"可换性"的终极约束：**任何 impl 必须通过同一套契约测试**。测试写成泛型 trait 测试，每个 impl 只传构造器：

```rust
// crates/phira-contract/src/rooms.rs —— 契约测试套件（泛型，只依赖 api）
// 评审 §4：不能放 workspace 根 tests/（virtual manifest 下不属于任何 package，cargo test --workspace 不会执行）
pub async fn room_contract_suite<F: RoomFactory>(factory: &F) {
    // deps 由工厂持有（构造时注入 fake，§4.9-6；评审 §8 五-11：create 不再收第二份）
    let mut room = factory.create(RoomId::new("test"));
    // 建房 → 选图 → 请求开始 → Ready → 开打 → 上报成绩 → 结算，全流程断言
    // 边界用例（见 §6 状态机规范）：8 人上限、非 host 越权、断线重连（窗口内重连保留座位 / 窗口外踢人 / Playing 中断线即 abort / 重连恢复房间状态）、锁房、观战权限……
    // 时间用 TimeMs 伪造：Tick{now} 任意构造，10s 窗口可精确推进（评审 §5）
}

// crates/impl-rooms-v1/tests/contract.rs
#[tokio::test]
async fn rooms_v1_passes() { room_contract_suite(&RoomsV1::new(cfg, fake_deps())).await; }

// 未来 impl-rooms-v2/tests/contract.rs —— 就一行，过了才能合并
#[tokio::test]
async fn rooms_v2_passes() { room_contract_suite(&RoomsV2::new(cfg, fake_deps())).await; }
```

**V2 想上线？先过同一套契约测试。** 这是换实现的安全网。

### 5.4 第 4 层：CI 工作流 + 文档（流程层）

```yaml
# .github/workflows/ci.yml —— 五道闸门
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: dtolnay/rust-toolchain@stable   # + rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --all -- --check                     # 1. 格式
      - run: cargo clippy --workspace --all-targets -- -D warnings  # 2. lint
      - run: python tools/check-deps.py                     # 3. 依赖方向
      - run: cargo test --workspace --all-targets           # 4. 测试（含契约测试）
      - run: cargo deny check                               # 5. 第三方依赖许可
```

文档层：`docs/architecture.md` 画依赖图，`docs/adr/` 记录每个架构决策的"为什么"。**文档给人看，前三层给机器看。**

### 5.5 Day 1 清单（约束必须与代码同一天出生）

1. workspace 骨架 + `rust-toolchain.toml` + 全局 lints（`forbid(unsafe_code)`）
2. `phira-api`：`RoomCommand / CmdCtx / RoomEvent / RoomFactory / RoomActor / RoomDeps / AuthHandler` 类型 + 薄缝（**并发模型先定案，见 §4.9**）
3. `phira-core`：bus 路由 + session 桩 + 配置
4. `impl-rooms-v1`：照原版语义实现，**同时写契约测试**（套件在 `phira-contract`，评审 §4）
5. `phira-server`：main.rs 接线（组合根）
6. `tools/check-deps.py` + CI 五道闸门——**第 6 步和第 1 步一样紧急**

> 架构约束晚一天出生，就是欠一天的债。

### 5.6 契约演进规则（api 不是写死的，是版本化的）

契约会变（客户端协议演进、玩法扩展），但**必须有规则地变**。**两层兼容性必须分开判断**（评审 §6）：

| 变更 | 协议兼容 | Rust API 兼容（下游 match/semver） | 处理 |
|---|---|---|---|
| 追加协议命令变体（ClientCommand 等，tag 后追加） | ✅ | 内部 workspace，breaking 无碍 | 版本字节不变，新旧客户端并存 |
| 追加内部契约变体（RoomCommand/RoomEvent） | 协议无关 | ✅（api crate 枚举加 **`#[non_exhaustive]`**，下游必须留通配分支） | 加变体 + 契约测试补用例 |
| 给 `RoomActor` 追加方法 | — | ❌（无默认实现即 breaking） | 走 ADR + api 主版本；能进设计期的（如 deadline 唤醒）就设计期做 |
| 语义微调（错误文案） | ✅ | ✅ | 文档 + ADR 记录 |
| 依赖新增（impl 需要新能力） | ✅ 若不动薄缝 | ❌ 若动薄缝签名 | 契约 crate 先定类型，再谈实现 |
| 破坏性变更（改字段/删变体/改帧格式） | ❌ | ❌ | 协议版本字节升级 + api 主版本 + 新旧入口并存（§3.2） |

`#[non_exhaustive]` 的代价：impl 的 match 必须写通配分支——把"变体未穷尽"当作常态，这正是版本演进的正确姿态。

配套工具：`cargo-semver-checks` 在 CI 里盯 api 主版本；`docs/adr/` 记录每次破坏性变更的动机。

---

## 6. 房间状态机与协议语义（契约的内容，契约测试的依据）

> 本节是"契约在测什么"的权威规范，直接来源于原版 phira-mp（`command.rs` / `room.rs` / `session.rs` / `bin.rs`）。契约测试的每个断言都可回溯到本节某条规则。

### 6.1 传输层

| 项 | 规范 |
|---|---|
| 协议 | 纯 TCP，明文（无 TLS，公网部署见 §11） |
| 握手 | 客户端先发 1 字节版本号（当前 v1），服务端读取 |
| 帧格式 | `ULEB128 长度 + 载荷`，载荷以 `u8` 命令 tag 开头 |
| 包上限 | 协议上限 2 MiB（服务端可配置更紧，默认 ~1MiB）；长度字段超过 32 bit 拒绝（防攻击）；**鉴权前帧上限 ~4KiB**（§10.4，评审 §7） |
| 心跳 | **客户端**每 3s 发 `Ping`、2s 未收到 `Pong` 计 1 次失败；**服务端不发 `Ping`**（ServerCommand 只有 `Pong`，评审 §7），以 10s 无任何包判定断线 |

### 6.2 序列化（BinaryData）

- 整数小端；`bool` = 1 字节；字符串/数组/容器长度一律 ULEB128
- 坐标 `CompactPos` = **f16 半精度 ×2**（x, y）——不是 f32，写错即不兼容；使用 `half` crate 的 `half::f16`（纯数学转换、no_std、零依赖，见 §4.8）
- 长度受限字符串（Varchar）：token ≤32、聊天 ≤200、RoomId ≤20
- `Option` = bool + 值；`Result` = bool + Ok/Err；`HashMap` = 数量 + 键值对
- RoomId 合法字符：`[A-Za-z0-9_-]` 且非空

### 6.3 命令枚举（tag 顺序即协议，不能乱）

**ClientCommand**：Ping · Authenticate{token} · Chat{message} · Touches{frames} · Judges{judges} · CreateRoom{id} · JoinRoom{id, monitor} · LeaveRoom · LockRoom{lock} · CycleRoom{cycle} · SelectChart{id} · RequestStart · Ready · CancelReady · Played{id} · Abort

**ServerCommand**：Pong · Authenticate(Result) · Chat(Result) · Touches{player, frames} · Judges{player, judges} · Message · ChangeState · ChangeHost(bool) · OnJoinRoom · CreateRoom/JoinRoom/LeaveRoom/LockRoom/CycleRoom/SelectChart/RequestStart/Ready/CancelReady/Played/Abort(各 Result)

**Message（房间广播）**：Chat · CreateRoom · JoinRoom · LeaveRoom · NewHost · SelectChart · GameStart · Ready · CancelReady · CancelGame · StartPlaying · Played{score, accuracy, full_combo} · GameEnd · Abort · LockRoom · CycleRoom

### 6.4 房间状态机

```
SelectChart(可选谱面) ──host RequestStart(须已选图)──► WaitForReady
WaitForReady ──全员 Ready──► Playing
Playing ──全员 Played/Abort──► SelectChart（cycle 则顺延换房主）
```

### 6.5 规则清单（契约测试逐条断言）

**房间与权限**
1. 房间容量：玩家上限 **8 人**；monitor（观战者）不占名额、不限数量
2. 仅 host 可：锁房、循环房、选图、请求开始（`CheckHost`）
3. 锁房不可加入；仅 `SelectChart` 状态可加入
4. monitor 权限 = 用户 id 在 `server_config.yml` 白名单；monitor 加入 → 房间 `live=true`
5. 房主**离开或被驱逐**（掉线超时 / 主动离开，措辞与规则 21 统一）→ 随机指定新 host（**RNG 经 `RoomDeps` 注入，契约测试可测**）→ 广播 `NewHost` + `ChangeHost`
6. 空房（所有人离开）→ actor 返回 `RoomEvent::RoomClosed` → core 排空 channel、拆任务（§4.9-9，评审 §8）

**游戏流程**
7. `RequestStart` 前必须已选谱面；进入 WaitForReady 时 host 默认已 ready
8. 全员（玩家+monitor）ready → `StartPlaying` → Playing
9. host `CancelReady` → `CancelGame` + 回 SelectChart；非 host → 仅 `CancelReady`
10. `Played`：回源官方 API 校验成绩记录（**仅阻塞该房间 actor，见 §4.9-2**），且 `record.player == 用户 id`；重复上报 → 错误
11. 全员完成/abort → `GameEnd` → 回 SelectChart；`cycle=true` 时房主顺延给下一位
12. **断线重连总览**：Playing 中断线 → 判定断线后立即 abort；非 Playing 断线 → 10s 重连窗口（dangle），超时踢人（细节见规则 19-23）

**鉴权与会话**
13. 鉴权前收到任何非 `Ping`/`Authenticate` 包 → 忽略
14. 鉴权：回源 `GET {API}/me`（Bearer token）→ `{id, name, language}`；同 id 重连复用用户对象
15. 选图/成绩校验：`GET {API}/chart/{id}`、`GET {API}/record/{id}`

**观战转播（性能热点）**
16. live 模式下收到 `Touches`/`Judges` → **只转发给 monitor**（不广播给玩家）
17. 转发实现必须是"序列化一次 + 共享缓冲"（零拷贝），禁止逐接收者克隆；共享缓冲用 `bytes::Bytes`（O(1) 切片，见 §4.8）；**慢消费策略：monitor 队列满则丢最旧帧（丢旧保新），绝不阻塞房间、绝不无限积压**（§10.4，评审 §7）；**热路径机制（方案 A：结构化转发，编解码归 core）**：core 解码一次（校验）→ 命令侧 `Touches{frames}`/`Judges{judges}`（§4.4）→ actor 查 live、计算 `targets = Specific(monitor_ids)` → 返回结构化事件 `RelayTouches`/`RelayJudges`（§4.4）→ **core 用它的编码器把 ServerCommand 编码一次**为 `Bytes` → 共享给所有 monitor。总编解码：**每命令 1 解 + 1 编，每接收者 0 次**；impl 永不碰协议编码（§4.3-3 成立，评审 §8 一-1）

**时间驱动逻辑（§4.6）**
18. 掉线超时/倒计时等时间逻辑由柜台 `Tick`/`UserDisconnected` 驱动；**impl 内禁止后台任务直接广播**

**断线与重连（规则 19-23）**
19. **身份与重连**：用户身份 = token 解析出的 user id；同 id 再次鉴权 = 重连 → 复用用户对象、**替换会话（epoch+1，关闭旧 TCP、取消旧会话任务）**（core 职责，§4.9-3）
20. **断线判定**：心跳 10s 无包（§6.1）→ core **用户生命周期任务**派发 `UserDisconnected{user, epoch}`
21. **重连窗口**：非 Playing 断线 → 10s 内重连则保留座位（`UserReconnected{user, epoch}`）；10s 到期 → **先查权威会话状态再派发** `UserDangleExpired`（防 9.999s 重连排在 10.000s 定时器后，§4.9-3）→ impl 移除座位、广播 `LeaveRoom`、若为房主则迁移房主（规则 5）
22. **Playing 中断线**：无重连窗口，判定断线后立即 abort（移除 + 广播）
23. **重连恢复**：重连成功的鉴权响应必须携带当前房间状态（`ClientRoomState`）——core 通过 `RoomCommand::GetClientState` 查询 impl（§4.4）；**旧连接（epoch 不匹配）到达的命令一律拒绝**
24. **两个 10s 的区别**：心跳断线判定（最后包后 ~10s）与重连窗口（断线后 10s）独立计时，最坏约 20s 完成踢人
25. **可测性**：impl 唯一时钟源是 `Tick { now: TimeMs }`（u64 毫秒，测试可伪造）；HTTP/随机数经 `RoomDeps` 注入（§4.9-6）——没有这三件事，规则 21/22 的 10s 窗口和规则 10 的回源校验根本无法写契约测试断言
26. **广播范围**：所有 Message 变体均为**房内广播**（用户+monitor；已核实原版 `broadcast` 仅遍历房内 `users()+monitors()`，协议无全服广播，评审 §8 一）——`Targets::All` 的语义即此
27. **重复入房**：已在房间中再次 `JoinRoom`/`CreateRoom` → 错误（`already in room`，原版 `user.room` 判重；契约测试需要此用例）

### 6.6 转换层映射表（产出规则，评审 §8 二：契约测试断言的直接依据）

**表 1：命令 → 事件产出（成功路径）**

| 命令 | 产出事件 |
|---|---|
| CreateRoom | `[RoomCreated{host}]` |
| JoinRoom | `[UserJoined{user}]` |
| LeaveRoom | `[UserLeft{user}]`；房主 → 追加 `[NewHost{new,old}]` |
| Chat | `[Chat]` |
| SelectChart | `[SelectChart]` |
| RequestStart | `[GameStart]`（→ ChangeState(WaitingForReady)，表 2） |
| Ready | `[Ready]`；全员 → 追加 `[StartPlaying]`（→ ChangeState(Playing)） |
| CancelReady | 非房主 `[CancelReady]`；房主 `[CancelGame]`（→ ChangeState(SelectChart)） |
| Played | `[Played]`；全员完成 → 追加 `[GameEnd]`（→ ChangeState(SelectChart)）；cycle → 追加 `[NewHost]` |
| Abort | `[Abort]`；全员 → 追加 `[GameEnd]`（同上） |
| LockRoom / CycleRoom | `[LockRoom]` / `[CycleRoom]` |
| Touches / Judges | `[RelayTouches]` / `[RelayJudges]`（live 时） |
| UserDangleExpired（系统） | `[UserLeft]`；房主 → 追加 `[NewHost]` |
| UserDisconnected / UserReconnected（系统） | 无事件（缺席标记 / 恢复） |

**表 2：事件 → ServerCommand（转换层；含非 Message 变体——不是纯机械，评审 §8 二-2）**

| 事件 | 产出 ServerCommand（投递目标） |
|---|---|
| Chat | Message(Chat) → All |
| RoomCreated | Message(CreateRoom) → All |
| UserJoined | OnJoinRoom(user) → All + Message(JoinRoom) → All |
| UserLeft | Message(LeaveRoom) → All |
| NewHost | Message(NewHost) → All + ChangeHost(true) → Specific(new) + ChangeHost(false) → Specific(old) |
| SelectChart | Message(SelectChart) → All + **ChangeState**(SelectChart(Some(id))) → All |
| GameStart | Message(GameStart) → All + **ChangeState**(WaitingForReady) → All |
| Ready | Message(Ready) → All |
| CancelReady | Message(CancelReady) → All |
| CancelGame | Message(CancelGame) → All + **ChangeState**(SelectChart) → All |
| StartPlaying | Message(StartPlaying) → All + **ChangeState**(Playing) → All |
| Played | Message(Played) → All |
| GameEnd | Message(GameEnd) → All + **ChangeState**(SelectChart) → All |
| Abort | Message(Abort) → All |
| LockRoom / CycleRoom | Message(LockRoom) / Message(CycleRoom) → All |
| RelayTouches / RelayJudges | Touches(player,frames) / Judges(player,judges) → targets |
| RoomClosed | 无协议输出（core 内部） |

注：`ChangeState`/`OnJoinRoom`/`ChangeHost` 由状态转换/入房/房主迁移触发，**不是 Message 的一一对应**——§14 阶段 2 说的"机械映射"仅覆盖 Message 部分；表 2 整体进 Oracle 核实清单（尤其 ChangeHost 的双向性与 NewHost 事件携带 old_host 的约定）。

---

## 7. 模块清单（货物盘点）

### 7.1 分类

| 类别 | 角色 | 同时活跃数 | 模块 |
|---|---|---|---|
| **Handler**（命令持有者） | 被路由，处理命令返回响应；每房间一个 actor 实例（§4.9） | 每房 1 个，可整体替换 | 房间管理（rooms） |
| **认证服务**（AuthHandler） | token → 身份解析（纯查询，不涉房间/会话） | 1 个，可整体替换 | 鉴权（auth，§4/§6.5-19） |
| **Observer/Interceptor**（观察者+拦截者） | 订阅事件；可否决命令 | 可多个，按序执行 | 封禁/审核（mod）、聊天过滤、反作弊、Web 面板 |
| **观测**（计数器，非中间件） | 每命令类型 成功/失败/延迟（§3.2） | 1 组原子计数器 | Metrics（内嵌总线，§11.1 健康检查共用） |
| **叶子**（核心部件，不可换） | 无状态纯函数 | 1 | 协议编解码（codec） |
| **核心**（柜台，不可换） | 会话、总线、**定时器**、**路由表（user→room_id 元数据）**、**用户生命周期**、配置 | 1 | phira-core |

### 7.2 判定规则（新模块放哪）

一个模块该做成什么，问三个问题：

1. **存在多个合理解释吗？** 有 → 可换实现（Handler/Observer）；没有（如编解码）→ 叶子，不做接口
2. **契约稳定可描述吗？** 是 → 定义类型进 api；否 → 还没到抽象时机
3. **它有独立状态吗？** 有 → 独立 impl crate；没有 → 考虑并入 core 或做成纯函数

### 7.3 封禁模块的接口形态（Observer 示例）

```rust
//（Moderator 待定：封禁拦鉴权的通路在阶段 4 前不存在——鉴权不经 RoomCommand 流，intercept 看不到 Authenticate；
//  路径标注阶段 4 再定，评审 §8 二-5）
#[async_trait]   // §4.7 规则：对象安全
pub trait Moderator: Send + Sync {
    /// 命令处理前拦截：返回 Err 则拒绝该命令
    async fn intercept(&self, cmd: &RoomCommand, ctx: &CmdCtx) -> Result<(), RoomError>;  // 与 RoomActor 共用 RoomError 两分类（评审 §8）
    /// 事件广播时被通知（只收领域事件，不收 RelayTouches/Judges，§4.4 分类）
    async fn on_event(&self, ev: &RoomEvent);
}
```

`CmdCtx` 与 `RoomActor::handle` 共用同一类型（§4.4）——评审指出的上下文不一致由此消除。**`intercept`/`on_event` 双能力为初步形态**：第一个观察者（阶段 4）动工时再定形，现在不承诺它是最终接口（原则 5）。**ModMemory 配置热更：v1 不做**（封禁名单重启生效）；需要时给 `Moderator` 加 `UpdateConfig` 同款命令（原则 5 推迟，评审 §8）

封禁不是命令持有者——它订阅领域事件（如 Chat）并在命令路径上否决，不碰其它货物。**拦鉴权的通路在阶段 4 前不存在**（鉴权不经 RoomCommand 流，`intercept` 看不到 Authenticate——`AuthAttempt` 是幽灵类型，已删，评审 §8 二-5），届时再设计。

---

## 8. 数据流示例（一次开房的完整旅程）

```
客户端 A                    柜台（core）                   房间 actor（RoomsV1 实例）
   │   CreateRoom{id}          │                              │
   ├──────────────────────────►│  factory.create(id) ────────►│ 新建 actor + channel
   │                           │  1. Moderator::intercept?    │（阶段 4 后）
   │                           │  2. enqueue(CmdCtx{Client}) ──►│ 串行进入（§4.9）
   │                           │◄──────────────── (Ok, [RoomCreated{host}]) │
   │                           │  3. 先解析事件 targets（All 反解），再应用路由增量(RoomCreated→host→id)，
   │                           │     再发送响应（§4.9-4 不变量）
   │◄───── CreateRoom(Ok) ─────┤                              │
   │                           │  4. 按事件 targets 投递（领域事件→成员+观察者）
   │                           ├──────────► 封禁货物（听）
   │                           ├──────────► 面板/Bot（听）
   │                           │
```

要点：**步骤 1/3/4 是柜台的事，actor 只做步骤 2**——且 actor 是每房间一个实例、命令串行进入（§4.9），事件自带 `room_id + targets`，柜台不需要知道房间成员。**路由增量来自事件的封闭集**（UserJoined 增 / UserLeft 删 / RoomClosed 删房间，§4.4 分类）——不是 core 在命令时直接登记（评审 §8：与 §4.9-4 对齐）。换实现 = 换工厂，其余不变。

---

## 9. 测试策略（四层测试，各有分工）

| 层 | 测什么 | 手段 |
|---|---|---|
| **单元** | 编解码 roundtrip、RoomId 校验、状态机纯函数 | 普通 `#[test]` |
| **模糊** | 解码器吃随机/畸形字节不 panic、不超限 | `proptest` / `arbitrary`（CI 固定种子跑） |
| **契约** | 任何 impl 的完整行为（见 §5.3、§6.5 清单） | 泛型契约套件，每个 impl 复用 |
| **Oracle** | 与原版 phira-mp 的**字节级一致性** | golden files：抓原版字节流，断言本实现解码/编码逐字节一致；或双实现互连互通测试。**录制方法学（评审 §8）**：原版回源是外部单点——录制时把原版连到**本地 mock API**（/me、/chart、/record 响应预先录好回放），或直接抓"真客户端 ↔ 原版服务器"字节流存 golden files；录制环境必须可控，禁止录制中途依赖线上官方 API |

**契约测试的可行性由 §4.9-6 保证**：`TimeMs` 时钟可伪造、`ApiClient`/`RandomSource` 注入 fake。否则规则 21/22 的 10s 窗口（`Instant` 无法伪造）和规则 10 的回源校验（HTTP 无法 mock）根本写不了断言——薄缝形状必须支撑它的测试承诺（评审 §5）。

外加一次性的**集成验证**：真 Phira 客户端连本地服务器，完整开一局。

**Oracle 测试是本项目最重要的正确性保险**——协议逆向最容易"以为懂了"，字节级对比能直接拆穿任何自以为是。

---

## 10. 性能预算与测量（内存是硬指标，必须可量化）

### 10.1 RSS 预算（几百连接稳态）

| 项 | 预算 |
|---|---|
| 二进制基底 + 代码段常驻 | ~2-4 MB |
| 每连接（读写缓冲 + task + 会话结构） | ~2-4 KB × N |
| tokio worker（1-2 线程） | ~1-2 MB |
| HTTP 客户端（异步 + rustls 单栈） | ~2-3 MB（https 回源躲不掉；若官方 API 允许明文 HTTP 可省 ~1.5MB，但大概率不允许，按 https 计，评审 §6）；**估算待实测校准（附录 D-P1）** |
| 房间/用户表（每房 8 人规模） | ~1-2 MB |
| **合计** | **~7-15 MB** |

### 10.2 测量方法

- 稳态 RSS/PSS：`/usr/bin/time -v`、`cgroup memory.max` 压力测试（连续开房/开打 24h）
- 关注 **PSS**（按共享页分摊），多实例共存场景下比 RSS 更真实
- 每次大 PR 附 RSS 对比（CI 可加一个 `memory-check` job）

### 10.3 红线（CI 或评审强制）

- 新增依赖需评审：reqwest 级重型 HTTP 栈属重；**rustls 单栈可接受**（TLS 不可避免，评审 §6）；新增阻塞客户端必须进 `spawn_blocking` 且限量
- 禁止"悄悄把内存吃回去"——`phira-api` 零 tokio 是第一条红线

### 10.4 内存攻击面（评审 §7）

| 攻击面 | 对策 |
|---|---|
| **大帧并发** | 协议 2MiB 是上限不是常态：服务端配置更紧帧上限（默认 ~1MiB）；**每连接解码缓冲记账** + 全局在途字节上限（如 64MiB），超限即断连 |
| **鉴权前大帧** | **鉴权前帧上限收紧到 ~4KiB**（握手 + token ≤32B 之外无合法大帧）——直接堵死"未鉴权 2MiB 帧"攻击 |
| **慢消费者（观战）** | live 路径（Touches/Judges→monitor）用**丢旧保新**策略：每 monitor 有界环形缓冲，满则丢最旧帧，**绝不阻塞房间 actor、绝不无限积压**（评审 §7）；房间命令队列：有界（1024），满时按消息类处理——**热路径满则丢新、Tick 可丢、生命周期事实等待、其它客户端命令丢弃断连**（§4.9-9，评审 §8 五-2） |
| **半开连接** | 握手超时（peek 等首字节 ≤5s）+ 鉴权超时（≤10s）+ 未鉴权连接数上限 + 每 IP 限额（§11） |

---

## 11. 安全与运维考量

| 面 | 措施 |
|---|---|
| 协议健壮性 | 包上限 2MiB、ULEB128 ≤32bit、RoomId 白名单字符、token ≤32 字符（§6） |
| 日志泄露 | **token 绝不落日志**（原版 debug 日志打印过 token，本项目禁止） |
| 会话劫持 | 明文 + token 即身份：token 泄露 → 同 id 重连直接顶替原会话（规则 19）。**协议层无法根治**，缓解：token 不落日志、短有效期、异常顶替告警（Observer）；记为已知风险（评审 §8） |
| 传输安全 | 明文 TCP 是协议特性；TLS 前置仅适用于自建端/受信网络——**落地前需验证 Phira 客户端是否支持 TLS（大概率不支持，与 P1 真客户端直连目标冲突，评审 §8）** |
| 鉴权前包 | 未鉴权时忽略非 Ping/Auth 包（§6.5-13） |
| 滥用防护（可选观察者） | 登录失败限速、聊天频率限制——做成 Observer，不塞进核心 |
| 连接准入 | 总连接数上限、**未鉴权连接数上限**（全局小额度）、每 IP 限额、握手/鉴权超时（评审 §7） |
| 内存 DoS | 帧大小分级上限（鉴权前 ~4KiB）+ 每连接记账 + 全局在途字节上限（§10.4） |
| 供应链 | `cargo deny` 许可审查 + 依赖白名单；impl 是编译进二进制的**受信代码**（无沙箱需求，信任模型简单） |
| 优雅停机 | SIGTERM → 停止接受新连接 → **向所有房间广播"服务器维护中"** → 宽限窗口（如 10s，供玩家看到消息）→ 强制退出。**无持久化下不存在"排空"语义**——停机即丢房，降低损失靠广播消息 + 快速重启（评审 §8） |

### 11.1 测活设计（Liveness / Health Check）

**背景**：原版 phira-mp 没有内置测活接口，社区被迫用 Cloudflare Worker + 硬编码账号密码 + 完整鉴权握手来测活（Pimeng 的测活脚本），存在三个问题：

1. 测"活"却要登录——需要账号、依赖官方登录 API
2. 官方 API 挂了 → 所有服务器被判死（假阴性）
3. 测的是"鉴权通不通"，不是"服务健不健康"

**决策**：内置测活，且**不依赖任何 impl、不依赖 Phira API**。测活是柜台的前门（core 的一部分），不是可换货物。

**三层方案（按推荐顺序）**：

| 方案 | 服务器改动 | 用途 |
|---|---|---|
| A. 协议内 Ping 测活 | **0 行**（鉴权前处理 Ping→Pong 是项目固定行为，与 §6.5-13 同源） | 社区/脚本测活的官方手段；复用 phira-mp-client，~30 行即可探测 |
| B. 同端口 HTTP 嗅探 `/healthz` | ~50-100 行（config 开关） | K8s probe / 监控大盘 |
| C. 独立健康端口 | ~20 行 | 最简外部监控 |

**方案 B 设计（推荐实现）**：

- 端口复用：accept 后 `socket.peek(&mut buf).await` 窥探前几个字节（peek 不消费数据）——首字节 `0x01` 走 MP 协议；`b"GET "` / `b"HEAD"` 走 HTTP 分支，**完全不污染核心 MP 状态机**
- 健壮性：peek 可能一次读不满 4 字节，循环等到 ≥1 字节再判断（MP 首字节 `0x01` vs HTTP 首字节是 ASCII 字母，实际只需 1 字节即可区分）；**peek 等待带超时（≤5s）**，防半开连接无限挂起（评审 §7）
- 返回 JSON：`{"status":"ok","version":"0.1.0","uptime_s":1234,"connections":57,"rooms":12}`
- 数据源来自 Metrics（§3.2），深度健康信息白送：**测活（liveness）+ 测健康（readiness）一步到位**
- 安全：不返回用户名/房间内容等敏感信息；公开部署建议限速或仅内网可访问

**验收标准**：无 token、无官方 API 依赖，3s 内判定存活；官方 API 挂掉不影响测活结果。

---

## 12. 风险与缓解

| 风险 | 缓解 |
|---|---|
| **过早抽象**：接口猜错第二个实现 | 薄缝原则 + 抽象时机原则；接口只长在类型上（§2.3） |
| **房间状态机语义微妙**（dangle/重连/房主迁移）写错 | §6.5 规则清单逐条契约测试 + Oracle 字节级对照（§9） |
| **内存悄悄涨回去**（新依赖） | §10.3 红线 + CI memory-check |
| **协议版本漂移**（新客户端协议升级） | 版本字节 + 契约演进规则（§5.6）+ 新旧入口并存 |
| **灰度翻车**（切了进行中的房间） | 入口粒度天然满足"同房间同入口"（§3.2，评审 §8） |
| **结算期观战冻结**（Played HTTP 串行阻塞该房） | v1 接受（短且稀有）；不可接受则拆两段副作用（§4.9-2，评审 §8） |
| **并发模型错误**（全局锁队头阻塞 / 系统命令乱序踢人） | 每房间 actor 串行 + 用户生命周期单一生产者（§4.9） |
| **契约测试写不出来**（时钟/HTTP/随机不可伪造） | `TimeMs` + `RoomDeps` 注入（§4.9-6、规则 25） |
| **阻塞客户端冻结全服**（ureq-2 进 async） | 只允许异步客户端；阻塞实现必须 `spawn_blocking` 限量（§10.3，评审 §6） |
| **内存 DoS**（大帧/慢消费者/半开连接） | 帧大小分级 + 每连接记账 + 丢旧保新 + 连接准入（§10.4/§11，评审 §7） |
| **过度设计**：插件框架、HMR 等面子工程 | 非目标清单（§1.3）+ 模块判定规则（§7.2） |
| **契约测试与实现同源**（测了等于没测） | Oracle 测试用**原版**做基准，不与本实现同源 |
| **官方 API 单点**（挂 → 鉴权/选图/成绩全断 → 全服不可用） | 鉴权结果短期缓存（TTL）+ 显式降级模式（拒绝新连接 / 缓存白名单放行）；`ApiClient` 是可注入 trait——**降级 = 换一个缓存/离线实现**，架构白送（评审 §8） |
| **会话劫持**（token 泄露即顶替） | §11 会话劫持行；协议层无法根治，记为已知风险（评审 §8） |
| **热路径双次编解码** | 已解决：结构化转发 + core 单次编码（方案 A，§6.5-17）——每命令 1 解 1 编，每接收者 0 次（评审 §8 一-1） |

---

## 13. 常见误解（FAQ）

**Q1：第一天就要把模块全部抽象？**
不。第一天只把**类型**放对位置 + 依赖方向正确；接口等第二个实现出现（§2.3-5）。

**Q2：没有接口，模块不会耦合吗？**
耦合 = 依赖方向，不是接口数量。组合根（只有 main.rs 认识所有人）+ 薄缝 = 零耦合（§2.3-3/4）。

**Q3：为什么用 Rust 不用 Go/C#？**
内存 KPI：Rust ≈ Go 的 1/3、C# 的 1/5 常驻内存；抽象机制（trait/async-trait）的代价可忽略（§4.7），与"可换"需求天然契合（§3.3）。

**Q4：为什么不做热替换（HMR）？**
服务端子系统替换不需要运行时热卸载——重启即换，成本最低（§1.3）。

**Q5：广播和点对点不是一回事吗？**
不是。命令 = 调用（点对点、带返回路径、单一持有者）；事件 = 通知（扇出、无返回、多订阅者）。两种通道都在柜台（§2.4、§8）。

**Q6：封禁模块怎么接入？**
Observer/Interceptor：订阅事件 + 在命令路径上否决，不碰其它货物（§7.3）。

**Q7：impl 能认识 core 吗？**
不能。事件通过薄缝返回值带出，impl 永远不需要 import core（§4.3-3）。

**Q8：换实现真的只要一行代码？**
概念演示是——`let rooms = RoomsV2::new(...)`；**真实路径**：新 impl crate + 契约测试通过 + Oracle 对照（§5.3、§9）+ 部署级灰度上线（§3.2）。"一行"是宣传债，已收敛为演示性表述。

**Q9：测活需要登录吗？**
不需要。协议内发 `Ping` 在鉴权前就返回 `Pong`（§11.1 方案 A，免费）；或启用同端口 `/healthz`（方案 B）。两者都不依赖 Phira 官方 API。

**Q10：一个房间的 `Played` 卡在 HTTP 回源，会卡死整个服务器吗？**
不会。每房间一个 actor，HTTP 只阻塞该房间（§4.9-2）。**禁止**用全局锁串起所有房间。

**Q11：impl 里能开后台定时器吗？**
不能。时间与连接事实全部走薄缝命令（`Tick`/`UserDisconnected`/`UserDangleExpired`），impl 内禁止后台任务直接改共享状态（§4.6）。

---

## 14. 落地路线图

| 阶段 | 内容 | 验收标准 |
|---|---|---|
| 阶段 1（2-3 周） | 协议层：编解码、帧、心跳。**可复用原版 phira-mp-common（Apache-2.0）**（Oracle 录制用本地 mock API，§9） | Oracle 测试：与原版字节流逐字节一致 |
| 阶段 2（1-2 周） | core 骨架：会话 + 总线 + 薄缝 + impl-rooms-v1 + **协议↔内部契约转换层**（映射表见 §6.6：ClientCommand→RoomCommand、RoomEvent→ServerCommand，含非 Message 的 ChangeState/OnJoinRoom/ChangeHost——不是纯机械，每次协议变更联动 api/转换/契约测试三处，评审 §8 二-2） | 真客户端开房联机全流程走通 |
| 阶段 3（1-2 周） | 契约测试补全（§6.5 清单）+ 边界用例 + 换实现验收流程（契约 + Oracle + 真客户端实测）+ 测活（A 免费，B 可选） | 契约测试全绿；错误率可观测；无 token 3s 内测活通过 |
| 阶段 4（按需） | 第一个观察者（封禁内存版）、轻量 HTTP 客户端、内存调优 | RSS 达标（~7-15MB，§10） |
| 阶段 5（未来） | 第二个实现出现时，契约成型，验证"整体替换"承诺 | 换 impl = 新 impl crate + 契约测试通过 + 组合根换工厂（"一行代码"仅为演示性表述，§3.1/Q8） |

---

## 附录 A：ADR 记录（真实决策已抽取为独立文件，评审 §8 六）

已提取的决策（见 `docs/adr/`）：

| 编号 | 决策 | 章节 |
|---|---|---|
| 0001 | 并发模型：每房间一个 actor | §4.9 |
| 0002 | 事件寻址：事件自带路由，core 不持影子状态 | §4.4/§4.9-5 |
| 0003 | 灰度降级：入口粒度，非需求 | §3.2 |
| 0004 | 结构化转发（方案 A）：编解码归 core | §6.5-17 |
| 0005 | 队列压力：按消息类分级 | §4.9-9 |
| 0006 | 会话纪元：重连编排输入侧闭环 | §4.9-3 |

新增决策的模板（复制后改编号与内容）：

```markdown
# ADR-0007：<决策标题>

- 日期：2026-XX-XX
- 状态：已接受
- 相关章节：

## 背景
<问题>

## 决策
<决策>

## 后果
- 正面：
- 负面：

## 替代方案
<被拒方案>——被拒：<理由>
```

---

## 附录 B：术语表

| 术语 | 含义 |
|---|---|
| 契约（api） | 两层：**协议层**（ClientCommand/ServerCommand/Message，协议直接投影）+ **内部契约层**（RoomCommand/RoomEvent，改写产物，按设计对待，§2.3 原则 1） |
| 薄缝（Seam） | 形状被类型钉死的最小接口 |
| 组合根 | 唯一认识所有模块的组装点（main.rs） |
| 货物 / 柜台 / 老板 | 商店比喻：impl / core / main.rs |
| Handler / Observer | 命令持有者（1 个）/ 事件订阅者+拦截者（多个） |
| 灰度 | 已降级为非需求（§1.2/§3.2）：未来运维选项 = **入口粒度**（新入口 + 引导新房主）；百分比级分流需协议加 redirect 命令 |
| 契约测试 | 对 trait 泛型编写、所有实现必须通过的测试套件 |
| Oracle 测试 | 与原版实现字节级一致性对照测试 |
| RSS / PSS | 常驻内存 / 按共享页分摊的常驻内存 |

---

## 附录 C：与相关工作的关系

- **原版 `phira-mp`（TeamFlos）**：协议语义的权威参考，Apache-2.0，协议层可复用
- **官方 Go 服务端 `phi-ch-server`**：完整后端，本项目不替代它，只做房间子系统
- **Cordis / 论文（cordiverse/paper）**：空间可组合性的形式化基础；本项目取其契约模型、舍其运行时热替换
- **社区多语言重写**（gooophira-mp、PhiraMpServerCSharp 等）：本项目不做语言之争，只聚焦"Rust + 最小内存 + 可换架构"

---

## 附录 D：编码前检查清单（按优先级）

> **上下文（单人实现者）**：本清单的读者是"三周后的自己"，机制按单人校准——**契约套件按主文 Day-1 建 `phira-contract`**（它是 P0 可替换性的验证机制，与主文一致，评审 §8 五-10）、ADR 仪式简化为决策日志、check-deps.py 保留（便宜，防未来的你）。**三个形状决定问题（§1.5）的截止点是 Day-1 清单第 2 条（薄缝 trait）落笔前**——过点即视为已定案，不允许再开；否则它们会被写代码时的默认值静默填上（评审 §8 总结）。
> **纪律（评审 §8 三）**：**§4.4 片段以 `phira-api` 首次编译通过为验收**——冻结后第一个 commit 就是把 §4.4 搬进真实 phira-api，`cargo check` 即 scratch crate，不为文档另建验证；它同时会让 §4.4 引用的未定义类型（Varchar/TouchFrame/RoomErrorCode…）现形。

### P0 编码前必须（已全部定案，见正文）

- [x] 并发模型：每房间 actor + 用户生命周期单一生产者（§4.9）
- [x] 事件寻址：事件自带 `room_id + targets`，路由表只存元数据（§4.4 / §4.9-5）
- [x] api 完整性：`CmdCtx` / `AuthHandler` / 路由规则 / `TimeMs` / `RoomDeps`（§4.4 / §4.9）
- [x] 依赖注入：时钟（`TimeMs`）/ HTTP（`ApiClient`）/ 随机（`RandomSource`），契约测试可 mock（§4.9-6）

### P1 编码前应核实（待办，未闭环）

- [ ] **ureq-3 async 可用性**：核实 async 支持存在且稳定；备选 = 最小化 hyper，或**手写 HTTP/1.1 GET**（回源只是带 Bearer 的 GET，约两百行，最贴合内存目标，评审 §8）
- [x] **原版 `Played` 是否持锁做 HTTP**：已读码核实（评审 §8 三）——**不持锁**（`get_room!` 克隆 Arc 后 guard 即释放、HTTP 在锁外、`room.state.write()` 在 HTTP 之后，各会话独立 task）；actor 串行是行为回退，§4.9-2 已按事实重写
- [ ] **ureq+TLS 真实内存实测**：§10.1 预算是估算——异步客户端 + rustls 单栈 ~2-3MB 待实测校准；先验证官方 API 是否允许明文 HTTP（评审 §6）
- [ ] **心跳语义对照原版逐条核对**：§6.1 视角已澄清，但超时边界值/重连窗口/断线判定的字节级行为需 Oracle 测试逐条验证（评审 §7）
- [ ] **TLS 前置可行性**：验证 Phira 客户端是否支持 TLS（大概率不支持，与 P1 直连冲突，§11）
- [ ] **目标客户端协议版本字节**：确认要支持的客户端实际发哪个版本（§6.1）
- [ ] **官方客户端是否支持 SRV**：两级寻址成立的前提——玩家用官方客户端，需核实其填裸域名时是否查 `_phira._tcp`（§3.5）

### P2 文档修正（已完成）

- [x] 示例代码可编译化：§4.5 `#[tokio::main]` / 依赖注入 / 无部分 move
- [x] 章节顺序 4.6→4.8→4.7 修复
- [x] 契约测试套件移至 `phira-contract` crate（§4.1 / §5.3）
- [x] 契约演进表拆"协议兼容 × Rust API 兼容"两层（§5.6）
