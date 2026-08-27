# 管理 API 设计（优雅管理面的定稿，为面板铺路）

> 2026-08 定稿。定位：**管理 API = 组合根旁的无状态翻译层**——不是第二个内核，
> 不新建机制，全部落进项目已有的三条通道（组合根编排 + 系统命令族 + 独立端口）。
> 参照：gooophira 管理全家桶（唯一成熟参照），但刻意避开其"单体里直接锁全局状态、
> 两级锁死锁风险自述、OTP 双步过重"的路径。

## 0. 设计总原则（"优雅"从哪来）

1. **管理动作翻译成系统命令，不直接改状态**——踢人进生命周期事实通道、封禁进
   Moderator 名单、广播进既有 Message 广播。管理 API 不认识 impl、不持有状态、
   不碰连接内部结构。鲁棒性 = 与房间 actor 通过串行队列排队（**通道防竞态，
   不用锁**——gooophira 用锁防的，我们用通道天然防）。
2. **只读与写分离、可独立上线**——阶段 1 只有只读面（零写风险），写完面再上。
3. **管理通道永不阻塞游戏通道**——独立端口（`http_port` 已存在）+ 独立任务 +
   try 语义；管理 API 挂掉不影响 MP 入口（端口隔离，ISSUE-0005 已立的先例）。
4. **失败面收敛**——所有写接口返回结构化 `{ok | error{code,msg}}`；管理动作的
   后果最高到"踢人/禁言/广播"，碰不到房间状态机本身（防线 = 系统命令语义）。
5. **快照读，不传播状态**——读路径全部走既有快照（RoomListSink/Metrics），
   管理查询不会拉长房间 actor 的串行位。

## 1. 三个职责域 × 三条既有通道

| 职责域 | 能力 | 通道 |
|---|---|---|
| **观测**（只读） | 房间/用户/对局/metrics/审计 | 组合根直读 bus 快照 + RoomListSink + SessionSink（`/rooms` `/healthz` 的扩展） |
| **干预**（写） | 踢人/移房/封禁/广播/解散 | **系统命令族**（`AdminKick`/`AdminBan`/`AdminBroadcast` → `RoomCommand` 变体，AGENTS.md"碰 4 处"流程） |
| **配置**（热更） | runtime-config + rollback + observer 增删 | 复用 `Bus::update_config` 广播 + 全量快照机制 |

## 2. 传输与支撑层（守住内存红线）

- **零重型依赖**：不引 axum/hyper——手写 HTTP/1.1 升级成 mini-router（路径匹配 +
  serde_json，均为现库存量）。管理 API 低频，够用且守住 7-15MB 预算。
- **认证**（阶段 2）：静态 Bearer token（配置项 `admin_token`），对齐现有鉴权
  Bearer 模式；不做 OTP 双步（自建服过重，gooophira 教训）。
- **暴露边界**：默认绑定 loopback（`admin_host` 可配）；公网暴露靠认证 + 每 IP 限速。
- **不做 WebSocket**（v1）：面板轮询即可（`?since=` 增量）；console 日志流是
  gooophira 的复杂度来源，省掉。

## 3. 健壮性四件套（写面必备，阶段 2 兑现）

1. **审计日志**：每个写操作落结构化审计事件（谁/何时/动作/目标/结果）——面板审计页
   与纠纷回溯的地基；
2. **干预走既有安全语义**：AdminBan = 更新 Moderator 名单（触发既有 intercept），
   不是旁路拔线；误操作最高到"踢人/禁言"；
3. **配置回滚**：runtime-config 每次写入保留上一份全量快照，
   `/admin/config/rollback` 一步回切（gooophira rollback 概念，v1 做"上一份"；
   不搞版本栈）；
4. **幂等 + 限速**：系统命令语义自带幂等（重复 AdminKick 缺席用户 → NotInRoom，
   无害）；管理入口限速防御暴力。

## 4. 端点规范（当前实现对照）

| 端点 | 阶段 | 状态 | 说明 |
|---|---|---|---|
| `GET /` | 0 | ✅ | 端点清单 |
| `GET /rooms` | 0 | ✅ | 公开房间列表（隐私过滤，`hidden_prefixes`） |
| `GET /healthz` | 0 | ✅ | 测活 + Metrics 暴露（B3） |
| `GET /admin/rooms?state=` | 1 | ✅ 本轮 | 房间列表 + 状态过滤（playing/waiting/selectchart；GET 值不区分大小写，含子串匹配；不传 = 全部） |
| `GET /admin/rooms/{id}` | 1 | ✅ 本轮 | 单房详情（RoomInfo + cycle）；不存在 → 404 |
| `GET /admin/users` | 1 | ✅ 本轮 | 在线用户（id + name + room_id）；name 缺失（未注册）→ null |
| `GET /admin/metrics` | 1 | ✅ 本轮 | bus Metrics 快照（与 /healthz.metrics 同构） |
| `POST /admin/rooms/{id}/kick` | 2 | ✅ 本轮 | 系统命令 `AdminKick`（复用 evict；不断 TCP）+ 审计 |
| `POST /admin/rooms/{id}/broadcast` | 2 | ✅ 本轮 | 系统命令 `AdminBroadcast`（房内系统 Chat user=0）+ 审计 |
| `POST /admin/users/{id}/ban` | 2 | ✅ 本轮 | kick（若有房）+ 断 TCP（kicker force_close）+ 审计；**名单拦截依赖 P2** |
| `POST /admin/users/{id}/disconnect` | 2 | ✅ 本轮 | 仅断连（连接收尾发生命周期事实）+ 审计 |
| `GET /admin/audit` | 2 | ✅ 本轮 | 审计环（有界 256，时间倒序） |
| `POST /admin/config` | 3 | ✅ 本轮 | runtime-config 热更（先存"上一份"→广播）+ 审计 |
| `POST /admin/config/rollback` | 3 | ✅ 本轮 | 一步回切上一份（取走即清空，二次回滚 409）+ 审计 |
| `POST /admin/observers {kind,op}` | 3 | ✅ 本轮 | observer 热插拔：`kind:"ban"`（BanObserver）add/remove 幂等；其它 kind 400 + 审计 |

**阶段 1 已知限制（诚实记录）**：
- `/admin/rooms/{id}` 详情目前 = RoomInfo（id/host/users/state/locked/cycle）；
  **成员名单、谱面难度（level）** 列阶段 2 数据源扩展（RoomListSink 记录成员）；
- `/admin/users` 暂不含 lang/队列状态（SessionSink 未暴露），阶段 2 补；
- 状态过滤的匹配对象是 RoomListSink 的状态字符串（`SelectChart(1)` 等），
  子串语义已够面板用。

## 5. 演进路线（每阶段独立上线、可验收）

| 阶段 | 内容 | 验收 | 顺手清偿的债 |
|---|---|---|---|
| 0 | 设计定稿（本文档） | — | — |
| 1 ✅ | 只读管理面（/admin/rooms、rooms/{id}、users、metrics） | 零写风险、原 /rooms /healthz 回归不动 | **C1 拆分触发**（http_serve/http_accept_loop 从 server.rs → admin.rs） |
| 2 ✅ | 写面系统命令族（AdminKick/AdminBroadcast）+ Bearer 认证 + 审计环 | kick e2e 全链路（含被踢者本人收 LeaveRoom）+ 401/403 + 审计 4 端点 | `/admin/*` 全认证（读面收紧）；AdminBan 名单拦截留 P2 |
| 3 ✅ | runtime-config（存"上一份"+一步回滚）+ observer 热插拔（ban） | config 回滚 409/审计 + 热插拔生效失效/幂等 + 名单拦命令 | **§7.3 定形实锤**：Moderator 加 `kind()`（type_name_of_val 对 &dyn 返回 dyn 名不可作身份键） |
| 3.5 ✅ | 管理面持久化（ban / audit / config 快照） | 重启后名单拦截生效、审计历史可查、rollback 跨重启仍可用；损坏文件 fail soft | 组合根 storage 模块，契约/core 零改动 |
| 3.6 ✅ | 反作弊 P2（AntiCheatObserver，第二个真实 Moderator） | 跨房 record 重放被 Moderated 拒绝（端到端）+ /admin/anticheat 读面 + 热插拔 kind=anticheat | §7.3 观察者接口被第二个实例再次定形（intercept 同步判定 + 拒绝环形） |
| 4 | Web 面板 | 消费已稳定 API，不改服务端 | 反作弊 P2 的运营观察台顺手长在面板上 |

## 5.5 持久化（组合根 storage 模块，阶段 3.5 落地）

**只持久化管理事实**（决策与记录），不持久化状态（房间/会话内存态模型不变，关服清空是特性）。
实现全部落在 `phira-server/src/storage.rs`（组合根独占）：phira-api/core/impl/契约零感知，
无新 `RoomCommand`、无新 `Moderator`、零新依赖（std::fs + serde_json）。

文件布局（`persist_dir` 配置项 / `R0SEMI_MP_PERSIST_DIR` 环境变量，默认 `./data`）：

```text
data/
├── audit.jsonl          # 审计归档（追加；AuditEntry 一行一条）——启动回填至多 256 行进内存环
├── bans.json            # 封禁名单（全量原子写 tmp+rename）——启动加载，重启拦截生效
├── config.current.json  # 生效配置原文（POST /admin/config 请求体 rooms 子对象）
└── config.last.json     # 上一份（rollback 源）——启动回填 AdminConfigState，重启后仍可回滚
```

语义：
- **触发点全在既有写路径同步执行**（管理低频 <1ms，无后台任务/定时器，§4.6 事实命令化不违背）；
- **fail soft**：写失败仅日志、内存态继续；读损坏回退空态/默认（bans 损坏 = 按空名单启动并告警——名单只是反作弊工具）；
- **原子性**：config/bans 走 tmp+rename，半写文件永不落地；audit 追加天然可截断恢复（`fsync` 每行）；
- **安全**：文件不含 admin_token（审计只记 action/target/result）；production 建议目录权限 600；
- config 落盘存**请求体原文 JSON**（`{"rooms":{...}}` 的 rooms 子对象）而非反序列化结构——
  `monitors` 之外的未来字段演进自动跟随，不依赖新增 serde derive。
- **SQLite 门**：现不引。出现"面板过滤/搜索审计、多实例共享名单"需求才评估（那才是 C 库成本换查询收益的时刻）；
  单实例文件层与 SQLite 语义同构（都是事实持久化），换时只动 storage.rs。

## 6. 决策点结论（2026-08 拍板）

1. **认证**：静态 Bearer token（配置项），默认 loopback 绑定 —— 已定；
2. **管理命令族**：结构化变体（`AdminKick` 等逐条变体，契约测试逐条钉），
   拒绝通用透传 —— 已定；
3. **面板**：纯 API 消费方，前端不进仓库（gooophira 是前者，代价是 React 进仓库，
   本项目不做）—— 已定；
4. **阶段 1 动工**：已开启（本轮）。

## 7. 关联

- C1（server.rs 上帝文件 1594 行）：**本轮已触发**——admin.rs 抽出为第一步；
- gooophira 对照：controller/OTP/console/rollback 概念的取舍记录（server-comparison §3.2）；
- 反作弊 P2：运营观察台 = 阶段 4 面板的天然宿主，不单独做。