//! 命令路由 + 事件广播 + 房间生命周期（§4.9 并发模型的落地）。
//!
//! 结构（§4.9）：
//! - 路由表：`user_id → room_id` 元数据（不复制任何房间状态，§4.9-4）
//! - 每房间一个有界 mpsc channel + 每房间一个 actor 任务（§4.9-1，命令串行进入）
//! - 事件自带 targets，core 只执行投递（§4.9-5）
//!
//! 时序不变量（§4.9-4）：同一处理步骤内 **先解析事件 targets → 再应用路由增量 → 再发响应**。
//! - "先解析后应用"：离开者仍收到自己的 LeaveRoom
//! - "先应用后响应"：流水线客户端 `JoinRoom → SelectChart` 不会收到"你不在房间里"
//!
//! 队列压力分级（§4.9-9）：热路径/Tick 可丢、生命周期事实等待、其它命令满则断连。

use std::{
    collections::HashMap,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use phira_api::{
    ApiError, CmdCtx, Moderator, Origin, RoomCommand, RoomConfig, RoomError, RoomErrorCode,
    RoomEvent, RoomFactory, RoomId, RoomResponse, Targets,
};
use tokio::sync::{RwLock, mpsc, oneshot};

/// 房间命令队列容量（§4.9-9：有界 1024）。
const ROOM_CHANNEL_CAPACITY: usize = 1024;

/// A2 回源有界重试（§4.9-2）：总尝试 = 1 + `PLAYED_FETCH_RETRIES` 次，间隔
/// `PLAYED_FETCH_RETRY_DELAY`。回源（phira.5wyxi.com）是全局唯一上游——瞬时
/// 5s 超时/网络抖动直接判死会让玩家成绩静默丢失、房间卡 Playing；重试在 actor
/// 外执行，不占房间串行位。重试仍失败才回注 `Err`，由 impl 结算为"无有效成绩"。
const PLAYED_FETCH_RETRIES: usize = 2;
const PLAYED_FETCH_RETRY_DELAY: Duration = Duration::from_millis(500);

/// 单条命令的投递载荷。
struct Envelope {
    ctx: CmdCtx,
    cmd: RoomCommand,
    respond: Option<oneshot::Sender<RoomResponse>>,
}

/// 房间句柄：channel sender（§4.7：core 只持有 sender，actor 在任务手上）。
struct RoomHandle {
    tx: mpsc::Sender<Envelope>,
}

/// 事件投递目标（§4.9-5 / §6.6 表 2）。
///
/// core 只执行投递；`RoomEvent → ServerCommand` 的编码归转换层（阶段 2 实现，§14 阶段 2）。
#[async_trait::async_trait]
pub trait EventSink: Send + Sync {
    /// 向指定用户投递一个事件。
    async fn deliver(&self, user_id: i32, event: &RoomEvent);
}

/// 命令处理统计（§3.2：错误率只统计 Internal）。
#[derive(Debug, Clone, Copy, Default)]
pub struct CommandStats {
    /// 调用次数。
    pub calls: u64,
    /// 成功次数。
    pub ok: u64,
    /// 业务拒绝次数（预期行为，不计错误率）。
    pub business: u64,
    /// 内部故障次数。
    pub internal: u64,
    /// 平均延迟（毫秒）。
    pub avg_latency_ms: f64,
}

/// 总线内嵌计数器（§3.2 / §11.1 健康检查共用）。
///
/// 原子计数器集合（评审 §8：不是中间件）；错误率只统计 `Internal`，
/// 业务拒绝（房满/越权）是预期行为，混入会扭曲对比。
///
/// 锁优化（2026-08，performance-cpu.md §锁竞争矩阵）：热路径命令（Touches/Judges）
/// 从明细表豁免——触摸流 24k cmd/s 全量打 `Mutex<HashMap>` = 每命令一把锁
/// （实测指标锁占 CPU ~3.1%）；触摸流无错误语义、f64 moving-avg 无运营价值，
/// 计数保留在**单个原子**（吞吐观测仍可得），明细留在慢路径（低频无争）。
#[derive(Default)]
pub struct Metrics {
    /// 热路径命令（touches/judges）原子计数（Relaxed——单一计数无顺序需求）。
    hot: AtomicU64,
    /// 慢路径明细（其余命令，低频）。
    inner: std::sync::Mutex<HashMap<&'static str, CommandStats>>,
}

impl Metrics {
    fn record(
        &self,
        name: &'static str,
        result: &Result<RoomResponse, RoomError>,
        elapsed: Duration,
    ) {
        // 热路径豁免：触摸流/判定流只做原子计数（无锁、无明细、无 f64）
        if name == "touches" || name == "judges" {
            self.hot.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // 慢路径（其余命令，低频）：明细照旧
        // 中毒恢复（柜台不 panic）：持锁线程若 panic，取回 guard 继续（计数可能丢失，可接受）
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let stats = guard.entry(name).or_default();
        stats.calls += 1;
        stats.avg_latency_ms = moving_avg(stats.avg_latency_ms, stats.calls, elapsed);
        match result {
            Ok(RoomResponse::Failure(RoomError::Business { .. }))
            | Err(RoomError::Business { .. }) => stats.business += 1,
            Ok(RoomResponse::Failure(RoomError::Internal { .. }))
            | Err(RoomError::Internal { .. }) => stats.internal += 1,
            _ => stats.ok += 1,
        }
    }

    /// 每命令类型快照（§11.1 健康检查数据源）。
    pub fn snapshot(&self) -> Vec<(&'static str, CommandStats)> {
        let guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut v: Vec<_> = guard.iter().map(|(k, s)| (*k, *s)).collect();
        drop(guard);
        // 热路径合成条目（count 保留，明细全零——触摸流不计错误率/延迟，§3.2 语义不变）
        let hot = self.hot.load(Ordering::Relaxed);
        if hot > 0 {
            v.push((
                "touches.judges.hot",
                CommandStats {
                    calls: hot,
                    ..CommandStats::default()
                },
            ));
        }
        v.sort_by_key(|(k, _)| *k);
        v
    }

    /// 内部错误总数（§3.2：错误率统计基数）。
    pub fn internal_errors(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .map(|s| s.internal)
            .sum()
    }
}

/// 统计场景的移动平均（calls 达 2^53 前 u64→f64 精度无影响——Metrics 不是协议精度，§3.2 错误率用）。
#[allow(clippy::cast_precision_loss)]
fn moving_avg(prev: f64, calls: u64, elapsed: Duration) -> f64 {
    (prev * (calls - 1) as f64 + elapsed.as_secs_f64() * 1000.0) / calls as f64
}

struct BusInner {
    factory: Arc<dyn RoomFactory>,
    /// 房间表：room_id → channel sender。
    rooms: RwLock<HashMap<RoomId, RoomHandle>>,
    /// 路由表：user_id → room_id 元数据（§4.9-4，只存 id）。
    routes: RwLock<HashMap<i32, RoomId>>,
    /// 投递目标（阶段 2 由转换层实现）。
    sink: std::sync::Mutex<Option<Arc<dyn EventSink>>>,
    metrics: Metrics,
    /// 当前生效的房间配置（§4.9-8）。
    config: RwLock<Arc<RoomConfig>>,
    /// 回源客户端（A2，§4.9-2 规则 2）：Played 的房外 HTTP 校验任务用；
    /// 未注入 = 纯 actor 环境如部分测试，Played 受理后无回注。
    /// OnceLock：组合根在 `Bus::new` 后 `with_api` 注入一次（运行期只读）。
    api: std::sync::OnceLock<Arc<dyn phira_api::ApiClient>>,
    /// 观察者/拦截者（§7.3）：订阅领域事件 + 客户端命令路径否决。
    /// v1 构造期注入（`Bus::with_moderators`）；空 = 零开销短路。
    /// std Mutex（临界区极短，无 await——与 `sink` 同款纪律）；
    /// 运行期热插拔（管理 API 加/移除观察者）留到管理面动工时再做。
    moderators: std::sync::Mutex<Vec<Arc<dyn phira_api::Moderator>>>,
}

/// 柜台（§2.4）：命令路由 + 事件广播 + 房间生命周期。
#[derive(Clone)]
pub struct Bus {
    inner: Arc<BusInner>,
}

impl Bus {
    /// 创建柜台。`factory` 由组合根注入并持有 deps（§4.9-6）。
    ///
    /// A2：默认无回源客户端（`api=None`）——Played 受理后不会发起房外校验；
    /// 生产接线用 [`Bus::with_api`] 注入。
    pub fn new(factory: Arc<dyn RoomFactory>, config: Arc<RoomConfig>) -> Self {
        Self {
            inner: Arc::new(BusInner {
                factory,
                rooms: RwLock::default(),
                routes: RwLock::default(),
                sink: std::sync::Mutex::new(None),
                metrics: Metrics::default(),
                config: RwLock::new(config),
                api: std::sync::OnceLock::new(),
                moderators: std::sync::Mutex::new(Vec::new()),
            }),
        }
    }

    /// 注入回源客户端（A2，§4.9-2）：启用 Played 的房外两段式校验。
    ///
    /// 必须在 spawn 任何命令派发前调用（组合根接线期）；重复注入以首次为准。
    #[must_use]
    pub fn with_api(self, api: Arc<dyn phira_api::ApiClient>) -> Self {
        let _ = self.inner.api.set(api);
        self
    }

    /// 注入观察者/拦截者（§7.3）：客户端命令路径否决 + 领域事件订阅。
    ///
    /// 必须在 spawn 任何命令派发前调用（组合根接线期）；空列表 = 现状零开销。
    /// 运行期热插拔留给管理 API（阶段 3，`add_moderator`/`remove_moderator`）。
    #[must_use]
    pub fn with_moderators(self, moderators: Vec<Arc<dyn Moderator>>) -> Self {
        *self
            .inner
            .moderators
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = moderators;
        self
    }

    /// 运行期挂载观察者（阶段 3，docs/admin-api.md §4：observer 热插拔，管理 API 用）。
    ///
    /// 幂等：同 `kind()` 已存在则不重复追加（同名观察者视为同一策略）。
    pub fn add_moderator(&self, moderator: Arc<dyn Moderator>) {
        let kind = moderator.kind();
        let mut list = self
            .inner
            .moderators
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !list.iter().any(|m| m.kind() == kind) {
            list.push(moderator);
        }
    }

    /// 运行期卸载观察者（按 `kind()` 匹配；移除 ≥1 返回 true）。
    pub fn remove_moderator(&self, kind: &str) -> bool {
        let mut list = self
            .inner
            .moderators
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = list.len();
        list.retain(|m| m.kind() != kind);
        list.len() != before
    }

    /// 挂接事件投递目标（阶段 2：转换层 + session 写路径）。
    pub fn attach_sink(&self, sink: Arc<dyn EventSink>) {
        *self
            .inner
            .sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sink);
    }

    /// 计数器访问（§3.2 / §11.1）。
    #[must_use]
    pub fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }

    /// 当前生效的房间配置（§4.9-8）。
    pub async fn room_config(&self) -> Arc<RoomConfig> {
        Arc::clone(&*self.inner.config.read().await)
    }

    /// 路由表查询：用户当前所在房间（§4.9-4）。
    ///
    /// 生命周期任务用：断线/重连/窗口到期派发前查目标房间（§4.9-3）。
    pub async fn room_of(&self, user_id: i32) -> Option<RoomId> {
        self.inner.routes.read().await.get(&user_id).cloned()
    }

    /// 全部活跃房间（去重）。周期心跳广播 `Tick` 用（§4.6 单一生产者 = 生命周期任务）。
    ///
    /// 空”幽灵路由”（用户已离开但表未清的项去重后无房间名册可列）不会出现：
    /// 路由表以 in-room 用户为源，值域即房间集合。
    pub async fn active_rooms(&self) -> Vec<RoomId> {
        let routes = self.inner.routes.read().await;
        routes
            .values()
            .cloned()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// 派发客户端命令（session 收包解码后调用）。
    ///
    /// # Errors
    ///
    /// 路由层：`NotInRoom`（表 miss）/ `AlreadyInRoom`（重复入房，§6.5-27）/
    /// `RoomIdOccupied` / `RoomNotFound`；队列满拒收（§4.9-9）/ 房间关闭时 `Internal`。
    pub async fn dispatch(&self, ctx: CmdCtx, cmd: RoomCommand) -> Result<RoomResponse, RoomError> {
        let name = command_name(&cmd);
        let started = Instant::now();
        // §7.3：观察者拦截——仅客户端命令且**非热路径**（Touches/Judges 是 DropIfFull
        // 转发指令，观察者面不覆盖：慢观察者不得拖垮 60Hz 热路径，与 on_event 过滤
        // Relay* 同分类，§4.4/§4.9-9）；系统命令（生命周期/回注/配置）不可被拦。
        // 拦截点在路由之前：拒收的命令不产生任何房间副作用。
        let interceptable = matches!(ctx.origin, Origin::Client { .. })
            && !matches!(
                cmd,
                RoomCommand::Touches { .. } | RoomCommand::Judges { .. }
            );
        let result = if interceptable {
            match self.intercept_observers(&ctx, &cmd).await {
                Ok(()) => self.route(ctx, cmd).await,
                Err(e) => Err(e),
            }
        } else {
            self.route(ctx, cmd).await
        };
        self.inner.metrics.record(name, &result, started.elapsed());
        result
    }

    /// 观察者拦截（§7.3）：按注入顺序串行调用各 `Moderator::intercept`，任一拒绝即拒绝。
    /// 先克隆 Arc 列表再执行——不在 RwLock 读锁上跨 await（观察者实现应快速返回）。
    async fn intercept_observers(&self, ctx: &CmdCtx, cmd: &RoomCommand) -> Result<(), RoomError> {
        let moderators = self
            .inner
            .moderators
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for m in moderators {
            m.intercept(cmd, ctx).await?;
        }
        Ok(())
    }

    /// 派发系统命令（用户生命周期任务 / 定时器用，§4.6）。
    ///
    /// `room_id` 由调用方（生命周期任务查表后）填好；`GetClientState` 会返回响应（§4.4）。
    ///
    /// # Errors
    ///
    /// 房间不存在 / 关闭时 `Internal`。
    pub async fn dispatch_system(
        &self,
        room_id: RoomId,
        cmd: RoomCommand,
    ) -> Result<RoomResponse, RoomError> {
        let ctx = CmdCtx {
            origin: Origin::System,
            room_id,
        };
        self.dispatch(ctx, cmd).await
    }

    /// 配置热更广播（§4.9-8）：给所有房间派发 `UpdateConfig`。
    ///
    /// 配置不是构造期快照——`RoomsV1::new(config)` 之后配置仍可变。
    pub async fn update_config(&self, config: Arc<RoomConfig>) {
        broadcast_config(&self.inner, config).await;
    }

    /// 当前生效配置（管理面 rollback 快照源，docs/admin-api.md §3-3）。
    #[must_use]
    pub async fn current_config(&self) -> Arc<RoomConfig> {
        Arc::clone(&*self.inner.config.read().await)
    }

    /// 配置文件轮询监听（§4.9-8）：周期检查 `server_config.yml`（`R0SEMI_MP_CONFIG` 可改路径），
    /// 内容变化 → 重新解析 → `update_config` 广播给所有房间。
    ///
    /// 后台任务常驻（spawn 后立即返回）；文件不存在 / 解析失败仅 warn 并保留旧配置
    /// （运行时配置损坏不致命，启动时 `Config::load` 才是硬校验）。
    pub fn watch_config(&self, path: std::path::PathBuf, interval: Duration) {
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            // 直接驱动 BusInner（watch 是系统内务，不走 dispatch 路由）
            let bus_inner = inner;
            let mut last: Option<Arc<RoomConfig>> = None;
            loop {
                match tokio::fs::read_to_string(&path).await {
                    Ok(text) => {
                        let mut cfg = crate::config::Config::default();
                        match cfg.apply_yaml(&text, Some(&path.display().to_string())) {
                            Ok(()) => {
                                let rooms = Arc::new(cfg.rooms);
                                let changed = last.as_ref().is_none_or(|prev| *prev != rooms);
                                if changed {
                                    tracing::info!(
                                        "config reloaded from {}: {:?}",
                                        path.display(),
                                        rooms
                                    );
                                    broadcast_config(&bus_inner, Arc::clone(&rooms)).await;
                                    last = Some(rooms);
                                }
                            }
                            Err(e) => {
                                tracing::warn!("config reload from {} failed: {e}", path.display());
                            }
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        if last.is_none() {
                            tracing::warn!(
                                "config file {} not found; using defaults",
                                path.display()
                            );
                        }
                    }
                    Err(e) => tracing::warn!("config read {}: {e}", path.display()),
                }
                tokio::time::sleep(interval).await;
            }
        });
    }

    /// 路由解析 + 投递（§4.9-4 路由规则）。
    #[allow(clippy::too_many_lines)] // 路由规则完整呈现优于拆碎（§4.9-4 单一决策点）
    async fn route(&self, ctx: CmdCtx, cmd: RoomCommand) -> Result<RoomResponse, RoomError> {
        let needs_response = command_needs_response(&cmd);
        let policy = queue_policy(&cmd);
        // A2：Played 的回源载荷在 move 前提取（record_id），供房外回源任务使用
        let played_info = match &cmd {
            RoomCommand::Played { id } => Some(*id),
            _ => None,
        };

        // —— 路由解析（§4.9-4）：CreateRoom/JoinRoom 靠载荷 id；系统命令按 ctx.room_id；
        //    其余客户端命令靠路由表，表 miss → 回"不在房间" ——
        // 全局判重（§6.5-27）：入房类命令前查路由表，用户已在任意房间 → AlreadyInRoom
        if let Origin::Client { user_id } = ctx.origin
            && matches!(
                cmd,
                RoomCommand::CreateRoom { .. } | RoomCommand::JoinRoom { .. }
            )
            && self.inner.routes.read().await.contains_key(&user_id)
        {
            return Err(business(RoomErrorCode::AlreadyInRoom, "already in room"));
        }
        let (room_id, tx) = match &cmd {
            RoomCommand::CreateRoom { id, .. } => {
                // 新建房间：factory.create（出生证明）+ channel + 任务（§4.9-9）
                let mut rooms = self.inner.rooms.write().await;
                if rooms.contains_key(id) {
                    return Err(business(RoomErrorCode::RoomIdOccupied, "room id occupied"));
                }
                let actor = self.inner.factory.create(id.clone());
                let tx = spawn_room(self.inner.clone(), id.clone(), actor);
                rooms.insert(id.clone(), RoomHandle { tx: tx.clone() });
                (id.clone(), tx)
            }
            RoomCommand::JoinRoom { id, .. } => {
                let tx = self
                    .inner
                    .rooms
                    .read()
                    .await
                    .get(id)
                    .map(|h| h.tx.clone())
                    .ok_or_else(|| business(RoomErrorCode::RoomNotFound, "room not found"))?;
                (id.clone(), tx)
            }
            _ => match ctx.origin {
                Origin::System => {
                    let tx = self
                        .inner
                        .rooms
                        .read()
                        .await
                        .get(&ctx.room_id)
                        .map(|h| h.tx.clone())
                        .ok_or_else(|| internal("room not found"))?;
                    (ctx.room_id.clone(), tx)
                }
                Origin::Client { user_id } => {
                    let room_id = self
                        .inner
                        .routes
                        .read()
                        .await
                        .get(&user_id)
                        .cloned()
                        .ok_or_else(|| business(RoomErrorCode::NotInRoom, "not in room"))?;
                    let tx = self
                        .inner
                        .rooms
                        .read()
                        .await
                        .get(&room_id)
                        .map(|h| h.tx.clone())
                        .ok_or_else(|| internal("route table stale"))?;
                    (room_id, tx)
                }
            },
        };

        let (respond, rx) = if needs_response {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let env = Envelope {
            ctx: CmdCtx {
                origin: ctx.origin,
                room_id: room_id.clone(),
            },
            cmd,
            respond,
        };

        // —— 队列压力分级（§4.9-9）——
        match policy {
            QueuePolicy::DropIfFull => {
                // 热路径/Tick：满则丢新（队内顺序不变，触摸流每帧独立、下一帧自愈）
                let _ = tx.try_send(env);
            }
            QueuePolicy::Wait => {
                // 生命周期事实不可丢
                tx.send(env)
                    .await
                    .map_err(|_| internal("room closed while enqueueing"))?;
            }
            QueuePolicy::Reject => {
                // 其它客户端命令：满则断连（滥用防护，session 端处理）
                tx.try_send(env)
                    .map_err(|_| internal("room command queue full"))?;
            }
        }

        // A2 两段式（§4.9-2 规则 2）：Played 不再在 actor 内 await 回源——命令照常
        // 入队让 actor 做"受理"（幂等标记防重放），core 在房外发起 HTTP 回源任务
        // （不占房间串行位），完成后以 `RecordFetched` 系统命令回注房间应用。
        // 注意：回注**直接查表投递**而非再走 dispatch——后者经 route 形成类型级
        // 自递归（Future 大小不可计算）。运行期语义等价：系统 origin、无回话、
        // 房已关时发送失败仅记日志（Wait 语义下的可接受损失，对账由 inflight 兜底）。
        if let (Origin::Client { user_id }, Some(record_id)) = (&ctx.origin, played_info) {
            let api = self.inner.api.get().cloned();
            let inner = Arc::clone(&self.inner);
            let room_id = room_id.clone();
            let user_id = *user_id;
            tokio::spawn(async move {
                let Some(api) = api else {
                    tracing::warn!("no api injected; played record {record_id} unverifiable");
                    return;
                };
                // 有界重试（瞬时故障自愈）；仍失败则原样回注 Err，由 impl 兜底结算
                let mut record = Err(ApiError::Internal {
                    msg: "fetch not attempted".to_owned(),
                });
                for attempt in 0..=PLAYED_FETCH_RETRIES {
                    if attempt > 0 {
                        tokio::time::sleep(PLAYED_FETCH_RETRY_DELAY).await;
                    }
                    let started = Instant::now();
                    record = api.fetch_record(record_id).await;
                    inner.metrics.record(
                        "record_fetched",
                        &if record.is_ok() {
                            Ok(RoomResponse::Ok)
                        } else {
                            Err(internal("fetch failed"))
                        },
                        started.elapsed(),
                    );
                    if record.is_ok() {
                        break;
                    }
                    tracing::warn!(record_id, attempt, "record fetch attempt failed");
                }
                let env = Envelope {
                    ctx: CmdCtx {
                        origin: Origin::System,
                        room_id,
                    },
                    cmd: RoomCommand::RecordFetched {
                        user_id,
                        record_id,
                        record,
                    },
                    respond: None,
                };
                let sent = match inner.rooms.read().await.get(&env.ctx.room_id) {
                    Some(h) => h.tx.send(env).await.is_ok(),
                    None => false,
                };
                if !sent {
                    tracing::warn!(
                        user_id,
                        record_id,
                        "record_fetched delivery failed (room closed?)"
                    );
                }
            });
        }

        // 响应回传：非回注型命令等待 actor 回话（§4.4）；无回话命令按 Ok 归一
        match rx {
            Some(rx) => rx
                .await
                .map_err(|_| internal("room closed before response")),
            None => Ok(RoomResponse::Ok),
        }
    }
}

/// 队列压力策略（§4.9-9）。
enum QueuePolicy {
    /// 热路径/Tick：满则丢新。
    DropIfFull,
    /// 生命周期事实：不可丢，等待。
    Wait,
    /// 其它客户端命令：满则拒绝（断连，滥用防护）。
    Reject,
}

fn queue_policy(cmd: &RoomCommand) -> QueuePolicy {
    match cmd {
        RoomCommand::Touches { .. } | RoomCommand::Judges { .. } | RoomCommand::Tick { .. } => {
            QueuePolicy::DropIfFull
        }
        RoomCommand::UserDisconnected { .. }
        | RoomCommand::UserReconnected { .. }
        | RoomCommand::UserDangleExpired { .. }
        | RoomCommand::GetClientState { .. }
        | RoomCommand::RecordFetched { .. }
        | RoomCommand::AdminKick { .. }
        | RoomCommand::AdminBroadcast { .. } => QueuePolicy::Wait,
        // §5.6：新增命令默认按客户端命令处理（满则拒）
        _ => QueuePolicy::Reject,
    }
}

/// 哪些命令需要响应（§4.4：多数系统命令无回话；GetClientState 例外）。
fn command_needs_response(cmd: &RoomCommand) -> bool {
    match cmd {
        RoomCommand::CreateRoom { .. }
        | RoomCommand::JoinRoom { .. }
        | RoomCommand::LeaveRoom
        | RoomCommand::Chat { .. }
        | RoomCommand::SelectChart { .. }
        | RoomCommand::RequestStart
        | RoomCommand::Ready
        | RoomCommand::CancelReady
        | RoomCommand::Abort
        | RoomCommand::Played { .. }
        | RoomCommand::LockRoom { .. }
        | RoomCommand::CycleRoom { .. }
        | RoomCommand::GetClientState { .. }
        | RoomCommand::AdminKick { .. }
        | RoomCommand::AdminBroadcast { .. } => true,
        // RecordFetched 是回注型系统命令：结果经后续事件投递体现，本身无回话（§4.4）
        // §5.6：新增命令默认无回话，core 按 Ok 映射
        _ => false,
    }
}

/// 命令名（Metrics 键）。
fn command_name(cmd: &RoomCommand) -> &'static str {
    match cmd {
        RoomCommand::CreateRoom { .. } => "create_room",
        RoomCommand::JoinRoom { .. } => "join_room",
        RoomCommand::LeaveRoom => "leave_room",
        RoomCommand::Chat { .. } => "chat",
        RoomCommand::SelectChart { .. } => "select_chart",
        RoomCommand::RequestStart => "request_start",
        RoomCommand::Ready => "ready",
        RoomCommand::CancelReady => "cancel_ready",
        RoomCommand::Abort => "abort",
        RoomCommand::Played { .. } => "played",
        RoomCommand::LockRoom { .. } => "lock_room",
        RoomCommand::CycleRoom { .. } => "cycle_room",
        RoomCommand::Touches { .. } => "touches",
        RoomCommand::Judges { .. } => "judges",
        RoomCommand::Tick { .. } => "tick",
        RoomCommand::UserDisconnected { .. } => "user_disconnected",
        RoomCommand::UserReconnected { .. } => "user_reconnected",
        RoomCommand::UserDangleExpired { .. } => "user_dangle_expired",
        RoomCommand::GetClientState { .. } => "get_client_state",
        RoomCommand::RecordFetched { .. } => "record_fetched",
        RoomCommand::AdminKick { .. } => "admin_kick",
        RoomCommand::AdminBroadcast { .. } => "admin_broadcast",
        RoomCommand::UpdateConfig { .. } => "update_config",
        // §5.6：api 枚举 non_exhaustive，追加变体时必须留通配
        _ => "unknown",
    }
}

/// 启动房间 actor 任务（§4.9：每房间一个，命令串行）。
fn spawn_room(
    inner: Arc<BusInner>,
    room_id: RoomId,
    actor: Box<dyn phira_api::RoomActor>,
) -> mpsc::Sender<Envelope> {
    let (tx, rx) = mpsc::channel(ROOM_CHANNEL_CAPACITY);
    tokio::spawn(room_loop(inner, room_id, actor, rx));
    tx
}

/// 房间任务主循环：命令串行进入 actor，处理完统一走事件流程（§4.9）。
async fn room_loop(
    inner: Arc<BusInner>,
    room_id: RoomId,
    mut actor: Box<dyn phira_api::RoomActor>,
    mut rx: mpsc::Receiver<Envelope>,
) {
    while let Some(env) = rx.recv().await {
        let (resp, events) = actor.handle(env.ctx, env.cmd).await;
        let closed = process_events(&inner, &room_id, events, env.respond, resp).await;
        if closed {
            // 排空剩余（§4.9-9）：已入队命令回 Failure 而不是静默丢弃
            while let Ok(env) = rx.try_recv() {
                let (resp, events) = actor.handle(env.ctx, env.cmd).await;
                let _ = process_events(&inner, &room_id, events, env.respond, resp).await;
            }
            break;
        }
    }
    // 兜底清理（channel 断开 / RoomClosed 未覆盖路径）
    inner.rooms.write().await.remove(&room_id);
    inner.routes.write().await.retain(|_, rid| rid != &room_id);
}

/// 事件流程（§4.9-4 时序不变量）：解析 targets → 应用增量 → 响应 → 投递 → 观察者通知。
///
/// 返回该房间是否已关闭（RoomClosed）。
#[allow(clippy::too_many_lines)] // 五步时序完整呈现优于拆碎（§4.4/§4.9-4）
async fn process_events(
    inner: &Arc<BusInner>,
    room_id: &RoomId,
    events: Vec<RoomEvent>,
    respond: Option<oneshot::Sender<RoomResponse>>,
    resp: Option<RoomResponse>,
) -> bool {
    // 1. 解析投递目标（应用增量前——离开者仍被解析到，§4.9-4 先解析后应用）
    let mut deliveries: Vec<(i32, RoomEvent)> = Vec::new();
    {
        let routes = inner.routes.read().await;
        for ev in &events {
            match ev {
                RoomEvent::RoomClosed { .. } => {} // core 信号，不投递（§4.4）
                RoomEvent::RelayTouches { targets, .. }
                | RoomEvent::RelayJudges { targets, .. } => match targets {
                    Targets::Specific(ids) => {
                        deliveries.extend(ids.iter().map(|id| (*id, ev.clone())));
                    }
                    Targets::All => {
                        deliveries.extend(
                            routes
                                .iter()
                                .filter(|(_, rid)| **rid == *room_id)
                                .map(|(u, _)| (*u, ev.clone())),
                        );
                    }
                },
                _ => {
                    // 领域事件：恒 All（§4.4 分类）。
                    // 加入者/房主本人也在投递列表：其路由增量在本节之后才应用（§4.9-4
                    // 时序不变量“先解析后应用”只对“离开者仍被解析到”成立；加入者须手动补入）
                    deliveries.extend(
                        routes
                            .iter()
                            .filter(|(_, rid)| **rid == *room_id)
                            .map(|(u, _)| (*u, ev.clone())),
                    );
                    match ev {
                        RoomEvent::RoomCreated { host, .. } => {
                            deliveries.push((*host, ev.clone()));
                        }
                        RoomEvent::UserJoined { user, .. } => {
                            deliveries.push((user.id, ev.clone()));
                        }
                        _ => {}
                    }
                }
            }
        }
        // RoomClosed（core 信号）：无目标用户，不进 deliveries——步骤 4 单独通知观察者
        // （§4.4 修订：观察者依赖它清理快照；user_id=0 系统约定）。
    }

    // 2. 应用路由增量（事件封闭集：UserJoined 增 / UserLeft 删 / RoomClosed 删房间，§4.9-4）
    let mut closed = false;
    {
        let mut rooms = inner.rooms.write().await;
        let mut routes = inner.routes.write().await;
        for ev in &events {
            match ev {
                RoomEvent::RoomCreated { host, .. } => {
                    // 路由增量 host→room（§4.4）
                    routes.insert(*host, room_id.clone());
                }
                RoomEvent::UserJoined { user, .. } => {
                    routes.insert(user.id, room_id.clone());
                }
                RoomEvent::UserLeft { user, .. } => {
                    routes.remove(user);
                }
                RoomEvent::RoomClosed { .. } => {
                    closed = true;
                    rooms.remove(room_id);
                    routes.retain(|_, rid| rid != room_id);
                }
                _ => {}
            }
        }
    }

    // 3. 响应（先应用后响应——流水线 JoinRoom → SelectChart 不会"不在房间"）
    if let (Some(tx), Some(resp)) = (respond, resp) {
        let _ = tx.send(resp);
    }

    // 4. 投递（阶段 2 由 EventSink 实现编码）
    let sink = inner
        .sink
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    if let Some(sink) = sink {
        for (user_id, ev) in deliveries {
            sink.deliver(user_id, &ev).await;
        }
        // core 信号（RoomClosed）也通知观察者（§4.4 修订：RoomListSink 依赖它清理快照——
        // 修复前 RoomClosed 被拦在步骤 1，观察者永远收不到 → 空房残留列表）。
        // user_id=0 = 系统广播约定：转换层对 RoomClosed 无协议输出（§6.6 表 2），会话侧无害；
        // 观察者忽略 user_id 按事件清理。顺序：先 UserLeft（计数归零）后 RoomClosed（移除）。
        if closed {
            sink.deliver(
                0,
                &RoomEvent::RoomClosed {
                    room_id: room_id.clone(),
                },
            )
            .await;
        }
    }

    // 5. §7.3：领域事件通知观察者（尽力而为 fire-and-forget——不阻塞房间投递/串行位）。
    // 过滤：热路径 RelayTouches/Judges 与 core 信号 RoomClosed 不通知（§4.4 分类）；
    // 每个观察者每批一个 spawn（内部逐事件 await）。通知丢失可接受：观察者应幂等，
    // 权威判定走 intercept（同步路径），on_event 用于事后审计/统计。
    let moderators = inner
        .moderators
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    if !moderators.is_empty() {
        let domain: Vec<RoomEvent> = events
            .iter()
            .filter(|ev| {
                !matches!(
                    ev,
                    RoomEvent::RelayTouches { .. }
                        | RoomEvent::RelayJudges { .. }
                        | RoomEvent::RoomClosed { .. }
                )
            })
            .cloned()
            .collect();
        if !domain.is_empty() {
            for m in moderators {
                let domain = domain.clone();
                tokio::spawn(async move {
                    for ev in &domain {
                        m.on_event(ev).await;
                    }
                });
            }
        }
    }

    closed
}

fn business(code: RoomErrorCode, msg: &str) -> RoomError {
    RoomError::Business {
        code,
        msg: msg.to_owned(),
    }
}

fn internal(msg: &str) -> RoomError {
    RoomError::Internal {
        msg: msg.to_owned(),
    }
}

/// 配置热更广播（§4.9-8）：更新生效配置 + 给所有房间派发 `UpdateConfig`。
///
/// `update_config` 与 `watch_config` 共用（watch 直接驱动 `BusInner`）。
async fn broadcast_config(inner: &Arc<BusInner>, config: Arc<RoomConfig>) {
    *inner.config.write().await = Arc::clone(&config);
    let senders: Vec<(RoomId, mpsc::Sender<Envelope>)> = {
        let rooms = inner.rooms.read().await;
        rooms
            .iter()
            .map(|(rid, h)| (rid.clone(), h.tx.clone()))
            .collect()
    };
    for (room_id, tx) in senders {
        let env = Envelope {
            ctx: CmdCtx {
                origin: Origin::System,
                room_id,
            },
            cmd: RoomCommand::UpdateConfig {
                config: Arc::clone(&config),
            },
            respond: None,
        };
        let _ = tx.send(env).await;
    }
}
