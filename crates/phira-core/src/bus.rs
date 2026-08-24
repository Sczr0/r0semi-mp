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
    time::{Duration, Instant},
};

use phira_api::{
    CmdCtx, Origin, RoomCommand, RoomConfig, RoomError, RoomErrorCode, RoomEvent, RoomFactory,
    RoomId, RoomResponse, Targets,
};
use tokio::sync::{RwLock, mpsc, oneshot};

/// 房间命令队列容量（§4.9-9：有界 1024）。
const ROOM_CHANNEL_CAPACITY: usize = 1024;

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
#[derive(Default)]
pub struct Metrics {
    inner: std::sync::Mutex<HashMap<&'static str, CommandStats>>,
}

impl Metrics {
    fn record(
        &self,
        name: &'static str,
        result: &Result<RoomResponse, RoomError>,
        elapsed: Duration,
    ) {
        let mut guard = self.inner.lock().unwrap();
        let stats = guard.entry(name).or_default();
        stats.calls += 1;
        stats.avg_latency_ms = (stats.avg_latency_ms * (stats.calls - 1) as f64
            + elapsed.as_secs_f64() * 1000.0)
            / stats.calls as f64;
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
        let guard = self.inner.lock().unwrap();
        let mut v: Vec<_> = guard.iter().map(|(k, s)| (*k, *s)).collect();
        v.sort_by_key(|(k, _)| *k);
        v
    }

    /// 内部错误总数（§3.2：错误率统计基数）。
    pub fn internal_errors(&self) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .values()
            .map(|s| s.internal)
            .sum()
    }
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
}

/// 柜台（§2.4）：命令路由 + 事件广播 + 房间生命周期。
#[derive(Clone)]
pub struct Bus {
    inner: Arc<BusInner>,
}

impl Bus {
    /// 创建柜台。`factory` 由组合根注入并持有 deps（§4.9-6）。
    pub fn new(factory: Arc<dyn RoomFactory>, config: Arc<RoomConfig>) -> Self {
        Self {
            inner: Arc::new(BusInner {
                factory,
                rooms: RwLock::default(),
                routes: RwLock::default(),
                sink: std::sync::Mutex::new(None),
                metrics: Metrics::default(),
                config: RwLock::new(config),
            }),
        }
    }

    /// 挂接事件投递目标（阶段 2：转换层 + session 写路径）。
    pub fn attach_sink(&self, sink: Arc<dyn EventSink>) {
        *self.inner.sink.lock().unwrap() = Some(sink);
    }

    /// 计数器访问（§3.2 / §11.1）。
    pub fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }

    /// 当前生效的房间配置（§4.9-8）。
    pub async fn room_config(&self) -> Arc<RoomConfig> {
        Arc::clone(&*self.inner.config.read().await)
    }

    /// 派发客户端命令（session 收包解码后调用）。
    pub async fn dispatch(&self, ctx: CmdCtx, cmd: RoomCommand) -> Result<RoomResponse, RoomError> {
        let name = command_name(&cmd);
        let started = Instant::now();
        let result = self.route(ctx, cmd).await;
        self.inner.metrics.record(name, &result, started.elapsed());
        result
    }

    /// 派发系统命令（用户生命周期任务 / 定时器用，§4.6）。
    ///
    /// `room_id` 由调用方（生命周期任务查表后）填好；`GetClientState` 会返回响应（§4.4）。
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
    /// TODO(阶段 5): `watch_config` 文件轮询监听（解析 server_config.yml），机制 = 本方法。
    pub async fn update_config(&self, config: Arc<RoomConfig>) {
        *self.inner.config.write().await = Arc::clone(&config);
        let senders: Vec<(RoomId, mpsc::Sender<Envelope>)> = {
            let rooms = self.inner.rooms.read().await;
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

    /// 路由解析 + 投递（§4.9-4 路由规则）。
    async fn route(&self, ctx: CmdCtx, cmd: RoomCommand) -> Result<RoomResponse, RoomError> {
        let needs_response = command_needs_response(&cmd);
        let policy = queue_policy(&cmd);

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
            RoomCommand::CreateRoom { id } => {
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
        | RoomCommand::GetClientState { .. } => QueuePolicy::Wait,
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
        | RoomCommand::GetClientState { .. } => true,
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

/// 事件流程（§4.9-4 时序不变量）：解析 targets → 应用增量 → 响应 → 投递。
///
/// 返回该房间是否已关闭（RoomClosed）。
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
                    // 领域事件：恒 All（§4.4 分类；UserJoined 对称情形进 Oracle 核实清单）
                    deliveries.extend(
                        routes
                            .iter()
                            .filter(|(_, rid)| **rid == *room_id)
                            .map(|(u, _)| (*u, ev.clone())),
                    );
                }
            }
        }
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
    let sink = inner.sink.lock().unwrap().clone();
    if let Some(sink) = sink {
        for (user_id, ev) in deliveries {
            sink.deliver(user_id, &ev).await;
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
