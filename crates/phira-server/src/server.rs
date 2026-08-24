//! 服务器（§4.5）：监听 + accept + 连接处理（握手 → 鉴权 → 命令派发 → 事件投递）。
//!
//! 阶段 2 接线完成：`handle_connection` 驱动协议全流程（§6.6 表 1/表 2 + §4.9-3 生命周期）。
//!
//! TODO(阶段 5): 向所有房间广播"服务器维护中" + 宽限窗口（§11）。

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::time::Instant;

use anyhow::Result;
use phira_api::{
    AuthError, AuthHandler, ClientCommand, CmdCtx, HEARTBEAT_DISCONNECT_TIMEOUT, Origin,
    RoomCommand, RoomEvent, RoomResponse, ServerCommand, UserInfo,
};
use phira_core::{
    Bus, EventSink,
    convert::{client_to_room, response_to_server},
    lifecycle::{LifecycleEvent, SessionRegistry},
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc};
use tracing::{info, warn};

use crate::stream::Stream;

/// 服务器：持有监听器 + 柜台（组合根唯一接线点之外，本结构不认识具体货物）。
pub struct Server {
    listener: TcpListener,
    /// 连接处理上下文（bus/鉴权/生命周期/投递），accept 时克隆。
    ctx: Arc<ConnContext>,
}

impl Server {
    /// 绑定端口（默认 12346，§3.5）。
    ///
    /// # Errors
    ///
    /// 端口绑定失败（占用 / 权限）→ `std::io::Error`。
    pub async fn new(addr: SocketAddr, ctx: ConnContext) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            ctx: Arc::new(ctx),
        })
    }

    /// 运行主循环：accept → 会话（阶段 2 全流程）。
    ///
    /// # Errors
    ///
    /// 获取本地地址失败 / 停机信号 handler 安装失败。
    pub async fn run(self) -> Result<()> {
        let local = self.listener.local_addr()?;
        info!("r0semi-mp-server listening on {local}");

        // 优雅停机（§11）：SIGTERM/SIGINT → 停止 accept
        let shutdown = shutdown_signal();

        tokio::select! {
            () = shutdown => {
                info!("shutdown signal received");
                // TODO(阶段 5): 向所有房间广播"服务器维护中" + 宽限窗口（§11）
            }
            () = self.accept_loop() => {}
        }
        Ok(())
    }

    async fn accept_loop(self) {
        let accept = self.listener;
        loop {
            match accept.accept().await {
                Ok((stream, addr)) => {
                    info!("connection from {addr}");
                    let ctx = Arc::clone(&self.ctx);
                    tokio::spawn(async move {
                        if let Err(err) = handle_connection(stream, addr, ctx).await {
                            warn!("connection handler error from {addr}: {err:?}");
                        }
                    });
                }
                Err(err) => warn!("failed to accept: {err:?}"),
            }
        }
    }
}

/// 连接处理上下文（组合根接线；accept 时 Arc 共享）。
pub struct ConnContext {
    /// 柜台：命令路由 + 事件广播。
    pub bus: Bus,
    /// 鉴权处理器（回源 /me，§6.5-14）。
    pub auth: Arc<dyn AuthHandler>,
    /// 会话注册表（epoch 分配，§4.9-3）。
    pub registry: Arc<SessionRegistry>,
    /// 生命周期事实发送端（Connected/Disconnected，§4.9-3）。
    pub fact_tx: mpsc::Sender<LifecycleEvent>,
    /// 事件投递（user → 会话写通道 + 转换层过滤，§6.6 表 2）。
    pub sink: Arc<SessionSink>,
}

/// 事件投递：`user_id → 会话发送通道`映射 + 转换层目标过滤。
///
/// bus 按事件 targets 投递 `deliver(user_id, event)`（领域=All/Relay=Specific）；
/// 本实现再按转换层产出的**命令级 targets** 过滤（如 NewHost 的 ChangeHost 只给新旧房主）。
pub struct SessionSink {
    sessions: RwLock<std::collections::HashMap<i32, Arc<mpsc::Sender<ServerCommand>>>>,
}

impl Default for SessionSink {
    fn default() -> Self {
        Self {
            sessions: RwLock::new(std::collections::HashMap::new()),
        }
    }
}

impl SessionSink {
    /// 新投递表。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册会话（鉴权成功）：替换旧连接（重连语义，§6.5-19）。
    pub async fn register(&self, user_id: i32, tx: Arc<mpsc::Sender<ServerCommand>>) {
        self.sessions.write().await.insert(user_id, tx);
    }

    /// 注销会话：仅当当前映射仍是本连接（重连后旧连接断开不误删新连接，§4.9-3）。
    pub async fn unregister(&self, user_id: i32, tx: &Arc<mpsc::Sender<ServerCommand>>) {
        let mut sessions = self.sessions.write().await;
        if sessions
            .get(&user_id)
            .is_some_and(|cur| Arc::ptr_eq(cur, tx))
        {
            sessions.remove(&user_id);
        }
    }
}

#[async_trait::async_trait]
impl EventSink for SessionSink {
    async fn deliver(&self, user_id: i32, event: &RoomEvent) {
        let commands = phira_core::convert::event_to_server(event.clone());
        for (targets, cmd) in commands {
            let should_send = match targets {
                phira_api::Targets::All => true,
                phira_api::Targets::Specific(ids) => ids.contains(&user_id),
            };
            if should_send && let Some(tx) = self.sessions.read().await.get(&user_id) {
                // 队列满/连接断开 → 丢弃（send 任务已退出）；热路径可丢（§4.9-9）
                let _ = tx.send(cmd).await;
            }
        }
    }
}

/// 单连接状态（跨 handler 调用共享；原子标志 + 轻量 Mutex）。
struct ConnState {
    /// 已鉴权（§6.5-13 之前只收 Ping/Authenticate）。
    authed: AtomicBool,
    /// 鉴权失败 / 连接应终止（后续帧忽略）。
    panicked: AtomicBool,
    /// 收尾只执行一次（客户端断开 或 心跳超时，CAS）。
    closed: AtomicBool,
    user_id: AtomicI32,
    epoch: AtomicU64,
    /// 最近收包时刻（心跳监控，§6.1 10s 无包判定断线）。
    last_recv: Mutex<Instant>,
    /// 本连接发送通道（sink 注销比较用）。
    send_tx: Mutex<Option<Arc<mpsc::Sender<ServerCommand>>>>,
}

impl ConnState {
    fn new() -> Self {
        Self {
            authed: AtomicBool::new(false),
            panicked: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            user_id: AtomicI32::new(0),
            epoch: AtomicU64::new(0),
            last_recv: Mutex::new(Instant::now()),
            send_tx: Mutex::new(None),
        }
    }
}

/// 单连接处理（§4.5）：握手 → 心跳 → 鉴权 → 命令派发 → 事件投递 → 断开收尾。
///
/// # Errors
///
/// 握手失败（版本读取失败）时返回；业务错误走 `warn` 日志（不中断 accept）。
pub async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    ctx: Arc<ConnContext>,
) -> Result<()> {
    let state = Arc::new(ConnState::new());

    let handler_ctx = Arc::clone(&ctx);
    let handler_state = Arc::clone(&state);
    let handler = Box::new(
        move |send_tx: Arc<mpsc::Sender<ServerCommand>>, cmd: ClientCommand| {
            let ctx = Arc::clone(&handler_ctx);
            let state = Arc::clone(&handler_state);
            async move {
                handle_frame(&ctx, &state, &send_tx, cmd).await;
            }
        },
    );

    let stream = Stream::<ServerCommand, ClientCommand>::new(None, stream, handler).await?;
    let stream = Arc::new(tokio::sync::Mutex::new(Some(stream)));

    // 心跳监控（§6.1）：10s 无任何包 → 判断线 → 生命周期 Disconnected + 断开连接
    let monitor_state = Arc::clone(&state);
    let monitor_ctx = Arc::clone(&ctx);
    let monitor_stream = Arc::clone(&stream);
    let monitor = tokio::spawn(async move {
        loop {
            let recv = *monitor_state
                .last_recv
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            tokio::time::sleep_until(recv + HEARTBEAT_DISCONNECT_TIMEOUT).await;
            if *monitor_state
                .last_recv
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                + HEARTBEAT_DISCONNECT_TIMEOUT
                > Instant::now()
            {
                continue;
            }
            // 超时：标记断开 + 通知生命周期 + 断开连接
            if monitor_state.closed.swap(true, Ordering::SeqCst) {
                return; // 已收尾（客户端已断）
            }
            let user_id = monitor_state.user_id.load(Ordering::SeqCst);
            let epoch = monitor_state.epoch.load(Ordering::SeqCst);
            if monitor_state.authed.load(Ordering::SeqCst) {
                warn!("heartbeat timeout, user={user_id} disconnected");
                let _ = monitor_ctx
                    .fact_tx
                    .send(LifecycleEvent::Disconnected { user_id, epoch })
                    .await;
            }
            // 断开连接（drop Stream → abort 收发任务）
            let mut guard = monitor_stream.lock().await;
            if let Some(s) = guard.take() {
                drop(s);
            }
            drop(guard);
            return;
        }
    });

    // 等待连接关闭（客户端断开 / 监控超时 drop）
    {
        let mut guard = stream.lock().await;
        if let Some(s) = guard.take() {
            drop(guard);
            s.await_closed().await;
        }
    }

    // 收尾（幂等：监控超时可能已做）
    if !state.closed.swap(true, Ordering::SeqCst) {
        let user_id = state.user_id.load(Ordering::SeqCst);
        let epoch = state.epoch.load(Ordering::SeqCst);
        if state.authed.load(Ordering::SeqCst) {
            let _ = ctx
                .fact_tx
                .send(LifecycleEvent::Disconnected { user_id, epoch })
                .await;
        }
    }
    // 注销会话（仅当仍是本连接；先取出发送端再 await，避免 MutexGuard 跨 await）
    let send_tx_opt = {
        let guard = state
            .send_tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.clone()
    };
    if let Some(tx) = send_tx_opt {
        ctx.sink
            .unregister(state.user_id.load(Ordering::SeqCst), &tx)
            .await;
    }
    monitor.abort();
    info!("connection from {addr} closed");
    Ok(())
}

/// 单帧处理（handler 主体）：心跳 / 鉴权 / 命令派发。
async fn handle_frame(
    ctx: &ConnContext,
    state: &ConnState,
    send_tx: &Arc<mpsc::Sender<ServerCommand>>,
    cmd: ClientCommand,
) {
    *state
        .last_recv
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Instant::now();

    if state.panicked.load(Ordering::SeqCst) {
        return;
    }

    if matches!(cmd, ClientCommand::Ping) {
        let _ = send_tx.send(ServerCommand::Pong).await;
        return;
    }

    if !state.authed.load(Ordering::SeqCst) {
        // 鉴权前只接受 Authenticate（§6.5-13）
        if let ClientCommand::Authenticate { token } = cmd {
            authenticate_flow(ctx, state, send_tx, token.as_str()).await;
        } else {
            warn!("packet before authentication, ignoring: {cmd:?}");
        }
        return;
    }

    // —— 已鉴权命令 ——
    let user_id = state.user_id.load(Ordering::SeqCst);
    // 心跳/鉴权归 core；其余转房间命令（§6.6 表 1；CreateRoom/JoinRoom 需要昵称）
    let name = ctx.registry.name_of(user_id).unwrap_or_default();
    let Some(room_cmd) = client_to_room(cmd.clone(), name) else {
        return;
    };
    // 路由目标：CreateRoom/JoinRoom 用载荷 id（bus 盖章，§4.9-4）；其余 bus 查表覆盖
    let room_id =
        match &cmd {
            ClientCommand::CreateRoom { id } | ClientCommand::JoinRoom { id, .. } => id.clone(),
            _ => ctx.bus.room_of(user_id).await.unwrap_or_else(|| {
                phira_api::RoomId::new("x".to_owned()).expect("static valid id")
            }),
        };
    let resp = ctx
        .bus
        .dispatch(
            CmdCtx {
                origin: Origin::Client { user_id },
                room_id,
            },
            room_cmd,
        )
        .await;
    let server_cmd = response_to_server(&cmd, resp);
    let _ = send_tx.send(server_cmd).await;
}

/// 鉴权流程（§6.5-14/19/23）：回源 /me → 注册 epoch → 恢复房间状态 → 应答。
async fn authenticate_flow(
    ctx: &ConnContext,
    state: &ConnState,
    send_tx: &Arc<mpsc::Sender<ServerCommand>>,
    token: &str,
) {
    match ctx.auth.authenticate(token).await {
        Ok(identity) => {
            let user_id = identity.user_id;
            let epoch = ctx.registry.register(user_id, identity.name.clone());
            state.user_id.store(user_id, Ordering::SeqCst);
            state.epoch.store(epoch, Ordering::SeqCst);
            *state
                .send_tx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(send_tx));
            ctx.sink.register(user_id, Arc::clone(send_tx)).await;
            let _ = ctx
                .fact_tx
                .send(LifecycleEvent::Connected { user_id, epoch })
                .await;

            // 重连恢复：携带当前房间状态（§6.5-23）
            let room_state = match ctx.bus.room_of(user_id).await {
                Some(rid) => match ctx
                    .bus
                    .dispatch_system(rid, RoomCommand::GetClientState { user_id })
                    .await
                {
                    Ok(RoomResponse::ClientState(cs)) => cs,
                    _ => None,
                },
                None => None,
            };
            let info = UserInfo {
                id: user_id,
                name: identity.name,
                monitor: false,
            };
            let _ = send_tx
                .send(ServerCommand::Authenticate(Ok((info, room_state))))
                .await;
            state.authed.store(true, Ordering::SeqCst);
            info!("user={user_id} authenticated (epoch={epoch})");
        }
        Err(err) => {
            let msg = match err {
                AuthError::Business { msg, .. } => msg,
                AuthError::Internal { msg } => {
                    warn!("auth upstream failed: {msg}");
                    "internal error".to_owned()
                }
            };
            let _ = send_tx.send(ServerCommand::Authenticate(Err(msg))).await;
            // 鉴权失败：忽略后续帧，等客户端断开（原版语义：立即断开）
            state.panicked.store(true, Ordering::SeqCst);
        }
    }
}

/// 优雅停机信号（§11）：SIGTERM（Unix）或 Ctrl+C（Windows）。
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl-c handler");
    }
}
