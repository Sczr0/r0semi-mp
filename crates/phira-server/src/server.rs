//! 服务器（§4.5）：监听 + accept + 连接处理（握手 → 鉴权 → 命令派发 → 事件投递）。
//!
//! 阶段 2 接线完成：`handle_connection` 驱动协议全流程（§6.6 表 1/表 2 + §4.9-3 生命周期）。
//!
//! 优雅停机（§11）：SIGTERM/SIGINT → 广播"服务器维护中" → 宽限窗口 → 强制退出。
//!
//! # 前置层（Front Gate）——连接建立瞬间的"第一线"
//!
//! 借鉴 Blade 网关思想（Solar Network）：把连接建立时的横切职责集中成一条链，
//! 未来新增准入规则/分流时改动集中在此，不散落进会话逻辑：
//!
//! ```text
//! accept → [PROXY protocol（反代真实 IP，config 开关）] → 连接准入（未鉴权上限 + 每 IP）
//!        → 握手/鉴权前帧上限（4KiB）→ 心跳监控（10s 无包判死）
//!        → 发送积压踢出（乌龟客户端）→ 命令限速（ADR-0008）→ 内存守卫（ADR-0010）
//! ```
//!
//! 各环节职责与代码位置：
//! - PROXY 解析：`crate::proxy`（本文件 `handle_connection` 开头，准入前——按真实 IP 计数）
//! - 连接准入：`ConnectionAdmission`（`try_acquire`/`release`，§10.4）
//! - 握手/帧分级：`Stream::new` + `PRE_AUTH_MAX_PACKET`（§10.4 鉴权前 4KiB）
//! - 心跳：本文件 monitor 任务（§6.1 10s）
//! - 积压踢出：本文件 kicker 任务（ISSUE-0004）
//! - 命令限速：`CommandLimiter`（ADR-0008）
//! - 内存守卫：`IN_FLIGHT_BYTES`/`queue_bytes`（ADR-0010）

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

use anyhow::Result;
use phira_api::{
    AuthError, AuthHandler, ClientCommand, CmdCtx, HEARTBEAT_DISCONNECT_TIMEOUT, Origin,
    RoomCommand, RoomError, RoomErrorCode, RoomEvent, RoomId, RoomResponse, ServerCommand,
    UserInfo, encode_packet,
};
use phira_core::{
    Bus, EventSink,
    convert::{client_to_room, response_to_server},
    lifecycle::{LifecycleEvent, SessionRegistry},
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc};
use tracing::{info, warn};

use crate::proxy;
use crate::stream::{MAX_PACKET_SIZE, Outbound, PRE_AUTH_MAX_PACKET, PROTOCOL_VERSION, Stream};

/// 服务器：持有监听器 + 柜台（组合根唯一接线点之外，本结构不认识具体货物）。
pub struct Server {
    listener: TcpListener,
    /// 连接处理上下文（bus/鉴权/生命周期/投递），accept 时克隆。
    ctx: Arc<ConnContext>,
    /// 停机维护通知文案（§11 系统 Chat，yml `maintenance_notice`）。
    maintenance_notice: String,
    /// 停机宽限窗口（§11，yml `maintenance_grace`）。
    maintenance_grace: std::time::Duration,
    /// 管理 HTTP 监听器（§运营：/rooms 房间列表，yml `http_port`）。
    http_listener: Option<TcpListener>,
}

impl Server {
    /// 绑定端口（默认 12346，§3.5）并指定停机维护参数（yml 接线点）。
    ///
    /// # Errors
    ///
    /// 端口绑定失败（占用 / 权限）→ `std::io::Error`。
    pub async fn new(
        addr: SocketAddr,
        ctx: ConnContext,
        maintenance_notice: String,
        maintenance_grace: std::time::Duration,
        http_port: Option<u16>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let http_listener = match http_port {
            Some(port) => {
                let l = TcpListener::bind(("0.0.0.0", port)).await?;
                info!("http admin listening on :{port} (rooms list)");
                Some(l)
            }
            None => None,
        };
        Ok(Self {
            listener,
            ctx: Arc::new(ctx),
            maintenance_notice,
            maintenance_grace,
            http_listener,
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
        let ctx = Arc::clone(&self.ctx);
        let notice = self.maintenance_notice.clone();
        let grace = self.maintenance_grace;
        // 拆字段给两个 accept 循环（select 分支不能同时 move self）
        let listener = self.listener;
        let http_listener = self.http_listener;

        // ISSUE-0008 修复：accept 循环放后台任务，**shutdown 是唯一退出路径**——
        // 修复前 select 任一分支完成即退出：http_port 未配置时 http_accept_loop 立即返回
        // → 默认配置下服务器启动即退出（ISSUE-0008）。
        let accept = tokio::spawn(accept_loop(listener, Arc::clone(&ctx)));
        let http_accept = tokio::spawn(crate::admin::http_accept_loop(
            http_listener,
            Arc::clone(&ctx),
        ));

        tokio::select! {
            () = shutdown => {
                info!("shutdown signal received, broadcasting maintenance notice");
                // §11：广播"服务器维护中"（系统 Chat，user=0）+ 宽限窗口供玩家看到。
                // 无持久化下不存在"排空"语义——停机即丢房，降低损失靠消息 + 快速重启。
                ctx.sink
                    .broadcast(ServerCommand::Message(phira_api::Message::Chat {
                        user: 0,
                        content: notice,
                    }))
                    .await;
                info!("maintenance grace window {grace:?}");
                tokio::time::sleep(grace).await;
            }
        }

        // 停止接受新连接（accept 循环中止；已建立的连接由各自任务自然结束）
        accept.abort();
        http_accept.abort();
        Ok(())
    }
}

/// MP 协议 accept 循环（组合根 run() 调用）。
async fn accept_loop(listener: TcpListener, ctx: Arc<ConnContext>) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("connection from {addr}");
                let ctx = Arc::clone(&ctx);
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

/// 管理 HTTP accept 循环（独立端口，§运营：/rooms；http_port 未配置时立即结束）。
/// 房间列表快照项（§运营：公开房间列表，`/rooms` HTTP 端点返回）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct RoomInfo {
    /// 房间 id。
    pub id: String,
    /// 房主用户 id。
    pub host: i32,
    /// 当前人数。
    pub users: usize,
    /// 房间状态。
    pub state: String,
    /// 是否锁定。
    pub locked: bool,
    /// 循环对局（admin 详情用，阶段 1：RoomListSink 维护 CycleRoom 事件）。
    pub cycle: bool,
}

/// 房间列表观察者（§7.3 观察者模式）：订阅事件维护活动房间快照。
///
/// 纯观察者——不碰核心（bus/actor），数据源 = EventSink 事件流。
/// 隐私过滤：房间 id 匹配 `hidden_prefixes` 任一前缀 → 不进入公开列表。
pub struct RoomListSink {
    rooms: tokio::sync::RwLock<std::collections::HashMap<RoomId, RoomInfo>>,
    /// 私密房间 id 前缀（yml `hidden_room_prefixes`，如 `["solo"]`）。
    hidden_prefixes: Vec<String>,
}

impl RoomListSink {
    /// 构造。`hidden_prefixes` = 私密房间 id 前缀（命中则不公开展示）。
    #[must_use]
    pub fn new(hidden_prefixes: Vec<String>) -> Self {
        Self {
            rooms: tokio::sync::RwLock::new(std::collections::HashMap::new()),
            hidden_prefixes,
        }
    }

    fn hidden(&self, id: &RoomId) -> bool {
        self.hidden_prefixes
            .iter()
            .any(|p| id.as_str().starts_with(p))
    }

    /// 公开房间列表快照（已过滤私密房间）。
    pub async fn snapshot(&self) -> Vec<RoomInfo> {
        let mut list: Vec<_> = self.rooms.read().await.values().cloned().collect();
        list.sort_by(|a, b| a.id.cmp(&b.id));
        list
    }
}

#[async_trait::async_trait]
impl EventSink for RoomListSink {
    async fn deliver(&self, _user_id: i32, event: &RoomEvent) {
        use phira_api::RoomEvent as E;
        match event {
            E::RoomCreated { room_id, host } => {
                if !self.hidden(room_id) {
                    self.rooms.write().await.insert(
                        room_id.clone(),
                        RoomInfo {
                            id: room_id.as_str().to_owned(),
                            host: *host,
                            users: 1,
                            state: "SelectChart".to_owned(),
                            locked: false,
                            cycle: false,
                        },
                    );
                }
            }
            E::RoomClosed { room_id } => {
                self.rooms.write().await.remove(room_id);
            }
            E::UserJoined { room_id, .. } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.users += 1;
                }
            }
            E::UserLeft { room_id, .. } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.users = r.users.saturating_sub(1);
                }
            }
            E::NewHost {
                room_id, new_host, ..
            } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.host = *new_host;
                }
            }
            E::SelectChart { room_id, id, .. } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.state = format!("SelectChart({id})");
                }
            }
            E::GameStart { room_id, .. } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    "WaitingForReady".clone_into(&mut r.state);
                }
            }
            E::StartPlaying { room_id } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    "Playing".clone_into(&mut r.state);
                }
            }
            E::GameEnd { room_id, chart } | E::CancelGame { room_id, chart, .. } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.state = match chart {
                        Some(id) => format!("SelectChart({id})"),
                        None => "SelectChart".to_owned(),
                    };
                }
            }
            E::LockRoom { room_id, lock } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.locked = *lock;
                }
            }
            E::CycleRoom { room_id, cycle } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.cycle = *cycle;
                }
            }
            // 热路径（RelayTouches/Judges）与不改变列表展示的（Chat/Ready/Played/Abort/CycleRoom）
            // 不更新快照
            _ => {}
        }
    }
}

/// 组合投递目标：多个 EventSink 的扇出（§4.9-5 观察者组合，bus 零改动）。
#[derive(Default)]
pub struct CompositeSink {
    sinks: tokio::sync::RwLock<Vec<Arc<dyn EventSink>>>,
}

impl CompositeSink {
    /// 构造时注入观察者列表（同步，避免 async 构造在非 async 上下文不可用）。
    #[must_use]
    pub fn new(sinks: Vec<Arc<dyn EventSink>>) -> Self {
        Self {
            sinks: tokio::sync::RwLock::new(sinks),
        }
    }
}

#[async_trait::async_trait]
impl EventSink for CompositeSink {
    async fn deliver(&self, user_id: i32, event: &RoomEvent) {
        let sinks = self.sinks.read().await.clone();
        for sink in sinks {
            sink.deliver(user_id, event).await;
        }
    }
}

/// 连接处理上下文（组合根接线；accept 时 Arc 共享）。
#[derive(Clone)]
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
    /// 连接准入（§10.4：未鉴权连接上限 + 每 IP 限额）。
    pub admission: Arc<ConnectionAdmission>,
    /// 进服欢迎语（yml `welcome_message`；鉴权成功后发给本人，None = 不发）。
    pub welcome_message: Option<String>,
    /// 房间列表快照（§运营 `/rooms`；HTTP 分流端点读取）。
    pub room_list: Arc<RoomListSink>,
    /// PROXY protocol 开关（§前置层：反代后真实 IP；yml `proxy_protocol`）。
    pub proxy_protocol: bool,
    /// 管理 API Bearer token（阶段 2；None = 管理面禁用，见 admin.rs）。
    pub admin_token: Option<String>,
    /// 管理写操作审计（docs/admin-api.md §3 四件套之一；组合根注入）。
    pub admin_audit: Arc<crate::admin::AuditLog>,
}

/// 事件投递：`user_id → 会话发送通道`映射 + 转换层目标过滤。
///
/// bus 按事件 targets 投递 `deliver(user_id, event)`（领域=All/Relay=Specific）；
/// 本实现再按转换层产出的**命令级 targets** 过滤（如 NewHost 的 ChangeHost 只给新旧房主）。
/// 发送积压标记（ISSUE-0004 修复：踢"乌龟"客户端）。
///
/// 每连接发送队列（1024 帧）持续满超过 [`SLOW_CONSUMER_KICK_AFTER`] → 判定慢消费者 → 断连。
/// 正常波动自愈：下一次 `try_send` 成功即清除标记（§10.4 哲学：绝不阻塞房间 actor）。
pub struct Backpressure {
    since: Mutex<Option<Instant>>,
    /// 内存守卫强制踢出（安全锁 A：每连接/全局超限——不等积压超时，立即踢）。
    force_close: AtomicBool,
}

impl Backpressure {
    /// 新标记（未积压）。
    #[must_use]
    pub const fn new() -> Self {
        Self {
            since: Mutex::new(None),
            force_close: AtomicBool::new(false),
        }
    }

    /// 内存守卫强制踢出（安全锁 A）。
    pub fn force_close(&self) {
        self.force_close.store(true, Ordering::SeqCst);
    }

    /// 是否被强制踢出。
    #[must_use]
    pub fn is_forced(&self) -> bool {
        self.force_close.load(Ordering::SeqCst)
    }

    /// 标记积压开始（仅首次失败时设置，幂等）。
    pub fn mark(&self) {
        let mut s = self
            .since
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if s.is_none() {
            *s = Some(Instant::now());
        }
    }

    /// 队列恢复有空间：清除积压标记。
    pub fn clear(&self) {
        let mut s = self
            .since
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *s = None;
    }

    /// 积压持续时长（未积压 = None）。
    #[must_use]
    pub fn elapsed(&self) -> Option<Duration> {
        let s = self
            .since
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        s.map(|t| t.elapsed())
    }
}

impl Default for Backpressure {
    fn default() -> Self {
        Self::new()
    }
}

/// 慢消费者判定阈值：发送队列持续满的时长（ISSUE-0004 验收）。
///
/// 60Hz 触摸流下 5s ≈ 300 帧——队列持续满 5s 即确认写任务被 socket 阻塞（客户端不收包）。
/// 可参数化（后续进 config；当前常量满足 v1）。
const SLOW_CONSUMER_KICK_AFTER: Duration = Duration::from_secs(5);
/// 安全锁 A：全局在途字节上限（§10.4 承诺兑现）。
///
/// 威胁模型：海量已鉴权连接 × 大帧（Touches/Judges 热路径，帧上限 2MiB）——
/// 每连接 send 队列 1024 × 2MiB = 2GB。全局记账 + 超限丢新 + 断最重连接，
/// 内存**硬上限**（攻击下绝不膨胀）。
const MEMORY_GUARD_LIMIT: usize = 64 * 1024 * 1024;

/// 每连接 send 队列字节上限（超限 → 该连接被踢）。
const PER_CONN_MEM_LIMIT: usize = 8 * 1024 * 1024;

/// 安全锁 B：已鉴权连接总数上限（§11"总连接数上限"兑现）。
///
/// 未鉴权上限（100）已存在；鉴权后无上限 = 攻击者用真实 token 建海量连接的内存/CPU 向量。
const MAX_AUTHED_CONNECTIONS: usize = 1000;

/// 全局在途字节（安全锁 A；进程单例——一个进程一个内存守卫）。
static IN_FLIGHT_BYTES: AtomicUsize = AtomicUsize::new(0);

/// 已鉴权连接数（安全锁 B；进程单例）。
static AUTHED_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

/// 全局记账：返回 false = 超限（调用方丢新）。
pub(crate) fn charge_memory(bytes: usize) -> bool {
    IN_FLIGHT_BYTES.fetch_add(bytes, Ordering::SeqCst) + bytes <= MEMORY_GUARD_LIMIT
}

/// 全局记账：写任务消费后释放。
pub(crate) fn release_memory(bytes: usize) {
    IN_FLIGHT_BYTES.fetch_sub(bytes, Ordering::SeqCst);
}

/// 当前在途字节（安全锁 A 观测：测试断言记账平衡；/healthz 可显示）。
#[must_use]
pub fn in_flight_bytes() -> usize {
    IN_FLIGHT_BYTES.load(Ordering::SeqCst)
}
/// 每连接命令限速器（ISSUE-0006 修复：滥用控制"快端"防线）。
///
/// 令牌桶简化版：每个受限命令类别记录"上次允许时刻"，距上次 ≥ interval 才放行。
/// 超限 → 回 `TooManyRequests` Business 错误（客户端可见），不触发队列 Reject 断连
/// （§4.9-9："滥用控制优先用每连接限速，不让队列压力触发断连"）。
pub struct CommandLimiter {
    last: Mutex<std::collections::HashMap<&'static str, Instant>>,
}

impl CommandLimiter {
    /// 新限速器（空表）。
    #[must_use]
    pub fn new() -> Self {
        Self {
            last: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// 尝试放行：距上次允许 ≥ `interval` 才允许并更新时间戳；否则拒绝。
    pub fn allow(&self, cmd: &'static str, interval: Duration) -> bool {
        let mut last = self
            .last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        if last
            .get(cmd)
            .is_some_and(|t| now.duration_since(*t) < interval)
        {
            return false;
        }
        last.insert(cmd, now);
        true
    }
}

impl Default for CommandLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// 热路径编码缓存（ISSUE-0003 方案 2：编码一次共享）。
///
/// 独立组件（非 SessionSink 内嵌逻辑）——未来需要泛化（所有事件共享编码）时
/// 可整体提升到 bus 层（ADR-0009：泛化触发条件 = 第二个大扇出广播场景）。
/// 缓存键 = 帧 `Arc` 指针地址（同一帧的多个 monitor 投递命中同一缓存），
/// 值 = 编码后的载荷（不含 ULEB128 长度前缀，写任务统一加）。
///
/// ## ABA 防护（ISSUE-0011）
/// 键是裸地址；缓存条目**同时持有源 `Arc` 的克隆**（[`EncodeEntry::_pin`]），
/// 条目存活期间该源地址被强引用钉住 → 分配器不可能把这块内存复用于新批次，
/// 从而**杜绝**"新事件命中历史死条目、观战者收到陈旧帧"的 ABA 危害。
/// 旧注释「旧帧指针不复用，留着只会是死条目」是错误断言：
/// Rust 分配器不承诺地址唯一性，同尺寸释放块被立即复用是常见行为。
pub struct EncodeCache {
    inner: Mutex<std::collections::HashMap<usize, EncodeEntry>>,
    capacity: usize,
}

/// 缓存条目：编码结果 + 钉住源 `Arc` 的擦除类型持有者（[`EncodeCache`] 的 ABA 说明）。
struct EncodeEntry {
    /// 钉住键所指向的内存块（源帧 `Arc` 克隆）；条目存活期间该地址不可被分配器复用。
    /// 用 `Box<dyn Any>` 擦除类型——`Touches`/`Judges` 两种源 `Arc` 类型不同，统一装箱。
    /// 字段名为 `_pin`（不以 `_` 开头的字段也会有未用警告，但这里它专用于生命周期钉住）。
    _pin: Box<dyn std::any::Any + Send + Sync>,
    bytes: Arc<Vec<u8>>,
}

impl EncodeCache {
    /// 新缓存（`capacity` = 最大缓存条目；满则整体清空——每帧最多命中一次，
    /// 清空后下一帧重新编码一次，可接受）。
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(std::collections::HashMap::new()),
            capacity,
        }
    }

    /// 取或编码：命中返回共享 `Arc<Vec<u8>>`；miss 则调用 `encode` 一次并缓存。
    ///
    /// `pin` = 键所指向的源 `Arc` 的克隆（`Box<dyn Any>` 擦除类型）：miss 时存入
    /// 条目以钉住地址（ISSUE-0011 ABA 防护），hit 时直接丢弃（缓存命中，无需新 pin）。
    pub fn get_or_encode(
        &self,
        key: usize,
        pin: Box<dyn std::any::Any + Send + Sync>,
        encode: impl FnOnce() -> Vec<u8>,
    ) -> Arc<Vec<u8>> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = inner.get(&key) {
            return Arc::clone(&entry.bytes);
        }
        let bytes = Arc::new(encode());
        if inner.len() >= self.capacity {
            inner.clear(); // 满则清（简单淘汰：pin 随条目一起释放，地址归还分配器）
        }
        inner.insert(
            key,
            EncodeEntry {
                _pin: pin,
                bytes: Arc::clone(&bytes),
            },
        );
        bytes
    }
}

impl Default for EncodeCache {
    fn default() -> Self {
        Self::new(64)
    }
}

/// 错误码 → 本地化 key 映射（B2 i18n）：仅对齐原版 ftl 覆盖的 6 条；
/// 未映射的错误码返回 None → 保留 impl 原文（优雅降级，不丢信息）。
const fn localize_key(code: RoomErrorCode) -> Option<crate::l10n::Key> {
    match code {
        RoomErrorCode::RoomIdOccupied => Some(crate::l10n::Key::CreateIdOccupied),
        RoomErrorCode::RoomLocked => Some(crate::l10n::Key::JoinRoomLocked),
        RoomErrorCode::GameOngoing => Some(crate::l10n::Key::JoinGameOngoing),
        RoomErrorCode::CannotMonitor => Some(crate::l10n::Key::JoinCantMonitor),
        RoomErrorCode::RoomFull => Some(crate::l10n::Key::JoinRoomFull),
        RoomErrorCode::NoChartSelected => Some(crate::l10n::Key::StartNoChartSelected),
        _ => None,
    }
}

/// 按发起者语言本地化 Failure 文案（B2 i18n 出口点，见 [`localize_key`]）。
///
/// `Ok(Failure(Business))` 与 `Err(Business)` 双形态都处理（`response_to_server`
/// 对二者同归一为 Err 文案——翻译须在归一化之前拦截）。
fn localize_failure(
    resp: Result<RoomResponse, RoomError>,
    lang: crate::l10n::Locale,
) -> Result<RoomResponse, RoomError> {
    let translate = |code: RoomErrorCode, msg: &mut String| {
        if let Some(key) = localize_key(code) {
            key.localized(lang).clone_into(msg);
        }
    };
    match resp {
        Ok(RoomResponse::Failure(RoomError::Business { code, msg })) => {
            let mut m = msg;
            translate(code, &mut m);
            Ok(RoomResponse::Failure(RoomError::Business { code, msg: m }))
        }
        Err(RoomError::Business { code, msg }) => {
            let mut m = msg;
            translate(code, &mut m);
            Err(RoomError::Business { code, msg: m })
        }
        other => other,
    }
}

/// 受限命令的限速键 + 最小间隔（ISSUE-0006：只限"贵"命令，资源成本驱动）。
///
/// - `CreateRoom`：spawn actor + channel（最贵）→ 1/s
/// - `JoinRoom`：入房流程 + 广播 → 5/s
/// - `SelectChart`/`Played`：回源官方 API（配额宝贵）→ 5/s
/// - `Chat`：高频滥用（刷屏/垃圾消息，对客户端可感知且易被当作攻击面）→ 2/s（D1 技术债）
/// - 热路径（Touches/Judges）不限（靠 DropIfFull + 帧上限）
///
/// 注：间隔/白名单是 v1 常量（可参数化进 config）；热路径滥用靠队列 DropIfFull + 帧上限兜底。
const fn rate_limit(cmd: &ClientCommand) -> Option<(&'static str, Duration)> {
    match cmd {
        ClientCommand::CreateRoom { .. } => Some(("create_room", Duration::from_millis(1000))),
        ClientCommand::JoinRoom { .. } => Some(("join_room", Duration::from_millis(200))),
        ClientCommand::SelectChart { .. } => Some(("select_chart", Duration::from_millis(200))),
        ClientCommand::Played { .. } => Some(("played", Duration::from_millis(200))),
        ClientCommand::Chat { .. } => Some(("chat", Duration::from_millis(500))),
        _ => None,
    }
}

/// 积压检查间隔（kicker 轮询粒度）：远小于阈值，保证踢出延迟 ≈ 阈值 + 1 拍（≤6s），
/// 而不是阈值 + 整拍（最坏 2× 阈值）——"绝不无限积压"的判定也应快速。
const BACKPRESSURE_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// 发送槽位（SessionSink 投递表）：发送通道 + 积压标记 + send 队列字节记账。
struct SendSlot {
    tx: Arc<mpsc::Sender<Outbound>>,
    backpressure: Arc<Backpressure>,
    /// 本连接 send 队列在途字节（安全锁 A：写任务消费时经同一 Arc 递减）。
    queue_bytes: Arc<AtomicUsize>,
    /// 用户语言（B2 i18n：鉴权响应的 `language` 字段解析结果）——错误响应本地化用；
    /// 随会话生灭（unregister 释放），无影子表驻留（C2 同款纪律）。
    lang: crate::l10n::Locale,
}

pub struct SessionSink {
    sessions: RwLock<std::collections::HashMap<i32, Arc<SendSlot>>>,
    /// 热路径编码缓存（ISSUE-0003 方案 2）：同一帧的多 monitor 共享编码结果。
    encode_cache: EncodeCache,
}

impl Default for SessionSink {
    fn default() -> Self {
        Self {
            sessions: RwLock::new(std::collections::HashMap::new()),
            encode_cache: EncodeCache::default(),
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
    pub async fn register(
        &self,
        user_id: i32,
        tx: Arc<mpsc::Sender<Outbound>>,
        backpressure: Arc<Backpressure>,
        queue_bytes: Arc<AtomicUsize>,
        lang: crate::l10n::Locale,
    ) {
        self.sessions.write().await.insert(
            user_id,
            Arc::new(SendSlot {
                tx,
                backpressure,
                queue_bytes,
                lang,
            }),
        );
    }

    /// 查询用户语言（B2 i18n：错误响应出口本地化用；未注册回落默认 en-US）。
    async fn locale_of(&self, user_id: i32) -> crate::l10n::Locale {
        self.sessions
            .read()
            .await
            .get(&user_id)
            .map_or_else(crate::l10n::Locale::default, |slot| slot.lang)
    }

    /// 找出 send 队列在途字节最大的会话（安全锁 A：全局超限时断最重）。
    fn heaviest(&self) -> Option<(i32, Arc<Backpressure>)> {
        let Ok(s) = self.sessions.try_read() else {
            return None;
        };
        s.iter()
            .max_by_key(|(_, slot)| slot.queue_bytes.load(Ordering::SeqCst))
            .map(|(id, slot)| (*id, Arc::clone(&slot.backpressure)))
    }

    /// 在线用户 id 列表（管理面只读查询，§admin；按 id 排序稳定输出）。
    pub async fn online(&self) -> Vec<i32> {
        let mut ids: Vec<i32> = self.sessions.read().await.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// 管理断连（阶段 2，docs/admin-api.md：ban/disconnect）：借 kicker 的
    /// `force_close` 拆掉该用户连接（kicker 1s 轮询执行，连接收尾流程发
    /// 生命周期事实）；**不删会话映射**（断连是连接层事实）。返回该用户是否在线。
    pub async fn force_disconnect(&self, user_id: i32) -> bool {
        let slot = self.sessions.read().await.get(&user_id).cloned();
        if let Some(slot) = slot {
            slot.backpressure.force_close();
            true
        } else {
            false
        }
    }

    /// 注销会话：仅当当前映射仍是本连接（重连后旧连接断开不误删新连接，§4.9-3）。
    pub async fn unregister(&self, user_id: i32, tx: &Arc<mpsc::Sender<Outbound>>) {
        let mut sessions = self.sessions.write().await;
        if sessions
            .get(&user_id)
            .is_some_and(|slot| Arc::ptr_eq(&slot.tx, tx))
        {
            sessions.remove(&user_id);
        }
    }

    /// 向所有在线会话广播一帧（§11 停机维护通知；队列满/已断连则丢弃，ISSUE-0004 同款 try_send）。
    pub async fn broadcast(&self, cmd: ServerCommand) {
        let sessions = self.sessions.read().await;
        for slot in sessions.values() {
            let _ = slot.tx.try_send(Outbound::Command(cmd.clone()));
        }
    }

    /// 在线会话数（§11.1 /healthz 数据源；不含未鉴权连接）。
    pub async fn conn_count(&self) -> usize {
        self.sessions.read().await.len()
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
            if should_send && let Some(slot) = self.sessions.read().await.get(&user_id) {
                // ISSUE-0003 方案 2：热路径（Touches/Judges）按帧 Arc 指针编码一次共享
                // （同一帧的多个 monitor 命中同一缓存）；其余命令写任务各自编码（低频）。
                let out = match &cmd {
                    ServerCommand::Touches { frames, .. } => {
                        let key = Arc::as_ptr(frames) as usize;
                        // ISSUE-0011：把源帧 Arc 克隆传入，miss 时被条目钉住（防 ABA）
                        Outbound::Encoded(self.encode_cache.get_or_encode(
                            key,
                            Box::new(Arc::clone(frames)),
                            || {
                                let mut buf = Vec::new();
                                encode_packet(&cmd, &mut buf);
                                buf
                            },
                        ))
                    }
                    ServerCommand::Judges { judges, .. } => {
                        let key = Arc::as_ptr(judges) as usize;
                        // ISSUE-0011：同 Touches——钉住源 Arc，杜绝地址复用命中死条目
                        Outbound::Encoded(self.encode_cache.get_or_encode(
                            key,
                            Box::new(Arc::clone(judges)),
                            || {
                                let mut buf = Vec::new();
                                encode_packet(&cmd, &mut buf);
                                buf
                            },
                        ))
                    }
                    _ => Outbound::Command(cmd),
                };
                // 安全锁 A：send 队列字节记账（Encoded 热路径大帧——Command 是小帧不记）。
                // 每连接超限 → 踢本连接；全局超限 → 丢新 + 断最重连接。
                if let Outbound::Encoded(bytes) = &out {
                    let len = bytes.len();
                    let conn = slot.queue_bytes.fetch_add(len, Ordering::SeqCst) + len;
                    if conn > PER_CONN_MEM_LIMIT {
                        slot.backpressure.force_close();
                    }
                    if !charge_memory(len) {
                        // 全局超限：丢新（不投递）+ 断最重连接（回收内存）
                        slot.queue_bytes.fetch_sub(len, Ordering::SeqCst);
                        release_memory(len);
                        if let Some((_, bp)) = self.heaviest() {
                            bp.force_close();
                        }
                        continue;
                    }
                }
                // ISSUE-0004 修复：try_send 满则丢新（不阻塞房间投递，§10.4"绝不阻塞房间 actor"）；
                // 满时标记积压，供连接监控任务判定"乌龟"客户端并断连（Backpressure）。
                // 热路径可丢（§4.9-9）：触摸流每帧独立、下一帧自愈；丢新与"同通道保序"相容。
                let encoded_len = match &out {
                    Outbound::Encoded(bytes) => Some(bytes.len()),
                    Outbound::Command(_) => None,
                };
                match slot.tx.try_send(out) {
                    Ok(()) => slot.backpressure.clear(),
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // 队列满未入队：回滚记账
                        if let Some(len) = encoded_len {
                            slot.queue_bytes.fetch_sub(len, Ordering::SeqCst);
                            release_memory(len);
                        }
                        slot.backpressure.mark();
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        // 连接已断（写任务退出）：回滚记账
                        if let Some(len) = encoded_len {
                            slot.queue_bytes.fetch_sub(len, Ordering::SeqCst);
                            release_memory(len);
                        }
                    }
                }
            }
        }
    }
}

/// 未鉴权连接全局上限（§10.4：批量半开连接打满 accept 的闸门）。
const MAX_PENDING_CONNECTIONS: usize = 100;

/// 每 IP 未鉴权连接上限（§10.4：单 IP 打满资源的闸门）。
const MAX_PENDING_PER_IP: usize = 5;

/// 连接准入（§10.4）：未鉴权连接数 + 每 IP 限额——公网"被打"时的第一道闸。
///
/// 计数语义：连接建立时计入，鉴权成功后"转正"（release），连接结束时若仍在册则释放。
#[derive(Default)]
pub struct ConnectionAdmission {
    /// 未鉴权连接总数。
    pending: AtomicUsize,
    /// 每 IP 未鉴权连接数。
    per_ip: Mutex<std::collections::HashMap<std::net::IpAddr, usize>>,
}

impl ConnectionAdmission {
    /// 尝试准入（accept 时调用）。超限 → false（调用方应断开连接）。
    #[must_use]
    pub fn try_acquire(&self, ip: std::net::IpAddr) -> bool {
        // 先查每 IP 限额（lock 内检查 + 增加）
        let mut map = self
            .per_ip
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let n = map.entry(ip).or_default();
        if *n >= MAX_PENDING_PER_IP {
            return false;
        }
        *n += 1;
        drop(map);
        // 再查全局未鉴权数；超限回滚每 IP 计数
        if self.pending.fetch_add(1, Ordering::SeqCst) + 1 > MAX_PENDING_CONNECTIONS {
            self.pending.fetch_sub(1, Ordering::SeqCst);
            let mut map = self
                .per_ip
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(v) = map.get_mut(&ip) {
                if *v > 1 {
                    *v -= 1;
                } else {
                    map.remove(&ip);
                }
            }
            return false;
        }
        true
    }

    /// 释放一条未鉴权连接（鉴权转正 / 连接结束）。
    pub fn release(&self, ip: std::net::IpAddr) {
        self.pending.fetch_sub(1, Ordering::SeqCst);
        let mut map = self
            .per_ip
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(v) = map.get_mut(&ip) {
            if *v > 1 {
                *v -= 1;
            } else {
                map.remove(&ip);
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
    send_tx: Mutex<Option<Arc<mpsc::Sender<Outbound>>>>,
    /// 当前帧上限（§10.4：鉴权前 ~4KiB，鉴权后 2MiB；与 Stream recv 任务共享）。
    packet_limit: Arc<AtomicU32>,
    /// 本连接是否仍计入未鉴权准入（鉴权成功后转正释放，§10.4）。
    admission_held: AtomicBool,
    /// 对端 IP（准入释放用，§10.4）。
    peer_ip: std::net::IpAddr,
    /// 发送积压标记（ISSUE-0004：踢乌龟客户端；与 SessionSink 共享）。
    backpressure: Arc<Backpressure>,
    /// 命令限速器（ISSUE-0006：每连接滥用控制"快端"防线）。
    limiter: CommandLimiter,
    /// send 队列字节记账（安全锁 A：与 SessionSink/SendSlot 共享同一 Arc）。
    queue_bytes: Arc<AtomicUsize>,
}

impl ConnState {
    fn new(peer_ip: std::net::IpAddr) -> Self {
        Self {
            authed: AtomicBool::new(false),
            panicked: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            user_id: AtomicI32::new(0),
            epoch: AtomicU64::new(0),
            last_recv: Mutex::new(Instant::now()),
            send_tx: Mutex::new(None),
            packet_limit: Arc::new(AtomicU32::new(PRE_AUTH_MAX_PACKET)),
            admission_held: AtomicBool::new(false),
            peer_ip,
            backpressure: Arc::new(Backpressure::new()),
            limiter: CommandLimiter::new(),
            queue_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// PROXY 头读取超时（§前置层：半开连接防护，与握手超时同级）。
const PROXY_HEADER_TIMEOUT: Duration = Duration::from_secs(5);

/// 单连接处理（§4.5）：握手 → 心跳 → 鉴权 → 命令派发 → 事件投递 → 断开收尾。
///
/// 前置层顺序（§前置层）：PROXY protocol（反代真实 IP）→ 连接准入 → 版本握手 → …。
/// PROXY 头在准入**之前**解析——准入按真实 IP 计数（反代后每 IP 限额才有效）。
///
/// # Errors
///
/// 握手失败（版本读取失败）/ PROXY 头非法时返回；业务错误走 `warn` 日志（不中断 accept）。
#[allow(clippy::too_many_lines)] // 连接全生命周期（准入/握手/监控/收尾）单一函数完整呈现
pub async fn handle_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    ctx: Arc<ConnContext>,
) -> Result<()> {
    // 前置层 1：PROXY protocol（§前置层，config 开关）——反代后透传真实 IP。
    // 解析失败（直连客户端/头非法/超时）→ 断开（协议错乱比误放行安全）。
    let mut peer_ip = addr.ip();
    if ctx.proxy_protocol {
        let read = proxy::read_proxy_header(&mut stream);
        match tokio::time::timeout(PROXY_HEADER_TIMEOUT, read).await {
            Ok(Ok(Some(hdr))) => peer_ip = hdr.src_ip,
            Ok(Ok(None)) => {} // v2 LOCAL / v1 UNKNOWN：用 socket 地址
            Ok(Err(e)) => {
                warn!("proxy header rejected from {addr}: {e}");
                return Err(e.into());
            }
            Err(_) => return Err(anyhow::anyhow!("proxy header timeout from {addr}")),
        }
    }

    // 连接准入（§10.4）：未鉴权连接上限 + 每 IP 限额——超限直接断开
    // 注：HTTP 管理端点走独立端口（`http_port`），不混入 MP 入口（peek 分流在
    // Windows/current_thread 下不稳定，2026-08 实测 5s 延迟 + 后续卡死）
    if !ctx.admission.try_acquire(peer_ip) {
        return Ok(());
    }
    let state = Arc::new(ConnState::new(peer_ip));
    state.admission_held.store(true, Ordering::SeqCst);

    let handler_ctx = Arc::clone(&ctx);
    let handler_state = Arc::clone(&state);
    let handler = Box::new(
        move |send_tx: Arc<mpsc::Sender<Outbound>>, cmd: ClientCommand| {
            let ctx = Arc::clone(&handler_ctx);
            let state = Arc::clone(&handler_state);
            async move {
                handle_frame(&ctx, &state, &send_tx, cmd).await;
            }
        },
    );

    let stream = Stream::<ClientCommand>::new(
        None,
        stream,
        handler,
        Arc::clone(&state.packet_limit),
        Arc::clone(&state.queue_bytes), // 安全锁 A：写任务消费时经此释放记账
    )
    .await;
    let stream = match stream {
        Ok(s) => s,
        Err(e) => {
            // 握手失败/超时/EOF：收尾代码不会执行——这里显式释放准入（§10.4 防泄漏）
            if state.admission_held.swap(false, Ordering::SeqCst) {
                ctx.admission.release(state.peer_ip);
            }
            return Err(e);
        }
    };

    // D2（技术债）：版本握手校验——客户端版本不匹配立即断开（§6.1 / 对照 gooophira）。
    // 服务端模式 = 读客户端发来的 1 字节版本；v1 协议到来前只认 PROTOCOL_VERSION。
    // 不匹配一般为旧/新客户端，拒绝比容忍安全（避免旧客户端发 v2 帧被误解析）。
    if stream.version() != PROTOCOL_VERSION {
        warn!(
            "protocol version mismatch from {addr}: got {}, want {}",
            stream.version(),
            PROTOCOL_VERSION
        );
        // 校验失败：显式释放准入（§10.4 防泄漏），然后断开连接。
        if state.admission_held.swap(false, Ordering::SeqCst) {
            ctx.admission.release(state.peer_ip);
        }
        return Ok(());
    }

    // 心跳监控（§6.1）：10s 无任何包 → 判断线 → 生命周期 Disconnected + 通知主流程断开。
    // 主流程用 `select!` 同时等待客户端断开与超时信号（避免共享 Stream 的 take 竞争）。
    let monitor_state = Arc::clone(&state);
    let monitor_ctx = Arc::clone(&ctx);
    let (abort_tx, mut abort_rx) = mpsc::channel::<()>(1);
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
            // 超时：标记收尾（CAS 防重复）+ 通知生命周期 + 通知主流程断开
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
            let _ = abort_tx.send(()).await;
            return;
        }
    });

    // 发送积压监控（ISSUE-0004 修复）：队列持续满超阈值 → 判定"乌龟"客户端 → 断开。
    // 与心跳监控独立：心跳只看收包（乌龟持续发 Ping 不触发），积压看"发包被卡"。
    // 标记由 SessionSink::deliver 维护（try_send 满则 mark / 成功则 clear）。
    let kick_state = Arc::clone(&state);
    let kick_ctx = Arc::clone(&ctx);
    let (kick_tx, mut kick_rx) = mpsc::channel::<()>(1);
    let kicker = tokio::spawn(async move {
        loop {
            tokio::time::sleep(BACKPRESSURE_CHECK_INTERVAL).await;
            if kick_state.closed.load(Ordering::SeqCst) {
                return; // 已收尾
            }
            // 安全锁 A：内存守卫强制踢出（每连接超限/全局超限断最重）——不等积压超时
            if kick_state.backpressure.is_forced() {
                if kick_state.closed.swap(true, Ordering::SeqCst) {
                    return;
                }
                let user_id = kick_state.user_id.load(Ordering::SeqCst);
                let epoch = kick_state.epoch.load(Ordering::SeqCst);
                if kick_state.authed.load(Ordering::SeqCst) {
                    warn!("memory guard kicked, user={user_id}");
                    let _ = kick_ctx
                        .fact_tx
                        .send(LifecycleEvent::Disconnected { user_id, epoch })
                        .await;
                }
                let _ = kick_tx.send(()).await;
                return;
            }
            let Some(elapsed) = kick_state.backpressure.elapsed() else {
                continue; // 当前未积压（正常波动已自愈）
            };
            if elapsed < SLOW_CONSUMER_KICK_AFTER {
                continue; // 积压未满阈值
            }
            // 积压持续超阈值：收尾（CAS 防重复）→ 通知生命周期 → 通知主流程断开
            if kick_state.closed.swap(true, Ordering::SeqCst) {
                return;
            }
            let user_id = kick_state.user_id.load(Ordering::SeqCst);
            let epoch = kick_state.epoch.load(Ordering::SeqCst);
            if kick_state.authed.load(Ordering::SeqCst) {
                warn!("slow consumer kicked, user={user_id} (send queue full for {elapsed:?})");
                let _ = kick_ctx
                    .fact_tx
                    .send(LifecycleEvent::Disconnected { user_id, epoch })
                    .await;
            }
            let _ = kick_tx.send(()).await;
            return;
        }
    });

    // 等待连接关闭（客户端断开 / 心跳超时 / 慢消费者踢出）。
    // select：客户端断开 → await_closed 返回；超时/踢出 → abort/kick 分支触发，
    // await_closed future 被 drop（stream drop → abort 收发任务 → 连接断开）。
    tokio::select! {
        // await_closed move stream：客户端断开 → 正常返回；超时 → future drop → stream drop
        () = stream.await_closed() => {}
        () = async { abort_rx.recv().await; } => {}
        () = async { kick_rx.recv().await; } => {}
    }

    // 收尾（幂等：监控超时可能已发 Disconnected）
    if !state.closed.swap(true, Ordering::SeqCst) {
        let user_id = state.user_id.load(Ordering::SeqCst);
        let epoch = state.epoch.load(Ordering::SeqCst);
        if state.authed.load(Ordering::SeqCst) {
            // 安全锁 B：已鉴权连接计数释放（与 authenticate_flow 的 fetch_add 配对）
            AUTHED_CONNECTIONS.fetch_sub(1, Ordering::SeqCst);
            let _ = ctx
                .fact_tx
                .send(LifecycleEvent::Disconnected { user_id, epoch })
                .await;
        }
    }
    // 释放准入（若仍未鉴权转正——鉴权成功已 release，这里幂等兜底）
    if state.admission_held.swap(false, Ordering::SeqCst) {
        ctx.admission.release(state.peer_ip);
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
    kicker.abort();
    info!("connection from {addr} closed");
    Ok(())
}

/// 单帧处理（handler 主体）：心跳 / 鉴权 / 命令派发。
async fn handle_frame(
    ctx: &ConnContext,
    state: &ConnState,
    send_tx: &Arc<mpsc::Sender<Outbound>>,
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
        // ISSUE-0004：Pong 走 try_send（满则丢——客户端心跳自会失败断开；不阻塞 recv 任务）
        let _ = send_tx.try_send(Outbound::Command(ServerCommand::Pong));
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
    // ISSUE-0009 修复（§4.9-3 旧连接失效）：替换后旧 TCP 到达的命令以 epoch 校验拒绝——
    // 重连/顶替后旧连接仍活着，其命令以同一 user_id 混进房间 channel 会破坏顺序语义
    // （同 id 双活 + AlreadyInRoom check-then-act 竞态）。epoch 不匹配 → 拒绝该命令并
    // force_close：借 kicker（1s 轮询）拆掉旧连接、释放其已鉴权名额（与内存守卫踢出同机制）。
    if ctx.registry.current_epoch(user_id) != Some(state.epoch.load(Ordering::SeqCst)) {
        warn!("stale connection rejected, user={user_id} (epoch mismatch)");
        state.backpressure.force_close();
        return;
    }
    // 滥用控制"快端"（ISSUE-0006 修复）：每连接限速只限"贵"命令（资源成本驱动）；
    // 超限回 TooManyRequests Business 错误（客户端可见），不触发队列 Reject 断连
    if let Some((key, interval)) = rate_limit(&cmd)
        && !state.limiter.allow(key, interval)
    {
        let lang = ctx.sink.locale_of(user_id).await;
        let sc = response_to_server(
            &cmd,
            Ok(RoomResponse::Failure(RoomError::Business {
                code: RoomErrorCode::TooManyRequests,
                msg: crate::l10n::Key::TooManyRequests.localized(lang).to_owned(),
            })),
        );
        let _ = send_tx.try_send(Outbound::Command(sc));
        return;
    }
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
    // 热路径（Touches/Judges）只转发给 monitor，不回答发者（§6.5-17）
    let server_cmd = match &cmd {
        ClientCommand::Touches { .. } | ClientCommand::Judges { .. } => None,
        // B2 i18n：Failure 响应按发起者语言本地化（出口点唯一，与 impl 解耦）
        _ => {
            let lang = ctx.sink.locale_of(user_id).await;
            Some(response_to_server(&cmd, localize_failure(resp, lang)))
        }
    };
    if let Some(sc) = server_cmd {
        // ISSUE-0004：命令响应走 try_send——满则丢 + 标记积压（不阻塞本连接 recv 任务；
        // 队列持续满由 kicker 踢出——乌龟不享受无限等待）
        match send_tx.try_send(Outbound::Command(sc)) {
            Ok(()) => state.backpressure.clear(),
            Err(mpsc::error::TrySendError::Full(_)) => state.backpressure.mark(),
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

/// 鉴权流程（§6.5-14/19/23）：回源 /me → 注册 epoch → 恢复房间状态 → 应答。
async fn authenticate_flow(
    ctx: &ConnContext,
    state: &ConnState,
    send_tx: &Arc<mpsc::Sender<Outbound>>,
    token: &str,
) {
    match ctx.auth.authenticate(token).await {
        Ok(identity) => {
            let user_id = identity.user_id;
            // 安全锁 B：已鉴权连接总数上限（§11 兑现）——超限拒绝鉴权 + 等断开
            if AUTHED_CONNECTIONS.fetch_add(1, Ordering::SeqCst) + 1 > MAX_AUTHED_CONNECTIONS {
                AUTHED_CONNECTIONS.fetch_sub(1, Ordering::SeqCst);
                warn!("too many authed connections, rejecting user={user_id}");
                let _ = send_tx
                    .send(Outbound::Command(ServerCommand::Authenticate(Err(
                        "server full".to_owned(),
                    ))))
                    .await;
                state.panicked.store(true, Ordering::SeqCst);
                return;
            }
            let epoch = ctx.registry.register(user_id, identity.name.clone());
            state.user_id.store(user_id, Ordering::SeqCst);
            state.epoch.store(epoch, Ordering::SeqCst);
            *state
                .send_tx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(send_tx));
            ctx.sink
                .register(
                    user_id,
                    Arc::clone(send_tx),
                    Arc::clone(&state.backpressure),
                    Arc::clone(&state.queue_bytes),
                    crate::l10n::Locale::from_lang_str(&identity.lang),
                )
                .await;
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
                .send(Outbound::Command(ServerCommand::Authenticate(Ok((
                    info, room_state,
                )))))
                .await;
            // 进服欢迎语（§运营）：鉴权成功后发给本人（user=0 系统消息，协议兼容）
            if let Some(welcome) = &ctx.welcome_message {
                let _ = send_tx
                    .send(Outbound::Command(ServerCommand::Message(
                        phira_api::Message::Chat {
                            user: 0,
                            content: welcome.clone(),
                        },
                    )))
                    .await;
            }
            state.authed.store(true, Ordering::SeqCst);
            // 鉴权转正：释放未鉴权准入计数（§10.4）
            if state.admission_held.swap(false, Ordering::SeqCst) {
                ctx.admission.release(state.peer_ip);
            }
            // 鉴权通过：帧上限放开到协议上限（§10.4：鉴权前 ~4KiB）
            state.packet_limit.store(MAX_PACKET_SIZE, Ordering::SeqCst);
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
            let _ = send_tx
                .send(Outbound::Command(ServerCommand::Authenticate(Err(msg))))
                .await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use phira_api::TouchFrame;

    /// ISSUE-0011：缓存条目必须持有源 `Arc` 克隆钉住地址（防分配器复用 → ABA）。
    ///
    /// miss 时缓存 `_pin` 持有一份 `Arc::clone(&frames)`，因此源 `Arc` 强引用数 = 2
    /// （外层测试变量 + 缓存条目）——证明地址被钉住，不可能释放/复用。
    #[test]
    fn encode_cache_pins_source_arc() {
        let cache = EncodeCache::new(64);
        let frames = Arc::new(vec![TouchFrame {
            time: 1.0,
            points: vec![],
        }]);
        let addr = Arc::as_ptr(&frames) as usize;
        let _ = cache.get_or_encode(addr, Box::new(Arc::clone(&frames)), || vec![0xAA]);
        assert_eq!(Arc::strong_count(&frames), 2);
    }

    /// 同 key 命中：返回缓存字节（一次编码共享），pin 在 hit 时被丢弃（计数回到外层）。
    #[test]
    fn encode_cache_hit_returns_cached_bytes() {
        let cache = EncodeCache::new(64);
        let frames = Arc::new(vec![TouchFrame {
            time: 1.0,
            points: vec![],
        }]);
        let addr = Arc::as_ptr(&frames) as usize;
        let first = cache.get_or_encode(addr, Box::new(Arc::clone(&frames)), || vec![0xAA, 0xBB]);
        let second = cache.get_or_encode(addr, Box::new(Arc::new(())), || vec![0xCC]);
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(&*second, &[0xAA, 0xBB]);
    }

    /// 不同 key 互不污染：两批不同内容走不同条目，各自编码结果独立。
    #[test]
    fn encode_cache_distinct_keys_isolate() {
        let cache = EncodeCache::new(64);
        let a = Arc::new(vec![1u8]);
        let b = Arc::new(vec![2u8]);
        let ka = Arc::as_ptr(&a) as usize;
        let kb = Arc::as_ptr(&b) as usize;
        let va = cache.get_or_encode(ka, Box::new(Arc::clone(&a)), || vec![0xA1]);
        let vb = cache.get_or_encode(kb, Box::new(Arc::clone(&b)), || vec![0xB2]);
        assert_eq!(&*va, &[0xA1]);
        assert_eq!(&*vb, &[0xB2]);
    }

    /// ISSUE-0011 回归核心：地址复用模拟——两条内容不同、但构造出同 key 的源，
    /// 第二次投递必须命中第一次的缓存（被钉住），而不是取到历史/新编码。
    ///
    /// 注：无法在测试里确定性诱导分配器真实复用地址，这里用「同 addr 人工复现」
    /// 验证缓存正确性（命中旧条目则返回旧字节，绝不重编）。真正防复用靠 `_pin` 钉住。
    #[test]
    fn encode_cache_same_addr_reuses_pinned_entry() {
        let cache = EncodeCache::new(64);
        let frames = Arc::new(vec![TouchFrame {
            time: 1.0,
            points: vec![],
        }]);
        let addr = Arc::as_ptr(&frames) as usize;
        let first = cache.get_or_encode(addr, Box::new(Arc::clone(&frames)), || vec![0x11]);
        // 同地址（模拟分配器复用），但内容不同：应命中缓存返回旧字节，而非重新编码
        let second = cache.get_or_encode(addr, Box::new(Arc::clone(&frames)), || vec![0x22]);
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(&*second, &[0x11]);
    }

    /// D1（技术债）：Chat 加入限速白名单（2/s），热路径 Touches/Judges 仍不限。
    #[test]
    fn rate_limit_covers_chat_but_not_hot_path() {
        let chat = ClientCommand::Chat {
            message: phira_api::Varchar::new("hi".to_owned()).unwrap(),
        };
        // Chat 受限：键 chat，间隔 500ms（2/s）
        let (key, interval) = rate_limit(&chat).expect("Chat 应入限速白名单");
        assert_eq!(key, "chat");
        assert_eq!(interval, Duration::from_millis(500));

        // 热路径 Touches/Judges 不受限（靠 DropIfFull + 帧上限兜底）
        let touches = ClientCommand::Touches {
            frames: Arc::new(vec![]),
        };
        let judges = ClientCommand::Judges {
            judges: Arc::new(vec![]),
        };
        assert!(rate_limit(&touches).is_none(), "Touches 不应限速");
        assert!(rate_limit(&judges).is_none(), "Judges 不应限速");

        // 现有受限命令仍覆盖
        assert!(rate_limit(&ClientCommand::Ping).is_none(), "Ping 不限速");
        assert!(rate_limit(&ClientCommand::Ready).is_none(), "Ready 不限速");
    }
}
