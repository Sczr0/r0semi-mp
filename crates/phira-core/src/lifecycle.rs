//! 用户生命周期（§4.9-3）：会话注册表 + 单一生产者生命周期任务。
//!
//! ## 角色分工
//!
//! - **server（连接层）**：鉴权成功后 `registry.register(user_id)` 分配 epoch，
//!   发 `LifecycleEvent::Connected`；连接结束发 `Disconnected`。
//! - **core（本模块）**：消费事实（**单一生产者**），按序派发
//!   `RoomCommand::UserDisconnected` / `UserReconnected` / `UserDangleExpired`。
//!
//! ## 三条不变量（§4.9-3）
//!
//! 1. **会话纪元**：`register` 每次 +1 并替换；事实携带 epoch，`is_current` 校验——
//!    旧会话的死亡事实（`Disconnected`）与过期定时器一律忽略。
//! 2. **窗口边界**：10s 定时器到期**先查注册表再派发** `UserDangleExpired`——
//!    重连通知的入队序 ≠ 墙钟序，盲发会踢掉刚重连的用户。
//! 3. **单一生产者**：`DangleExpired` 由内部定时器**投回本队列**再处理，
//!    不直接从定时器回调派发——保证与重连事实严格按入队序串行。
//!
//! 红线程：phira-core 禁 unwrap/expect（柜台不 panic）；本模块只依赖 std + tokio。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use phira_api::{CmdCtx, Origin, RoomCommand, RoomId};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::bus::Bus;

/// 路由表 miss 重放参数（ISSUE-0001 修复，§4.9-3 第四竞态·幽灵座位）。
///
/// 入房时序：actor 返回 UserJoined → bus 应用路由增量 → 发响应；客户端入房后立即断线
/// （RST 即时可见）时，生命周期任务查表可能撞上"增量未应用"窗口（bus 忙时拉大）——
/// 立即丢事实会留下幽灵座位（无 dangle 窗口、无人驱逐、占坑）。
/// 挂起重放：短暂延迟后重查（process_events 的 await 点远小于 20ms），仍 miss 才放弃。
/// 正常路径（用户确实不在房间）代价 = 最多 2×20ms 一次性延迟（后台任务，可接受）。
const ROUTE_REPLAY_ATTEMPTS: usize = 3;
const ROUTE_REPLAY_DELAY: Duration = Duration::from_millis(20);

/// 会话注册表：`user_id → (当前会话纪元, 昵称)`（§4.9-3）。
///
/// 独立于任务（server 侧 `register` 与任务侧 `is_current` 并发访问）——
/// 用 `std::sync::Mutex`（临界区极短，无 await）。
/// 昵称存这里：`CreateRoom`/`JoinRoom` 派发时需要（§6.6 表 2），
/// 避免 impl 猜名字 / core 另持影子状态。
pub struct SessionRegistry {
    inner: Mutex<HashMap<i32, (u64, String)>>,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl SessionRegistry {
    /// 新注册表。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 用户连接建立：分配新纪元（旧纪元 + 1）并替换，记录昵称。
    ///
    /// 同 id 重连 = 再次调用 → epoch+1（§6.5-19 替换会话语义）。
    /// 由 server 鉴权成功后调用。
    #[must_use]
    pub fn register(&self, user_id: i32, name: String) -> u64 {
        let mut m = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let epoch = m.get(&user_id).map_or(0, |(e, _)| *e) + 1;
        m.insert(user_id, (epoch, name));
        epoch
    }

    /// 用户当前纪元（客户端命令 epoch 校验读入口，ISSUE-0009：旧连接命令拒绝）。
    ///
    /// `None` = 从未注册（理论不可达——活着且已鉴权的连接必然已 `register`）。
    #[must_use]
    pub fn current_epoch(&self, user_id: i32) -> Option<u64> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&user_id)
            .map(|(e, _)| *e)
    }

    /// 用户当前昵称（CreateRoom/JoinRoom 派发填充，§6.6 表 2）。
    pub fn name_of(&self, user_id: i32) -> Option<String> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&user_id)
            .map(|(_, n)| n.clone())
    }

    /// `epoch` 是否为该用户当前纪元（事实/定时器有效性校验，§4.9-3）。
    fn is_current(&self, user_id: i32, epoch: u64) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&user_id)
            .is_some_and(|(e, _)| *e == epoch)
    }
}

/// 生命周期事件（单一生产者队列载荷）。
///
/// 前三个变体由 server 发送（§4.9-3）；[`LifecycleEvent::DangleExpired`] 仅供
/// 内部定时器投回，server 不应使用（幂等无害）。
#[derive(Debug)]
pub enum LifecycleEvent {
    /// 连接建立（鉴权通过）。`epoch` = `SessionRegistry::register` 返回值。
    Connected { user_id: i32, epoch: u64 },
    /// 连接断开（心跳 10s 无包 / TCP 关闭，§6.1）。
    Disconnected { user_id: i32, epoch: u64 },
    /// 重连（同 id 再次鉴权；与 Connected 同效果，显式语义）。
    Reconnected { user_id: i32, epoch: u64 },
    /// 重连窗口到期（内部定时器投回，§4.9-3 单一生产者）。
    DangleExpired { user_id: i32, epoch: u64 },
}

/// 用户生命周期任务（§4.9-3）：单一生产者消费循环。
pub struct LifecycleTask {
    bus: Bus,
    registry: Arc<SessionRegistry>,
    rx: mpsc::Receiver<LifecycleEvent>,
    /// 内部定时器投回用（sender 可 clone，run 结束后任务退出）。
    event_tx: mpsc::Sender<LifecycleEvent>,
    /// 断线到驱逐的窗口（默认 10s，§6.1/§6.5-21；测试注入更小值）。
    dangle_window: Duration,
}

impl LifecycleTask {
    /// 构造任务。返回 `(task, registry, fact_tx)`：
    /// - `registry` 交给 server（鉴权成功后 `register` 分配 epoch）
    /// - `fact_tx` 交给 server（每连接发 Connected/Disconnected）
    ///
    /// `dangle_window` = 重连窗口（生产 10s；测试可注入）。
    #[must_use]
    pub fn new(
        bus: Bus,
        dangle_window: Duration,
    ) -> (Self, Arc<SessionRegistry>, mpsc::Sender<LifecycleEvent>) {
        let (event_tx, rx) = mpsc::channel(64);
        let registry = Arc::new(SessionRegistry::new());
        (
            Self {
                bus,
                registry: Arc::clone(&registry),
                rx,
                event_tx: event_tx.clone(),
                dangle_window,
            },
            registry,
            event_tx.clone(),
        )
    }

    /// 消费循环（组合根 spawn）。channel 关闭（server 全部断开）时自然退出。
    pub async fn run(mut self) {
        while let Some(event) = self.rx.recv().await {
            self.handle(event).await;
        }
        debug!("lifecycle task exiting");
    }

    async fn handle(&mut self, event: LifecycleEvent) {
        match event {
            LifecycleEvent::Connected { user_id, epoch } => {
                // 窗口内重连：恢复座位（impl 移除缺席标记，§6.5-21）
                self.dispatch(user_id, RoomCommand::UserReconnected { user_id, epoch })
                    .await;
            }
            LifecycleEvent::Reconnected { user_id, epoch } => {
                self.dispatch(user_id, RoomCommand::UserReconnected { user_id, epoch })
                    .await;
            }
            LifecycleEvent::Disconnected { user_id, epoch } => {
                // 旧会话死亡事实（epoch 不匹配）→ 忽略（§4.9-3）
                if !self.registry.is_current(user_id, epoch) {
                    debug!("ignoring stale disconnect user={user_id} epoch={epoch}");
                    return;
                }
                // 标记缺席（impl），并启动 10s 窗口
                self.dispatch(user_id, RoomCommand::UserDisconnected { user_id, epoch })
                    .await;
                let tx = self.event_tx.clone();
                let window = self.dangle_window;
                tokio::spawn(async move {
                    tokio::time::sleep(window).await;
                    // 到期：投回单一生产者队列（不直接派发，§4.9-3 窗口边界）
                    let _ = tx
                        .send(LifecycleEvent::DangleExpired { user_id, epoch })
                        .await;
                });
            }
            LifecycleEvent::DangleExpired { user_id, epoch } => {
                // 窗口边界：先查权威会话状态（§4.9-3）——已重连则忽略
                if !self.registry.is_current(user_id, epoch) {
                    debug!("dangle expired but user={user_id} reconnected, skipping");
                    return;
                }
                self.dispatch(user_id, RoomCommand::UserDangleExpired { user_id })
                    .await;
            }
        }
    }

    /// 向用户所在房间派发系统命令；重放后仍 miss（不在房/已关）→ 忽略。
    async fn dispatch(&self, user_id: i32, cmd: RoomCommand) {
        let Some(room_id) = self.lookup_room_with_replay(user_id).await else {
            debug!("user={user_id} not in any room after replay, dropping lifecycle fact");
            return;
        };
        let ctx = CmdCtx {
            origin: Origin::System,
            room_id,
        };
        if let Err(err) = self.bus.dispatch_system(ctx.room_id, cmd).await {
            warn!("lifecycle dispatch failed for user={user_id}: {err:?}");
        }
    }

    /// 路由表查询 + 幽灵座位重放（ISSUE-0001 修复，§4.9-3 第四竞态）：
    /// 表 miss 时挂起重放（短暂延迟重查），覆盖"join 增量未应用"窗口；
    /// 仍 miss 才放弃（用户确实不在房间）。
    async fn lookup_room_with_replay(&self, user_id: i32) -> Option<RoomId> {
        for attempt in 0..ROUTE_REPLAY_ATTEMPTS {
            if let Some(rid) = self.bus.room_of(user_id).await {
                return Some(rid);
            }
            if attempt + 1 < ROUTE_REPLAY_ATTEMPTS {
                tokio::time::sleep(ROUTE_REPLAY_DELAY).await;
            }
        }
        None
    }
}
