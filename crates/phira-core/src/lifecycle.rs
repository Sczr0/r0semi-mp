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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

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
///
/// ISSUE-0012（方案 A）：拆成两张表——`epochs`（**永不删除**，8B/用户，留作单调纪元
/// 不变量）与 `names`（可淘汰，仅在用户彻底离线时移除）。昵称只在
/// `CreateRoom`/`JoinRoom` 派发时需要（§6.6 表 2），那时用户必然已重新鉴权注入 name；
/// 而 epoch 必须单调递增不回收，否则重连回退可能撞上遗留僵尸连接复活 ISSUE-0009。
/// epochs 锁探针（performance-cpu.md §锁竞争矩阵）：`R0SEMI_EPOCHS_PROBE=1` 启用——
/// 计数 epochs 锁调用次数与慢锁（>50µs 等待）次数/总等待，定位锁竞争真源（低频调用
/// 长等待 vs 隐藏高频调用）。默认关闭（零成本：仅一次 bool 分支）。
#[derive(Default)]
struct SessionProbe {
    enabled: bool,
    calls: AtomicU64,
    slow_50us: AtomicU64,
    wait_ns: AtomicU64,
}

impl SessionProbe {
    fn new() -> Self {
        Self {
            enabled: std::env::var("R0SEMI_EPOCHS_PROBE").is_ok_and(|v| v == "1"),
            ..Self::default()
        }
    }

    /// 取锁并计时（关闭时退化为普通 lock）。
    fn lock_epochs<'a>(
        &self,
        lock: &'a Mutex<HashMap<i32, u64>>,
    ) -> MutexGuard<'a, HashMap<i32, u64>> {
        let start = Instant::now();
        let guard = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.enabled {
            let el = start.elapsed();
            self.calls.fetch_add(1, Ordering::Relaxed);
            if el > Duration::from_micros(50) {
                self.slow_50us.fetch_add(1, Ordering::Relaxed);
            }
            self.wait_ns.fetch_add(
                u64::try_from(el.as_nanos()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        guard
    }
}

pub struct SessionRegistry {
    /// 用户当前纪元（单调递增，永不删除）：`user_id → epoch`。
    epochs: Mutex<HashMap<i32, u64>>,
    /// 用户昵称（可淘汰）：`user_id → name`；离线超时后移除，重连时由 authenticate 重注。
    names: Mutex<HashMap<i32, String>>,
    /// 锁诊断探针（默认关闭；bench 侧 `probe_snapshot` 取数）。
    probe: SessionProbe,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self {
            epochs: Mutex::new(HashMap::new()),
            names: Mutex::new(HashMap::new()),
            probe: SessionProbe::new(),
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
    /// 由 server 鉴权成功后调用。epoch 单调保留（不删除）；name 注入（覆盖旧值）。
    #[must_use]
    pub fn register(&self, user_id: i32, name: String) -> u64 {
        let mut epochs = self.probe.lock_epochs(&self.epochs);
        let epoch = epochs.get(&user_id).copied().unwrap_or(0) + 1;
        epochs.insert(user_id, epoch);
        // name 独立表：覆盖注入（旧的若已被淘汰则重建，未淘汰则替换）
        self.names
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(user_id, name);
        epoch
    }

    /// 用户当前纪元（客户端命令 epoch 校验读入口，ISSUE-0009：旧连接命令拒绝）。
    ///
    /// `None` = 从未注册（理论不可达——活着且已鉴权的连接必然已 `register`）。
    #[must_use]
    pub fn current_epoch(&self, user_id: i32) -> Option<u64> {
        self.probe.lock_epochs(&self.epochs).get(&user_id).copied()
    }

    /// 探针快照（`(调用次数, >50µs 慢锁次数, 总等待 µs)`；未启用 = None）。
    ///
    /// 供 bench/诊断取数（R0SEMI_EPOCHS_PROBE=1 运行后打印，performance-cpu.md §锁矩阵）。
    #[must_use]
    pub fn probe_snapshot(&self) -> Option<(u64, u64, u64)> {
        if !self.probe.enabled {
            return None;
        }
        Some((
            self.probe.calls.load(Ordering::Relaxed),
            self.probe.slow_50us.load(Ordering::Relaxed),
            self.probe.wait_ns.load(Ordering::Relaxed) / 1000,
        ))
    }

    /// 用户当前昵称（CreateRoom/JoinRoom 派发填充，§6.6 表 2）。
    ///
    /// `None` = 昵称已被淘汰（用户离线超时）——`impl` 侧用 `unwrap_or_default()` 兜底；
    /// 但需要昵称的 CreateRoom/JoinRoom 派发只会发生在在线会话上，届时 name 已注入。
    pub fn name_of(&self, user_id: i32) -> Option<String> {
        self.names
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&user_id)
            .cloned()
    }

    /// `epoch` 是否为该用户当前纪元（事实/定时器有效性校验，§4.9-3）。
    fn is_current(&self, user_id: i32, epoch: u64) -> bool {
        self.current_epoch(user_id) == Some(epoch)
    }

    /// 淘汰昵称（ISSUE-0012：用户彻底离线且不在任何房间时调用）。
    ///
    /// 只删 `names` 表（释放字符串驻留），**不触碰 `epochs`**——epoch 必须单调保留，
    /// 否则重连回退会撞上遗留僵尸连接复活 ISSUE-0009。
    pub fn evict_name(&self, user_id: i32) {
        self.names
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&user_id);
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
    /// 周期 `Tick` 心跳间隔（B1/B6 通电：倒计时 + 观战聚合 flush 节拍；生产 50ms）。
    tick_interval: Duration,
}

impl LifecycleTask {
    /// 构造任务。返回 `(task, registry, fact_tx)`：
    /// - `registry` 交给 server（鉴权成功后 `register` 分配 epoch）
    /// - `fact_tx` 交给 server（每连接发 Connected/Disconnected）
    ///
    /// `dangle_window` = 重连窗口（生产 10s；测试可注入）；
    /// `tick_interval` = `Tick` 心跳周期（生产 50ms，对齐 gooophira 慢档刷新窗口；测试可调大减少噪声或调小加速观察）。
    #[must_use]
    pub fn new(
        bus: Bus,
        dangle_window: Duration,
        tick_interval: Duration,
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
                tick_interval,
            },
            registry,
            event_tx.clone(),
        )
    }

    /// 消费循环（组合根 spawn）。channel 关闭（server 全部断开）时自然退出。
    ///
    /// B1/B6 通电（2026-08）：select 上周期 `Tick` 心跳——对全部活跃房间广播
    /// `RoomCommand::Tick{now}`，作为 impl 内唯一时钟源（§4.9-6：时间事实命令化），
    /// 驱动 WaitForReady 倒计时与观战聚合缓冲 flush。选 [`MissedTickBehavior::Skip`]
    /// 不追帧：心跳只是节拍器，落后就跳过下一拍（积压无意义）。
    pub async fn run(mut self) {
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + self.tick_interval, // interval 首拍立即完成，跳过它
            self.tick_interval,
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                event = self.rx.recv() => {
                    let Some(event) = event else { break };
                    self.handle(event).await;
                }
                _ = ticker.tick() => self.broadcast_tick().await,
            }
        }
        debug!("lifecycle task exiting");
    }

    /// 心跳拍：向全部活跃房间广播 `Tick{now}`。空房间不收（无人可计时）；
    /// 房间队列满时 Tick 按 DropIfFull 丢弃（§4.9-9：可丢节拍，丢一拍自愈）。
    async fn broadcast_tick(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        for room_id in self.bus.active_rooms().await {
            if let Err(err) = self
                .bus
                .dispatch_system(room_id, RoomCommand::Tick { now })
                .await
            {
                debug!("tick dispatch failed (room likely closing): {err:?}");
            }
        }
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
                // ISSUE-0012：用户彻底离线（重连窗口到期）→ 淘汰昵称（释放字符串驻留），
                // 但保留下 epochs（单调不变量，防 ISSUE-0009 复活）。
                self.registry.evict_name(user_id);
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
