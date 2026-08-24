//! # impl-rooms-v1 —— 第一个货物（§4.1 / §6.5）
//!
//! 房间实现，照原版 phira-mp room.rs 语义（§6.5 规则清单逐条兑现）。
//! 只认识 phira-api，连 core 都不许认识（§4.3-3）——时间与连接事实全部经薄缝命令进入（§4.6）：
//! - `Tick` 推进玩法计时（v1 无玩法倒计时，占位）
//! - `UserDisconnected` 标记缺席 / Playing 中断线立即驱逐（规则 22）
//! - `UserReconnected` 恢复座位（规则 21）
//! - `UserDangleExpired` 执行驱逐（规则 21）
//!
//! 无锁：每房间一个 actor 实例，`&mut self` 独占状态（§4.9）。
#![allow(clippy::needless_pass_by_value)] // ctx: CmdCtx 传值是分发模式的契约形状（§4.4），非误用

use std::collections::{HashMap, HashSet};

use phira_api::{
    ApiError, Chart, ClientRoomState, CmdCtx, JoinRoomResponse, Origin, Record, RoomActor,
    RoomCommand, RoomConfig, RoomDeps, RoomError, RoomErrorCode, RoomEvent, RoomFactory, RoomId,
    RoomResponse, RoomState, Targets, UserInfo,
};

/// 房间容量：玩家上限 8 人（§6.5-1）；monitor 不占名额、不限数量。
const ROOM_MAX_USERS: usize = 8;

/// 房间内部状态（原版 InternalRoomState 语义，§6.4）。
#[derive(Debug)]
enum InternalState {
    /// 选图阶段（§6.4：SelectChart 状态才可加入，§6.5-3）。
    SelectChart,
    /// 等待全员准备（host 默认已 ready，§6.5-7）。
    WaitForReady {
        /// 已 ready 的用户集（玩家 + monitor）。
        started: HashSet<i32>,
    },
    /// 游玩中。
    Playing {
        /// 已上报成绩的玩家。
        results: HashMap<i32, Record>,
        /// 已 abort 的玩家。
        aborted: HashSet<i32>,
    },
}

impl InternalState {
    const fn to_client(&self, chart: Option<i32>) -> RoomState {
        match self {
            Self::SelectChart => RoomState::SelectChart(chart),
            Self::WaitForReady { .. } => RoomState::WaitingForReady,
            Self::Playing { .. } => RoomState::Playing,
        }
    }
}

/// 房间实现（每房间一个实例）。
pub struct RoomV1 {
    id: RoomId,
    deps: RoomDeps,
    config: RoomConfig,
    state: InternalState,
    host: Option<i32>,
    locked: bool,
    cycle: bool,
    live: bool,
    users: HashMap<i32, UserInfo>,
    monitors: HashMap<i32, UserInfo>,
    /// 玩家加入顺序（原版 Room.users 是 Vec——cycle 顺延依赖加入顺序，§6.5-11）。
    user_order: Vec<i32>,
    /// 断线未驱逐的玩家（§4.6：impl 只记状态，计时归 core 生命周期任务）。
    absent: HashSet<i32>,
    chart: Option<Chart>,
}

impl RoomV1 {
    fn new(id: RoomId, config: RoomConfig, deps: RoomDeps) -> Self {
        Self {
            id,
            deps,
            config,
            state: InternalState::SelectChart,
            host: None,
            locked: false,
            cycle: false,
            live: false,
            users: HashMap::new(),
            monitors: HashMap::new(),
            user_order: Vec::new(),
            absent: HashSet::new(),
            chart: None,
        }
    }

    // —— 辅助 ——

    fn in_room(&self, user_id: i32) -> bool {
        self.users.contains_key(&user_id) || self.monitors.contains_key(&user_id)
    }

    fn is_host(&self, user_id: i32) -> bool {
        self.host == Some(user_id)
    }

    fn check_host(&self, user_id: i32) -> Result<(), RoomError> {
        if self.is_host(user_id) {
            Ok(())
        } else {
            Err(RoomError::Business {
                code: RoomErrorCode::OnlyHost,
                msg: "only host can do this".to_owned(),
            })
        }
    }

    fn client_room_state(&self, _user_id: i32) -> RoomState {
        self.state.to_client(self.chart.as_ref().map(|c| c.id))
    }

    fn to_client_state(&self, user_id: i32) -> ClientRoomState {
        let is_ready = matches!(
            &self.state,
            InternalState::WaitForReady { started } if started.contains(&user_id)
        );
        let users: HashMap<i32, UserInfo> = self
            .users
            .iter()
            .chain(self.monitors.iter())
            .map(|(id, info)| (*id, info.clone()))
            .collect();
        ClientRoomState {
            id: self.id.clone(),
            state: self.client_room_state(user_id),
            live: self.live,
            locked: self.locked,
            cycle: self.cycle,
            is_host: self.is_host(user_id),
            is_ready,
            users,
        }
    }

    /// 房主迁移（规则 5：随机指定新 host；cycle 时顺延下一位，§6.5-11）。
    /// `cycle_rotate` = 结算顺延（依赖加入顺序）；false = 随机（原版 on_user_leave）。
    /// 返回 NewHost 事件（old_host 由调用方保证已记录）。
    fn migrate_host(&mut self, old_host: i32, cycle_rotate: bool) -> Vec<RoomEvent> {
        let new_host = if cycle_rotate {
            // 顺延下一位（原版 check_all_ready cycle 分支：position + 1 mod len）
            let idx = self
                .user_order
                .iter()
                .position(|id| *id == old_host)
                .map(|i| (i + 1) % self.user_order.len())
                .unwrap_or_default();
            self.user_order.get(idx).copied()
        } else {
            // 随机选择（RNG 注入，§4.9-6 可测）
            self.deps
                .rng
                .pick_index(self.user_order.len())
                .and_then(|i| self.user_order.get(i).copied())
        };
        if let Some(new_host) = new_host {
            self.host = Some(new_host);
            vec![RoomEvent::NewHost {
                room_id: self.id.clone(),
                new_host,
                old_host,
            }]
        } else {
            self.host = None;
            Vec::new()
        }
    }

    /// 驱逐/离开的统一收尾（规则 5/6/21）：广播 UserLeft + 移除 + host 迁移 + 空房判定。
    /// 返回 true = 房间应自毁（RoomClosed 已产出）。
    fn evict(&mut self, user_id: i32) -> Vec<RoomEvent> {
        // 名字在 remove 前取（玩家或 monitor；广播 LeaveRoom 需要，§6.6 表 2）
        let name = self
            .users
            .get(&user_id)
            .or_else(|| self.monitors.get(&user_id))
            .map(|u| u.name.clone())
            .unwrap_or_default();
        let mut events = vec![RoomEvent::UserLeft {
            room_id: self.id.clone(),
            user: user_id,
            name,
        }];
        self.users.remove(&user_id);
        self.monitors.remove(&user_id);
        self.user_order.retain(|id| *id != user_id);
        self.absent.remove(&user_id);

        if self.is_host(user_id) {
            events.extend(self.migrate_host(user_id, false));
        }
        if self.users.is_empty() {
            // 空房自毁（§6.5-6 / §4.9-9）：monitors 不阻止销毁（原版语义）
            events.push(RoomEvent::RoomClosed {
                room_id: self.id.clone(),
            });
        } else {
            events.extend(self.check_all_ready());
        }
        events
    }

    /// 检查开局/结算（原版 check_all_ready，规则 8/11）。
    fn check_all_ready(&mut self) -> Vec<RoomEvent> {
        match &self.state {
            InternalState::WaitForReady { started } => {
                let all = self
                    .users
                    .keys()
                    .chain(self.monitors.keys())
                    .all(|id| started.contains(id));
                if all {
                    // 全员 ready → StartPlaying（规则 8）
                    let events = vec![RoomEvent::StartPlaying {
                        room_id: self.id.clone(),
                    }];
                    self.state = InternalState::Playing {
                        results: HashMap::new(),
                        aborted: HashSet::new(),
                    };
                    events
                } else {
                    Vec::new()
                }
            }
            InternalState::Playing { results, aborted } => {
                let all = self
                    .users
                    .keys()
                    .all(|id| results.contains_key(id) || aborted.contains(id));
                if all {
                    // 全员完成/abort → GameEnd（规则 11）
                    let mut events = vec![RoomEvent::GameEnd {
                        room_id: self.id.clone(),
                        chart: self.chart.as_ref().map(|c| c.id),
                    }];
                    let old_host = self.host;
                    self.state = InternalState::SelectChart;
                    if self.cycle
                        && let Some(old) = old_host
                    {
                        events.extend(self.migrate_host(old, true));
                    }
                    events
                } else {
                    Vec::new()
                }
            }
            InternalState::SelectChart => Vec::new(),
        }
    }

    /// Playing 中断线：立即驱逐（规则 22，无重连窗口）。
    fn on_playing_disconnect(&mut self, user_id: i32) -> Vec<RoomEvent> {
        self.evict(user_id)
    }

    // —— 命令处理 ——

    fn handle_create(
        &mut self,
        ctx: CmdCtx,
        cmd: RoomCommand,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        let RoomCommand::CreateRoom { name, .. } = cmd else {
            unreachable!("handle_create 只收 CreateRoom")
        };
        self.host = Some(user_id);
        self.users.insert(
            user_id,
            UserInfo {
                id: user_id,
                name,
                monitor: false,
            },
        );
        self.user_order.push(user_id);
        (
            Some(RoomResponse::Ok),
            vec![RoomEvent::RoomCreated {
                room_id: self.id.clone(),
                host: user_id,
            }],
        )
    }

    fn handle_join(
        &mut self,
        ctx: CmdCtx,
        cmd: RoomCommand,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        let RoomCommand::JoinRoom { monitor, name, .. } = cmd else {
            unreachable!("handle_join 只收 JoinRoom")
        };
        let result = (|| -> Result<(), RoomError> {
            if self.in_room(user_id) {
                return Err(RoomError::Business {
                    code: RoomErrorCode::AlreadyInRoom,
                    msg: "already in room".to_owned(),
                });
            }
            if self.locked {
                return Err(RoomError::Business {
                    code: RoomErrorCode::RoomLocked,
                    msg: "room is locked".to_owned(),
                });
            }
            if !matches!(self.state, InternalState::SelectChart) {
                return Err(RoomError::Business {
                    code: RoomErrorCode::GameOngoing,
                    msg: "game is ongoing".to_owned(),
                });
            }
            if monitor && !self.config.monitors.contains(&user_id) {
                return Err(RoomError::Business {
                    code: RoomErrorCode::CannotMonitor,
                    msg: "no monitor permission".to_owned(),
                });
            }
            if !monitor && self.users.len() >= ROOM_MAX_USERS {
                return Err(RoomError::Business {
                    code: RoomErrorCode::RoomFull,
                    msg: "room is full".to_owned(),
                });
            }
            Ok(())
        })();
        if let Err(err) = result {
            return (Some(RoomResponse::Failure(err)), Vec::new());
        }

        let info = UserInfo {
            id: user_id,
            name,
            monitor,
        };
        if monitor {
            self.monitors.insert(user_id, info.clone());
            if !self.live {
                self.live = true; // monitor 加入 → live（§6.5-4）
            }
        } else {
            self.users.insert(user_id, info.clone());
            self.user_order.push(user_id);
        }
        let resp = RoomResponse::JoinRoom(JoinRoomResponse {
            state: self.client_room_state(user_id),
            users: self
                .users
                .values()
                .chain(self.monitors.values())
                .cloned()
                .collect(),
            live: self.live,
        });
        (
            Some(resp),
            vec![RoomEvent::UserJoined {
                room_id: self.id.clone(),
                user: info,
            }],
        )
    }

    fn handle_leave(&mut self, ctx: CmdCtx) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        if !self.in_room(user_id) {
            return (
                Some(RoomResponse::Failure(RoomError::Business {
                    code: RoomErrorCode::NotInRoom,
                    msg: "not in room".to_owned(),
                })),
                Vec::new(),
            );
        }
        (Some(RoomResponse::Ok), self.evict(user_id))
    }

    fn handle_chat(
        &mut self,
        ctx: CmdCtx,
        message: String,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        if !self.in_room(user_id) {
            return (
                Some(RoomResponse::Failure(RoomError::Business {
                    code: RoomErrorCode::NotInRoom,
                    msg: "not in room".to_owned(),
                })),
                Vec::new(),
            );
        }
        (
            Some(RoomResponse::Ok),
            vec![RoomEvent::Chat {
                room_id: self.id.clone(),
                user: user_id,
                content: message,
            }],
        )
    }

    async fn handle_select_chart(
        &mut self,
        ctx: CmdCtx,
        id: i32,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        let result = (|| -> Result<(), RoomError> {
            if !matches!(self.state, InternalState::SelectChart) {
                return Err(RoomError::Business {
                    code: RoomErrorCode::InvalidState,
                    msg: "invalid state".to_owned(),
                });
            }
            self.check_host(user_id)?;
            Ok(())
        })();
        if let Err(err) = result {
            return (Some(RoomResponse::Failure(err)), Vec::new());
        }
        match self.deps.api.fetch_chart(id).await {
            Ok(chart) => {
                let name = chart.name.clone();
                self.chart = Some(chart);
                (
                    Some(RoomResponse::Ok),
                    vec![RoomEvent::SelectChart {
                        room_id: self.id.clone(),
                        user: user_id,
                        name,
                        id,
                    }],
                )
            }
            Err(ApiError::Internal { msg }) => (
                Some(RoomResponse::Failure(RoomError::Internal { msg })),
                Vec::new(),
            ),
        }
    }

    fn handle_request_start(&mut self, ctx: CmdCtx) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        let result = (|| -> Result<(), RoomError> {
            if !matches!(self.state, InternalState::SelectChart) {
                return Err(RoomError::Business {
                    code: RoomErrorCode::InvalidState,
                    msg: "invalid state".to_owned(),
                });
            }
            self.check_host(user_id)?;
            if self.chart.is_none() {
                return Err(RoomError::Business {
                    code: RoomErrorCode::NoChartSelected,
                    msg: "no chart selected".to_owned(),
                });
            }
            Ok(())
        })();
        if let Err(err) = result {
            return (Some(RoomResponse::Failure(err)), Vec::new());
        }
        // 进入 WaitForReady，host 默认已 ready（§6.5-7）
        let mut started = HashSet::new();
        started.insert(user_id);
        self.state = InternalState::WaitForReady { started };
        let mut events = vec![RoomEvent::GameStart {
            room_id: self.id.clone(),
            user: user_id,
        }];
        events.extend(self.check_all_ready());
        (Some(RoomResponse::Ok), events)
    }

    fn handle_ready(&mut self, ctx: CmdCtx) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        match &mut self.state {
            InternalState::WaitForReady { started } => {
                if !started.insert(user_id) {
                    return (
                        Some(RoomResponse::Failure(RoomError::Business {
                            code: RoomErrorCode::AlreadyReady,
                            msg: "already ready".to_owned(),
                        })),
                        Vec::new(),
                    );
                }
                let mut events = vec![RoomEvent::Ready {
                    room_id: self.id.clone(),
                    user: user_id,
                }];
                events.extend(self.check_all_ready());
                (Some(RoomResponse::Ok), events)
            }
            _ => (
                Some(RoomResponse::Failure(RoomError::Business {
                    code: RoomErrorCode::InvalidState,
                    msg: "invalid state".to_owned(),
                })),
                Vec::new(),
            ),
        }
    }

    fn handle_cancel_ready(&mut self, ctx: CmdCtx) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        match &mut self.state {
            InternalState::WaitForReady { started } => {
                if !started.remove(&user_id) {
                    return (
                        Some(RoomResponse::Failure(RoomError::Business {
                            code: RoomErrorCode::NotReady,
                            msg: "not ready".to_owned(),
                        })),
                        Vec::new(),
                    );
                }
                if self.is_host(user_id) {
                    // host 取消 → CancelGame + 回 SelectChart（§6.5-9）
                    self.state = InternalState::SelectChart;
                    (
                        Some(RoomResponse::Ok),
                        vec![RoomEvent::CancelGame {
                            room_id: self.id.clone(),
                            user: user_id,
                            chart: self.chart.as_ref().map(|c| c.id),
                        }],
                    )
                } else {
                    // 非 host → 仅 CancelReady
                    (
                        Some(RoomResponse::Ok),
                        vec![RoomEvent::CancelReady {
                            room_id: self.id.clone(),
                            user: user_id,
                        }],
                    )
                }
            }
            _ => (
                Some(RoomResponse::Failure(RoomError::Business {
                    code: RoomErrorCode::InvalidState,
                    msg: "invalid state".to_owned(),
                })),
                Vec::new(),
            ),
        }
    }

    async fn handle_played(
        &mut self,
        ctx: CmdCtx,
        id: i32,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        // 回源校验成绩（规则 10；仅阻塞该房间 actor，§4.9-2）
        let record = match self.deps.api.fetch_record(id).await {
            Ok(record) => record,
            Err(ApiError::Internal { msg }) => {
                return (
                    Some(RoomResponse::Failure(RoomError::Internal { msg })),
                    Vec::new(),
                );
            }
        };
        if record.player != user_id {
            return (
                Some(RoomResponse::Failure(RoomError::Business {
                    code: RoomErrorCode::InvalidRecord,
                    msg: "invalid record".to_owned(),
                })),
                Vec::new(),
            );
        }
        match &mut self.state {
            InternalState::Playing { results, aborted } => {
                if aborted.contains(&user_id) {
                    return (
                        Some(RoomResponse::Failure(RoomError::Business {
                            code: RoomErrorCode::AlreadyAborted,
                            msg: "aborted".to_owned(),
                        })),
                        Vec::new(),
                    );
                }
                if results.insert(user_id, record.clone()).is_some() {
                    return (
                        Some(RoomResponse::Failure(RoomError::Business {
                            code: RoomErrorCode::AlreadyUploaded,
                            msg: "already uploaded".to_owned(),
                        })),
                        Vec::new(),
                    );
                }
                let mut events = vec![RoomEvent::Played {
                    room_id: self.id.clone(),
                    user: user_id,
                    score: record.score,
                    accuracy: record.accuracy,
                    full_combo: record.full_combo,
                }];
                events.extend(self.check_all_ready());
                (Some(RoomResponse::Ok), events)
            }
            // 原版语义：非 Playing 状态的成绩静默忽略（记录已回源校验）
            _ => (Some(RoomResponse::Ok), Vec::new()),
        }
    }

    fn handle_abort(&mut self, ctx: CmdCtx) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        match &mut self.state {
            InternalState::Playing { results, aborted } => {
                if results.contains_key(&user_id) {
                    return (
                        Some(RoomResponse::Failure(RoomError::Business {
                            code: RoomErrorCode::AlreadyUploaded,
                            msg: "already uploaded".to_owned(),
                        })),
                        Vec::new(),
                    );
                }
                if !aborted.insert(user_id) {
                    return (
                        Some(RoomResponse::Failure(RoomError::Business {
                            code: RoomErrorCode::AlreadyAborted,
                            msg: "aborted".to_owned(),
                        })),
                        Vec::new(),
                    );
                }
                let mut events = vec![RoomEvent::Abort {
                    room_id: self.id.clone(),
                    user: user_id,
                }];
                events.extend(self.check_all_ready());
                (Some(RoomResponse::Ok), events)
            }
            _ => (
                Some(RoomResponse::Failure(RoomError::Business {
                    code: RoomErrorCode::InvalidState,
                    msg: "invalid state".to_owned(),
                })),
                Vec::new(),
            ),
        }
    }

    fn handle_lock(&mut self, ctx: CmdCtx, lock: bool) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        if let Err(err) = self.check_host(user_id) {
            return (Some(RoomResponse::Failure(err)), Vec::new());
        }
        self.locked = lock;
        (
            Some(RoomResponse::Ok),
            vec![RoomEvent::LockRoom {
                room_id: self.id.clone(),
                lock,
            }],
        )
    }

    fn handle_cycle(&mut self, ctx: CmdCtx, cycle: bool) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        if let Err(err) = self.check_host(user_id) {
            return (Some(RoomResponse::Failure(err)), Vec::new());
        }
        self.cycle = cycle;
        (
            Some(RoomResponse::Ok),
            vec![RoomEvent::CycleRoom {
                room_id: self.id.clone(),
                cycle,
            }],
        )
    }

    fn handle_user_disconnected(&mut self, user_id: i32) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        if !self.in_room(user_id) {
            return (None, Vec::new());
        }
        if matches!(self.state, InternalState::Playing { .. }) {
            // Playing 中断线：立即 abort（规则 22，无重连窗口）
            (None, self.on_playing_disconnect(user_id))
        } else {
            // 非 Playing：标记缺席，等重连或窗口到期（§4.6）
            self.absent.insert(user_id);
            (None, Vec::new())
        }
    }

    fn handle_user_reconnected(&mut self, user_id: i32) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        // 窗口内重连：保留座位（规则 21）
        self.absent.remove(&user_id);
        (None, Vec::new())
    }

    fn handle_user_dangle_expired(
        &mut self,
        user_id: i32,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        if !self.absent.contains(&user_id) {
            // 已重连/已离开：忽略
            return (None, Vec::new());
        }
        // 窗口到期：执行驱逐（规则 21）
        (None, self.evict(user_id))
    }

    fn handle_get_client_state(&mut self, user_id: i32) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        if self.in_room(user_id) {
            (
                Some(RoomResponse::ClientState(Some(
                    self.to_client_state(user_id),
                ))),
                Vec::new(),
            )
        } else {
            (Some(RoomResponse::ClientState(None)), Vec::new())
        }
    }

    fn handle_update_config(
        &mut self,
        config: RoomConfig,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        self.config = config;
        (None, Vec::new())
    }
}

/// 工厂：组合根注入一次并持有 deps（§4.9-6）。
pub struct RoomsV1 {
    config: RoomConfig,
    deps: RoomDeps,
}

impl RoomsV1 {
    /// 构造工厂。`deps` 中的 API/随机源由组合根注入（契约测试注入 fake）。
    #[must_use]
    pub const fn new(config: RoomConfig, deps: RoomDeps) -> Self {
        Self { config, deps }
    }
}

impl RoomFactory for RoomsV1 {
    fn create(&self, room_id: RoomId) -> Box<dyn RoomActor> {
        Box::new(RoomV1::new(
            room_id,
            self.config.clone(),
            RoomDeps {
                api: std::sync::Arc::clone(&self.deps.api),
                rng: std::sync::Arc::clone(&self.deps.rng),
            },
        ))
    }
}

#[async_trait::async_trait]
impl RoomActor for RoomV1 {
    async fn handle(
        &mut self,
        ctx: CmdCtx,
        cmd: RoomCommand,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        match cmd {
            RoomCommand::CreateRoom { .. } => self.handle_create(ctx, cmd),
            RoomCommand::JoinRoom { .. } => self.handle_join(ctx, cmd),
            RoomCommand::LeaveRoom => self.handle_leave(ctx),
            RoomCommand::Chat { message } => self.handle_chat(ctx, message.into_inner()),
            RoomCommand::SelectChart { id } => self.handle_select_chart(ctx, id).await,
            RoomCommand::RequestStart => self.handle_request_start(ctx),
            RoomCommand::Ready => self.handle_ready(ctx),
            RoomCommand::CancelReady => self.handle_cancel_ready(ctx),
            RoomCommand::Played { id } => self.handle_played(ctx, id).await,
            RoomCommand::Abort => self.handle_abort(ctx),
            RoomCommand::LockRoom { lock } => self.handle_lock(ctx, lock),
            RoomCommand::CycleRoom { cycle } => self.handle_cycle(ctx, cycle),
            RoomCommand::Touches { frames } => {
                let Origin::Client { user_id } = ctx.origin else {
                    return (None, Vec::new());
                };
                // live 时只转发给 monitor（§6.5-16）
                if self.live {
                    let targets = Targets::Specific(self.monitors.keys().copied().collect());
                    (
                        None,
                        vec![RoomEvent::RelayTouches {
                            room_id: self.id.clone(),
                            targets,
                            player: user_id,
                            frames,
                        }],
                    )
                } else {
                    (None, Vec::new())
                }
            }
            RoomCommand::Judges { judges } => {
                let Origin::Client { user_id } = ctx.origin else {
                    return (None, Vec::new());
                };
                if self.live {
                    let targets = Targets::Specific(self.monitors.keys().copied().collect());
                    (
                        None,
                        vec![RoomEvent::RelayJudges {
                            room_id: self.id.clone(),
                            targets,
                            player: user_id,
                            judges,
                        }],
                    )
                } else {
                    (None, Vec::new())
                }
            }
            RoomCommand::Tick { .. } => {
                // v1 无玩法倒计时（原版同：结算靠 Played 触发），占位（§4.6）
                (None, Vec::new())
            }
            RoomCommand::UserDisconnected { user_id, .. } => self.handle_user_disconnected(user_id),
            RoomCommand::UserReconnected { user_id, .. } => self.handle_user_reconnected(user_id),
            RoomCommand::UserDangleExpired { user_id } => self.handle_user_dangle_expired(user_id),
            RoomCommand::GetClientState { user_id } => self.handle_get_client_state(user_id),
            RoomCommand::UpdateConfig { config } => self.handle_update_config((*config).clone()),
            // §5.6：api 枚举 non_exhaustive，追加变体时必须留通配
            _ => (None, Vec::new()),
        }
    }
}
