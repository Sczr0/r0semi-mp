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

use std::sync::Arc;

use phira_api::{
    ApiError, Chart, ClientRoomState, CmdCtx, JoinRoomResponse, JudgeEvent, Origin, Record,
    RoomActor, RoomCommand, RoomConfig, RoomDeps, RoomError, RoomErrorCode, RoomEvent, RoomFactory,
    RoomId, RoomResponse, RoomState, Targets, TimeMs, TouchFrame, UserInfo,
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
        /// 强开截止时刻（B1 倒计时）：`None` = 尚未锚定（impl 唯一时钟源是 Tick，
        /// 进入状态时拿不到 now，等首个 Tick 补记 `now + READY_TIMEOUT_MS`）。
        deadline: Option<TimeMs>,
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
    /// 观战聚合缓冲（B6 对齐 gooophira MonitorBuffer）：live 下攒待转播的触摸帧，
    /// Tick 到达按 player 合并产出 `RelayTouches`（§6.5-17；键 = 发帧玩家）。
    touch_buf: HashMap<i32, Vec<Arc<Vec<TouchFrame>>>>,
    /// 判定事件同款聚合缓冲。
    judge_buf: HashMap<i32, Vec<Arc<Vec<JudgeEvent>>>>,
    /// A2：已受理待回源的成绩 (user_id, record_id) 集——幂等对账
    /// （RecordFetched 回注时 remove；房关/重开自然丢弃）。
    inflight: HashSet<(i32, i32)>,
    /// 每玩家最近一次上报触摸帧的谱面内时间（秒）——ISSUE-0007 game_time 钩子移植：
    /// 语义对齐原版（`frames.last().time`，f32 存 Arc 钉住地址不适用此处，直接 HashMap）；
    /// 哨兵 = [`NEG_INFINITY`]（"本局未开打"，对齐原版 `reset_game_time`）。
    /// 当前零行为差异（无消费方），供 §6.5-23 断线进度恢复做数据基础。
    game_time: HashMap<i32, f32>,
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
            touch_buf: HashMap::new(),
            judge_buf: HashMap::new(),
            game_time: HashMap::new(),
            inflight: HashSet::new(),
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
            InternalState::WaitForReady { started, .. } if started.contains(&user_id)
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
            // ISSUE-0007：请求者的最近进度（无记录/未开打 = NEG_INFINITY 哨兵）
            last_game_time: self
                .game_time
                .get(&user_id)
                .copied()
                .unwrap_or(f32::NEG_INFINITY),
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
        // B6：其未 flush 的触摸/判定残帧不应再播出
        self.drop_relay_bufs_of(user_id);

        if self.is_host(user_id) {
            events.extend(self.migrate_host(user_id, false));
        }
        // B6 发现的遗留 bug：live 只在 monitor 加入时置 true、从不回落。
        // 观战者全走后必须复位，否则 GetClientState.live 与转发路径长期虚热。
        if self.monitors.is_empty() {
            self.live = false;
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

    /// 周期心跳（B1/B6 通电，Tick 驱动 §4.6；倒计时对照 gooophira ready 60s 强开）。
    ///
    /// - **B6 flush**：先合并观战聚合缓冲产出 Relay* 事件（任何状态，缓冲空零成本）。
    /// - **B1 倒计时**：仅 `WaitForReady` 消费：首个 Tick 锚定 deadline = now + 60s；
    ///   到期时把未 ready 者走 [`Self::evict`]（复用 UserLeft 广播/房主迁移/空房
    ///   自毁收尾），剩余全员已 ready 则顺势 StartPlaying。
    /// - `Playing` 结算靠 Played 触发（原版同），v1 不加对局超时。
    /// - 绝对截止时刻（而非相对计数）：Tick 可丢（DropIfFull §4.9-9），丢一拍自愈。
    fn handle_tick(&mut self, now: TimeMs) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        const READY_TIMEOUT_MS: TimeMs = 60_000;
        // B6：观战聚合 flush（优先于倒计时，产出顺序在先——触摸是更旧的输入）
        let mut events = self.flush_relay_buffers();
        if let InternalState::WaitForReady { started, deadline } = &mut self.state {
            let Some(d) = *deadline else {
                // 首拍：锚定强开时刻（进入 WaitForReady 后的下一个 Tick）
                *deadline = Some(now.saturating_add(READY_TIMEOUT_MS));
                return (None, events);
            };
            if now >= d {
                // 到期：驱逐未 ready 者（started 外的全部在线成员）
                let not_ready: Vec<i32> = self
                    .users
                    .keys()
                    .chain(self.monitors.keys())
                    .copied()
                    .filter(|id| !started.contains(id))
                    .collect();
                for id in &not_ready {
                    events.extend(self.evict(*id));
                }
                // 剩余者若已全部 ready → 强制开局；否则留在 WaitForReady
                // （如全员被驱逐则空房自毁已由 evict 产出 RoomClosed）
                if matches!(self.state, InternalState::WaitForReady { .. }) {
                    events.extend(self.check_all_ready());
                }
            }
        }
        (None, events)
    }

    /// 合并并清空观战聚合缓冲，产出转播事件（B6，§6.5-17；对齐 gooophira
    /// MonitorBuffer.Flush）。同玩家多批 frames 拼接为一条命令（协议兼容：frames
    /// 向量拼接不改帧边界）；不同 player 分开命令（`SrvTouches.player` 语义）；
    /// touch 先 judge 后。targets 取**当前** monitor 集（flush 时解析最准确）。
    fn flush_relay_buffers(&mut self) -> Vec<RoomEvent> {
        if self.touch_buf.is_empty() && self.judge_buf.is_empty() {
            return Vec::new();
        }
        let mut events = Vec::new();
        if self.monitors.is_empty() {
            // 无观战者：直接丢弃（不延迟投递——积压无意义）
            self.touch_buf.clear();
            self.judge_buf.clear();
            return events;
        }
        let targets = Targets::Specific(self.monitors.keys().copied().collect());
        for (player, batches) in self.touch_buf.drain() {
            let merged: Vec<TouchFrame> = batches.iter().flat_map(|b| b.iter().cloned()).collect();
            events.push(RoomEvent::RelayTouches {
                room_id: self.id.clone(),
                targets: targets.clone(),
                player,
                frames: Arc::new(merged),
            });
        }
        for (player, batches) in self.judge_buf.drain() {
            let merged: Vec<JudgeEvent> = batches.iter().flat_map(|b| b.iter().cloned()).collect();
            events.push(RoomEvent::RelayJudges {
                room_id: self.id.clone(),
                targets: targets.clone(),
                player,
                judges: Arc::new(merged),
            });
        }
        events
    }

    /// 清理某用户的聚合缓冲残留（被驱逐时调用——其未 flush 的帧不应再播出）。
    fn drop_relay_bufs_of(&mut self, user_id: i32) {
        self.touch_buf.remove(&user_id);
        self.judge_buf.remove(&user_id);
        self.game_time.remove(&user_id);
    }

    /// 开局进度重置：全部玩家归 NEG_INFINITY 哨兵（ISSUE-0007，对齐原版
    /// `reset_game_time`——"本局未开打"与"已开打 ≥0"可区分）。
    fn reset_game_time(&mut self) {
        self.game_time.clear();
    }

    /// 对局彻底结束/流产（回 SelectChart）后的残余清理。
    fn clear_relay_bufs(&mut self) {
        self.touch_buf.clear();
        self.judge_buf.clear();
    }

    /// 检查开局/结算（原版 check_all_ready，规则 8/11）。
    fn check_all_ready(&mut self) -> Vec<RoomEvent> {
        match &self.state {
            InternalState::WaitForReady { started, .. } => {
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
                    // ISSUE-0007：全员 ready 正式开打 → 进度归零哨兵（对齐原版 room.rs:247）
                    self.reset_game_time();
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
                    // B6：对局结束，残余触摸/判定不再播出
                    self.clear_relay_bufs();
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
        // B1：deadline 由首个 Tick 锚定（impl 无主动时钟，§4.9-6）
        self.state = InternalState::WaitForReady {
            started,
            deadline: None,
        };
        // ISSUE-0007：开局重置进度（对齐原版 session.rs:602 reset 时机）
        self.reset_game_time();
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
            InternalState::WaitForReady { started, .. } => {
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
            InternalState::WaitForReady { started, .. } => {
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
                    // B6：对局流产，残余触摸/判定不再播出
                    self.clear_relay_bufs();
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

    /// 上报成绩（A2 两段式第 1 段，§4.9-2 规则 2）：只做受理——状态/幂等预检 +
    /// 登记 in-flight，立即返回 Ok；回源由 core 房外任务进行（不再 await 阻塞房间），
    /// 完成后经 `RecordFetched` 回注应用（第 2 段）。
    ///
    /// 原版语义对齐：非 Playing 的成绩**受理后静默丢弃**（回注时无房/无记录即忽略），
    /// 与旧内联路径"非 Playing 返回 Ok"一致。
    fn handle_played(&mut self, ctx: CmdCtx, id: i32) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        // 幂等预检：已入账 / 已中止 / 已在途（回源未归）——任一即按"成绩以首条为准"
        // 拒绝。in-flight 是关键：回注前的窗口内重复上报无法从 results 察觉。
        match &self.state {
            InternalState::Playing { results, aborted } => {
                if results.contains_key(&user_id)
                    || aborted.contains(&user_id)
                    || self.inflight.iter().any(|(u, _)| *u == user_id)
                {
                    return (
                        Some(RoomResponse::Failure(RoomError::Business {
                            code: RoomErrorCode::AlreadyUploaded,
                            msg: "already uploaded".to_owned(),
                        })),
                        Vec::new(),
                    );
                }
            }
            // 非 Playing：与原版一致静默成功（不登记 in-flight、不发起回源无效功）
            _ => return (Some(RoomResponse::Ok), Vec::new()),
        }
        self.inflight.insert((user_id, id));
        (Some(RoomResponse::Ok), Vec::new())
    }

    /// 回源结果应用（A2 第 2 段）：`RecordFetched` 系统命令回注。
    /// 校验链 = 旧内联顺序（player 匹配 → aborted → 重复）。成功 → `Played` 广播 +
    /// 全员结算检查；**失败（回源重试耗尽 / player 不匹配）→ 提交者按"无有效成绩"
    /// 结算为 aborted**——协议上玩家已收到 Ok 受理、不会重试，若只记日志则
    /// results/aborted 两不占 → GameEnd 永不触发 → 房间卡 Playing（§4.9-2 兜底）。
    fn handle_record_fetched(
        &mut self,
        ctx: CmdCtx,
        user_id: i32,
        record_id: i32,
        record: Result<Record, ApiError>,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        // 仅 core 回源任务可发起（系统 origin）
        if !matches!(ctx.origin, Origin::System) {
            return (None, Vec::new());
        }
        // in-flight 对账：不在受理由来集（房已重开/未受理过/重复回注）→ 忽略
        if !self.inflight.remove(&(user_id, record_id)) {
            tracing::warn!(user_id, record_id, "record_fetched without inflight entry");
            return (None, Vec::new());
        }
        let record = match record {
            Ok(record) => record,
            Err(ApiError::Internal { msg }) => {
                // 回源失败（core 已做过有界重试）：两段式下受理 Ok 早已发出、客户端不会
                // 重试，若只记日志则 results/aborted 两不占 → GameEnd 永不触发 → 房间卡
                // Playing。结算为"无有效成绩"（abort），保证对局必然收尾（§4.9-2 兜底）。
                tracing::warn!(
                    user_id,
                    record_id,
                    "record fetch failed after retries: {msg}"
                );
                return (None, self.settle_record_failed(user_id));
            }
        };
        if record.player != user_id {
            // 违规成绩（协议外行为）：不能因为一次错误上报就冻结整间房——同按
            // "无有效成绩"结算提交者。原版此处回 Failure(InvalidRecord)，两段式下
            // 响应已发不可达，以 abort 结算 + 日志保存可诊断性。
            tracing::warn!(
                user_id,
                record_id,
                actual = record.player,
                "record player mismatch — settled as aborted"
            );
            return (None, self.settle_record_failed(user_id));
        }
        match &mut self.state {
            InternalState::Playing { results, .. } => {
                if results.insert(user_id, record.clone()).is_some() {
                    return (None, Vec::new());
                }
                let mut events = vec![RoomEvent::Played {
                    room_id: self.id.clone(),
                    user: user_id,
                    score: record.score,
                    accuracy: record.accuracy,
                    full_combo: record.full_combo,
                }];
                events.extend(self.check_all_ready());
                (None, events)
            }
            // 受理后对局结束/流产（Abort 竞态）：结果自然作废
            _ => (None, Vec::new()),
        }
    }

    /// 回注失败/校验失败的结算兜底（A2，§4.9-2）：成绩无法入账时，把该玩家按
    /// "无有效成绩（中止）"入 `aborted` 集并广播 `Abort`——复用与客户端主动
    /// `handle_abort` 相同的收尾路径（清残余缓冲 + `check_all_ready`），
    /// 全员结算后 GameEnd 必然触发，房间不会因单笔回注失败卡死。
    /// 幂等：已有成绩/已中止（重复回注或竞态）→ 不产生事件。
    fn settle_record_failed(&mut self, user_id: i32) -> Vec<RoomEvent> {
        match &mut self.state {
            InternalState::Playing { results, aborted } => {
                if results.contains_key(&user_id) || !aborted.insert(user_id) {
                    return Vec::new();
                }
                let mut events = vec![RoomEvent::Abort {
                    room_id: self.id.clone(),
                    user: user_id,
                }];
                self.drop_relay_bufs_of(user_id);
                events.extend(self.check_all_ready());
                events
            }
            // 非 Playing（对局已结束/回 SelectChart）：结果作废，无需结算
            _ => Vec::new(),
        }
    }

    fn handle_abort(&mut self, ctx: CmdCtx) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let Origin::Client { user_id } = ctx.origin else {
            return (None, Vec::new());
        };
        match &mut self.state {
            InternalState::Playing { results, aborted } => {
                if results.contains_key(&user_id)
                    || self.inflight.iter().any(|(u, _)| *u == user_id)
                {
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
                // B6：已 abort 的玩家不再有未播出的帧
                self.drop_relay_bufs_of(user_id);
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
            RoomCommand::Played { id } => self.handle_played(ctx, id),
            RoomCommand::RecordFetched {
                user_id,
                record_id,
                record,
            } => self.handle_record_fetched(ctx, user_id, record_id, record),
            RoomCommand::Abort => self.handle_abort(ctx),
            RoomCommand::LockRoom { lock } => self.handle_lock(ctx, lock),
            RoomCommand::CycleRoom { cycle } => self.handle_cycle(ctx, cycle),
            RoomCommand::Touches { frames } => {
                let Origin::Client { user_id } = ctx.origin else {
                    return (None, Vec::new());
                };
                // ISSUE-0007 game_time：取本包最后一帧时间戳 = 玩家当前进度点
                // （对齐原版 session.rs:393；空包不更新，非 live 也记录——打歌客户端
                // live 标志与发帧无强绑定，进度记录不应依赖转发是否发生）。
                if let Some(last) = frames.last() {
                    self.game_time.insert(user_id, last.time);
                }
                // live 时只转发给 monitor（§6.5-16）。B6：不再立即产出 Relay 事件，
                // 入聚合缓冲，Tick 到达按 player 合并 flush（高频帧零碎冲击不再直达网络）。
                if self.live {
                    self.touch_buf.entry(user_id).or_default().push(frames);
                }
                (None, Vec::new())
            }
            RoomCommand::Judges { judges } => {
                let Origin::Client { user_id } = ctx.origin else {
                    return (None, Vec::new());
                };
                // B6：同 Touches —— 聚合缓冲，Tick flush。
                if self.live {
                    self.judge_buf.entry(user_id).or_default().push(judges);
                }
                (None, Vec::new())
            }
            RoomCommand::Tick { now } => self.handle_tick(now),
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
