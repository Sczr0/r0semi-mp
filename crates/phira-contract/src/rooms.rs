//! 房间契约测试套件（§5.3 / §6.5）。
//!
//! 泛型套件：对 `RoomFactory` 编写，任何 impl 只传构造器即可全量验证。
//! 每个断言可回溯 §6.5 规则清单；时间用 `Tick { now }` 伪造（§6.5-25），
//! HTTP/随机经 `RoomDeps` 注入 fake（§4.9-6）。
//!
//! **V2 想上线？先过同一套契约测试**（§5.3）。
//!
//! 约定（评审 §8 五-11：create 不再收第二份 deps，deps 由工厂持有）：
//! - `FakeApi` 确定性生成数据：`fetch_chart(id)` → 永远成功；`fetch_record(id)` →
//!   `Record { player: id }`——上报 `Played { id }` 时 player 天然等于上报者（§6.5-10）
//! - monitor 白名单等配置场景用 `UpdateConfig` 命令动态注入（§4.9-8）

use std::sync::{Arc, Mutex};

use phira_api::{
    ApiClient, ApiError, Chart, ClientRoomState, CmdCtx, Origin, RandomSource, Record, RoomActor,
    RoomCommand, RoomConfig, RoomDeps, RoomError, RoomErrorCode, RoomEvent, RoomFactory, RoomId,
    RoomResponse, RoomState, Targets, TouchFrame, UserInfo, Varchar,
};

// —— 测试替身（§4.9-6 依赖注入的兑现） ——

/// 确定性回源 API：任何 chart/record 都可取，record.player == record.id。
#[derive(Default)]
pub struct FakeApi;

#[async_trait::async_trait]
impl ApiClient for FakeApi {
    async fn fetch_chart(&self, id: i32) -> Result<Chart, ApiError> {
        Ok(Chart {
            id,
            name: format!("chart-{id}"),
        })
    }
    async fn fetch_record(&self, id: i32) -> Result<Record, ApiError> {
        Ok(Record {
            id,
            player: id,     // 与上报者 id 对齐（§6.5-10）
            chart: Some(1), // 契约套件本局默认谱面 id = 1（setup_playing SelectChart{id:1}）
            score: 100,
            perfect: 1,
            good: 2,
            bad: 3,
            miss: 4,
            max_combo: 5,
            accuracy: 0.99,
            full_combo: true,
            std: 0.1,
            std_score: 0.9,
        })
    }
}

/// 脚本化随机源：按预置序列返回 pick_index 结果（测试房主迁移，§6.5-5）。
#[derive(Default)]
pub struct SeqRng {
    picks: Mutex<std::collections::VecDeque<usize>>,
}

impl SeqRng {
    /// 追加一个脚本值。
    ///
    /// # Panics
    ///
    /// 测试替身：内部 Mutex 中毒时 panic（测试环境可接受）。
    pub fn push(&self, pick: usize) {
        self.picks.lock().unwrap().push_back(pick);
    }
}

impl RandomSource for SeqRng {
    fn pick_index(&self, len: usize) -> Option<usize> {
        // 未预置时确定性回退：返回 0（保证契约测试可复现）
        self.picks
            .lock()
            .unwrap()
            .pop_front()
            .or(if len > 0 { Some(0) } else { None })
    }
}

// —— 套件辅助 ——

/// 构造套件默认 deps（确定性 API + 确定性 RNG）。
#[must_use]
pub fn suite_deps() -> RoomDeps {
    RoomDeps {
        api: Arc::new(FakeApi),
        rng: Arc::new(SeqRng::default()),
    }
}

fn rid() -> RoomId {
    RoomId::new("test".to_owned()).unwrap()
}

fn ctx(user_id: i32) -> CmdCtx {
    CmdCtx {
        origin: Origin::Client { user_id },
        room_id: rid(),
    }
}

fn sys_ctx() -> CmdCtx {
    CmdCtx {
        origin: Origin::System,
        room_id: rid(),
    }
}

// 确定性回源结果（A2 回注测试用，FakeApi 同口径：`player == record.id`）。
//
// `record_ok_fn` 默认 `chart = Some(1)`——契约套件统一 setup_playing 选图 id=1；
// `record_mismatch_fn`（外谱面）与 `record_no_chart_fn`（fail-open）供 P1 场景用。
#[allow(clippy::unnecessary_wraps, clippy::missing_const_for_fn)] // Result 形状是 RecordFetched 载荷所需
fn record_with_chart(id: i32, chart: Option<i32>) -> Result<Record, ApiError> {
    Ok(Record {
        id,
        player: id, // FakeApi 口径：player == record.id（§6.5-10）
        chart,
        score: 100,
        perfect: 1,
        good: 2,
        bad: 3,
        miss: 4,
        max_combo: 5,
        accuracy: 0.99,
        full_combo: true,
        std: 0.1,
        std_score: 0.9,
    })
}

#[allow(clippy::unnecessary_wraps)]
fn record_ok_fn(id: i32) -> Result<Record, ApiError> {
    record_with_chart(id, Some(1))
}

#[allow(clippy::unnecessary_wraps)]
fn record_mismatch_fn(id: i32) -> Result<Record, ApiError> {
    record_with_chart(id, Some(999))
}

#[allow(clippy::unnecessary_wraps)]
fn record_no_chart_fn(id: i32) -> Result<Record, ApiError> {
    record_with_chart(id, None)
}

fn assert_business(resp: &RoomResponse, code: RoomErrorCode) {
    assert!(
        matches!(
            resp,
            RoomResponse::Failure(RoomError::Business { code: c, .. }) if *c == code
        ),
        "期望业务错误 {code:?}，实际 {resp:?}"
    );
}

/// 建房（host = 1）并断言成功。
async fn create_room(room: &mut Box<dyn RoomActor>) {
    let (resp, events) = room
        .handle(
            ctx(1),
            RoomCommand::CreateRoom {
                id: rid(),
                name: "user1".to_owned(),
            },
        )
        .await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    assert_eq!(
        events,
        vec![RoomEvent::RoomCreated {
            room_id: rid(),
            host: 1,
        }]
    );
}

/// 建房 + 用户 2/3 入房 + 选图 + RequestStart + 全员 ready → Playing。
async fn setup_playing(room: &mut Box<dyn RoomActor>) {
    create_room(room).await;
    room.handle(
        ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await;
    room.handle(
        ctx(3),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user3".to_owned(),
        },
    )
    .await;
    room.handle(ctx(1), RoomCommand::SelectChart { id: 1 })
        .await;
    room.handle(ctx(1), RoomCommand::RequestStart).await;
    room.handle(ctx(2), RoomCommand::Ready).await;
    room.handle(ctx(3), RoomCommand::Ready).await;
}

// —— 契约测试套件 ——

/// 房间契约全流程套件（§6.5 逐条断言）。
pub async fn room_contract_suite<F: RoomFactory>(factory: &F) {
    create_and_join_flow(factory).await;
    permissions_and_state(factory).await;
    game_flow(factory).await;
    record_fetch_failure_settles(factory).await;
    record_chart_mismatch_settles(factory).await;
    admin_management(factory).await;
    disconnect_reconnect(factory).await;
    monitor_and_relay(factory).await;
    config_and_client_state(factory).await;
    chat_and_edge_cases(factory).await;
    monitor_capacity_and_config_hotswap(factory).await;
    host_leave_migrates(factory).await;
    playing_leave_triggers_settle(factory).await;
    join_during_game_rejected(factory).await;
    ready_countdown_tick(factory).await;
    relay_aggregation_buffer(factory).await;
    game_time_tracking(factory).await;
}

/// game_time 进度记录（ISSUE-0007，§6.5-16/23）：Touches 记录最后帧时间，
/// RequestStart/全员 ready 开打时重置为 NEG_INFINITY 哨兵，GetClientState 返回。
#[allow(clippy::too_many_lines)] // 场景脚本六段断言一体
async fn game_time_tracking<F: RoomFactory>(factory: &F) {
    let frame = |t: f32| TouchFrame {
        time: t,
        points: Vec::new(),
    };

    // —— 未开打：GetClientState 返回哨兵 ——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    room.handle(
        ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await;
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::GetClientState { user_id: 2 })
        .await;
    let Some(RoomResponse::ClientState(Some(state))) = resp else {
        panic!("应返回房间状态: {resp:?}");
    };
    assert!(
        state.last_game_time.is_infinite() && state.last_game_time.is_sign_negative(),
        "未开打应为 NEG_INFINITY 哨兵, got {}",
        state.last_game_time
    );

    // —— SelectChart 态发 Touches：记录进度（live 与否不影响记录）——
    room.handle(
        ctx(2),
        RoomCommand::Touches {
            frames: Arc::new(vec![frame(3.0), frame(7.5)]),
        },
    )
    .await;
    // 空帧包不更新、不 panic
    room.handle(
        ctx(2),
        RoomCommand::Touches {
            frames: Arc::new(Vec::new()),
        },
    )
    .await;
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::GetClientState { user_id: 2 })
        .await;
    let Some(RoomResponse::ClientState(Some(state))) = resp else {
        panic!("应返回房间状态: {resp:?}");
    };
    assert_eq!(
        state.last_game_time.to_bits(),
        7.5f32.to_bits(),
        "应取最后一帧时间（空包不覆盖）"
    );

    // —— RequestStart → 重置哨兵（原版 session.rs:602 时机）——
    room.handle(ctx(1), RoomCommand::SelectChart { id: 1 })
        .await;
    room.handle(ctx(1), RoomCommand::RequestStart).await;
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::GetClientState { user_id: 2 })
        .await;
    let Some(RoomResponse::ClientState(Some(state))) = resp else {
        panic!("应返回房间状态: {resp:?}");
    };
    assert!(
        state.last_game_time.is_infinite() && state.last_game_time.is_sign_negative(),
        "开局应重置为 NEG_INFINITY, got {}",
        state.last_game_time
    );

    // —— 全员 ready → StartPlaying 再次重置；随后 Touches 记录新进度 ——
    room.handle(ctx(2), RoomCommand::Ready).await;
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::GetClientState { user_id: 2 })
        .await;
    let Some(RoomResponse::ClientState(Some(state))) = resp else {
        panic!("应返回房间状态: {resp:?}");
    };
    assert_eq!(state.state, RoomState::Playing);
    assert!(
        state.last_game_time.is_infinite() && state.last_game_time.is_sign_negative(),
        "StartPlaying 应再次重置, got {}",
        state.last_game_time
    );
    room.handle(
        ctx(2),
        RoomCommand::Touches {
            frames: Arc::new(vec![frame(1.25)]),
        },
    )
    .await;
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::GetClientState { user_id: 2 })
        .await;
    let Some(RoomResponse::ClientState(Some(state))) = resp else {
        panic!("应返回房间状态: {resp:?}");
    };
    assert_eq!(state.last_game_time.to_bits(), 1.25f32.to_bits());

    // —— 其余用户不受影响（host 未发过触摸 → 保持哨兵）/ 每用户独立 ——
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::GetClientState { user_id: 1 })
        .await;
    let Some(RoomResponse::ClientState(Some(state))) = resp else {
        panic!("host 应在房间: {resp:?}");
    };
    assert!(
        state.last_game_time.is_infinite() && state.last_game_time.is_sign_negative(),
        "无触摸记录的 host 应保持哨兵"
    );
}

/// 建房/入房/容量（§6.5-1/3/4/6/27）
async fn create_and_join_flow<F: RoomFactory>(factory: &F) {
    // —— 建房：RoomCreated + host 入房 ——
    let mut room = factory.create(rid());
    create_room(&mut room).await;

    // —— 用户 2 入房：JoinRoomResponse 携带状态与成员 ——
    let (resp, events) = room
        .handle(
            ctx(2),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
                name: "user2".to_owned(),
            },
        )
        .await;
    let Some(RoomResponse::JoinRoom(join_resp)) = resp else {
        panic!("期望 JoinRoomResponse，实际 {resp:?}");
    };
    assert_eq!(join_resp.state, RoomState::SelectChart(None));
    assert_eq!(join_resp.users.len(), 2);
    assert!(!join_resp.live);
    assert_eq!(
        events,
        vec![RoomEvent::UserJoined {
            room_id: rid(),
            user: UserInfo {
                id: 2,
                name: "user2".to_owned(),
                monitor: false,
            },
        }]
    );

    // —— 重复入房 → AlreadyInRoom（§6.5-27）——
    let (resp, _) = room
        .handle(
            ctx(2),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
                name: "user2".to_owned(),
            },
        )
        .await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::AlreadyInRoom);

    // —— 锁房后不可加入（§6.5-3）——
    let (resp, _) = room
        .handle(ctx(1), RoomCommand::LockRoom { lock: true })
        .await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    let (resp, _) = room
        .handle(
            ctx(3),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
                name: "user3".to_owned(),
            },
        )
        .await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::RoomLocked);

    // —— 空房自毁（§6.5-6）：全员离开 → RoomClosed ——
    let (_, events) = room.handle(ctx(1), RoomCommand::LeaveRoom).await;
    let (_, events2) = room.handle(ctx(2), RoomCommand::LeaveRoom).await;
    let mut all = events;
    all.extend(events2);
    assert!(
        all.contains(&RoomEvent::RoomClosed { room_id: rid() }),
        "空房应产出 RoomClosed: {all:?}"
    );
}

/// 权限与状态机（§6.5-2/7）
async fn permissions_and_state<F: RoomFactory>(factory: &F) {
    // —— 非 host 越权 → OnlyHost（§6.5-2）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    let (resp, _) = room
        .handle(ctx(2), RoomCommand::SelectChart { id: 1 })
        .await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::OnlyHost);
    let (resp, _) = room
        .handle(ctx(2), RoomCommand::LockRoom { lock: true })
        .await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::OnlyHost);
    let (resp, _) = room
        .handle(ctx(2), RoomCommand::CycleRoom { cycle: true })
        .await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::OnlyHost);

    // —— 未选图请求开始 → NoChartSelected（§6.5-7）——
    let (resp, _) = room.handle(ctx(1), RoomCommand::RequestStart).await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::NoChartSelected);

    // —— 选图（回源 API）→ SelectChart 事件（§6.5-15）——
    let (resp, events) = room
        .handle(ctx(1), RoomCommand::SelectChart { id: 1 })
        .await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    assert_eq!(
        events,
        vec![RoomEvent::SelectChart {
            room_id: rid(),
            user: 1,
            name: "chart-1".to_owned(),
            id: 1,
        }]
    );

    // —— 用户 2 入房后 RequestStart → GameStart + host 默认 ready（§6.5-7）——
    //    （单人房会因全员 ready 立即 StartPlaying——原版语义；这里先加 user 2 避免）
    room.handle(
        ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await;
    let (resp, events) = room.handle(ctx(1), RoomCommand::RequestStart).await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    assert_eq!(
        events,
        vec![RoomEvent::GameStart {
            room_id: rid(),
            user: 1,
        }]
    );

    // —— host CancelReady → CancelGame + 回 SelectChart（§6.5-9）——
    let (resp, events) = room.handle(ctx(1), RoomCommand::CancelReady).await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    assert_eq!(
        events,
        vec![RoomEvent::CancelGame {
            room_id: rid(),
            user: 1,
            chart: Some(1),
        }]
    );

    // —— 非 host CancelReady → 仅 CancelReady（§6.5-9）——
    //    需要 3 人房：user2 Ready 后 user3 未 ready，阻止开局
    room.handle(
        ctx(3),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user3".to_owned(),
        },
    )
    .await;
    room.handle(ctx(1), RoomCommand::RequestStart).await; // 重新进入 WaitForReady
    room.handle(ctx(2), RoomCommand::Ready).await;
    let (resp, events) = room.handle(ctx(2), RoomCommand::CancelReady).await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    assert_eq!(
        events,
        vec![RoomEvent::CancelReady {
            room_id: rid(),
            user: 2,
        }]
    );
}

/// 游戏流程（§6.5-8/10/11）：Ready → StartPlaying → Played → GameEnd/cycle
#[allow(clippy::too_many_lines)] // 游戏全流程脚本长是验收场景需求
async fn game_flow<F: RoomFactory>(factory: &F) {
    let mut room = factory.create(rid());
    setup_playing(&mut room).await;

    // —— Playing 中非 host CancelReady → InvalidState（不在 WaitForReady）——
    let (resp, _) = room.handle(ctx(2), RoomCommand::CancelReady).await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::InvalidState);

    // —— Played：A2 两段式（§4.9-2 规则 2）——
    // 第 1 段（受理）：幂等预检 + in-flight 登记，立即 Ok，无事件。
    let (resp, events) = room.handle(ctx(1), RoomCommand::Played { id: 1 }).await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    assert!(events.is_empty(), "受理段不应产出事件: {events:?}");
    // 重复上报（受理段即拒，与旧 AlreadyUploaded 语义一致）
    let (resp, _) = room.handle(ctx(1), RoomCommand::Played { id: 1 }).await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::AlreadyUploaded);

    // 第 2 段（回注）：RecordFetched 系统命令应用回源结果 → Played 广播事件。
    let (resp, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::RecordFetched {
                user_id: 1,
                record_id: 1,
                record: record_ok_fn(1),
            },
        )
        .await;
    assert!(resp.is_none(), "回注型命令无回话");
    assert!(events.contains(&RoomEvent::Played {
        room_id: rid(),
        user: 1,
        score: 100,
        accuracy: 0.99,
        full_combo: true,
    }));

    // —— player 不匹配：回注时发现（player=3 ≠ user 2）→ 提交者按"无有效成绩"结算 ——
    let (resp, _) = room.handle(ctx(2), RoomCommand::Played { id: 3 }).await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    let (resp, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::RecordFetched {
                user_id: 2,
                record_id: 3,
                record: record_ok_fn(3), // player=3 ≠ 2
            },
        )
        .await;
    assert!(resp.is_none());
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, RoomEvent::Played { user, .. } if *user == 2)),
        "违规成绩不得入账: {events:?}"
    );
    assert!(
        events.contains(&RoomEvent::Abort {
            room_id: rid(),
            user: 2
        }),
        "违规成绩提交者应结算为 aborted（否则房间卡 Playing）: {events:?}"
    );
    // 被结算后重试上报 → AlreadyUploaded（aborted 幂等锁位，无成绩可再取）
    let (resp, _) = room.handle(ctx(2), RoomCommand::Played { id: 2 }).await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::AlreadyUploaded);

    // —— 全员完成 → GameEnd（§6.5-11；user3 受理 + 回注触发结算）——
    let (resp, _) = room.handle(ctx(3), RoomCommand::Played { id: 3 }).await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    let (_, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::RecordFetched {
                user_id: 3,
                record_id: 3,
                record: record_ok_fn(3),
            },
        )
        .await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RoomEvent::GameEnd { room_id, .. } if room_id == &rid())),
        "全员完成应 GameEnd: {events:?}"
    );
    // 回到 SelectChart：可再次选图
    let (resp, _) = room
        .handle(ctx(1), RoomCommand::SelectChart { id: 1 })
        .await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));

    // —— Abort 路径：全员 abort → GameEnd ——
    let mut room = factory.create(rid());
    setup_playing(&mut room).await;
    let (resp, events) = room.handle(ctx(1), RoomCommand::Abort).await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    assert_eq!(
        events,
        vec![RoomEvent::Abort {
            room_id: rid(),
            user: 1,
        }]
    );
    // 重复 abort → AlreadyAborted
    let (resp, _) = room.handle(ctx(1), RoomCommand::Abort).await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::AlreadyAborted);
    room.handle(ctx(2), RoomCommand::Abort).await;
    let (_, events) = room.handle(ctx(3), RoomCommand::Abort).await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RoomEvent::GameEnd { room_id, .. } if room_id == &rid())),
        "全员 abort 应 GameEnd: {events:?}"
    );

    // —— cycle 房：结算后房主顺延下一位（§6.5-11）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    room.handle(
        ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await;
    room.handle(
        ctx(3),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user3".to_owned(),
        },
    )
    .await;
    let (_, events) = room
        .handle(ctx(1), RoomCommand::CycleRoom { cycle: true })
        .await;
    assert!(events.contains(&RoomEvent::CycleRoom {
        room_id: rid(),
        cycle: true
    }));
    room.handle(ctx(1), RoomCommand::SelectChart { id: 1 })
        .await;
    room.handle(ctx(1), RoomCommand::RequestStart).await;
    room.handle(ctx(2), RoomCommand::Ready).await;
    room.handle(ctx(3), RoomCommand::Ready).await;
    // A2：受理 + 回注（回注触发的 GameEnd 结算 → 房主顺延在 settle 事件里）。
    // 前两名受理+回注，最后一名的回注触发全员结算。
    for uid in [1, 2] {
        room.handle(ctx(uid), RoomCommand::Played { id: uid }).await;
        room.handle(
            sys_ctx(),
            RoomCommand::RecordFetched {
                user_id: uid,
                record_id: uid,
                record: record_ok_fn(uid),
            },
        )
        .await;
    }
    room.handle(ctx(3), RoomCommand::Played { id: 3 }).await;
    let (_, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::RecordFetched {
                user_id: 3,
                record_id: 3,
                record: record_ok_fn(3),
            },
        )
        .await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RoomEvent::GameEnd { room_id, .. } if room_id == &rid())),
        "cycle 结算应 GameEnd: {events:?}"
    );
    // 房主顺延：old=1 → new=2（原版 position+1 语义）
    assert!(
        events.contains(&RoomEvent::NewHost {
            room_id: rid(),
            new_host: 2,
            old_host: 1,
        }),
        "cycle 房主应顺延给下一位: {events:?}"
    );
}

/// A2 兜底（§4.9-2）：回注失败（回源重试耗尽）→ 提交者按"无有效成绩"结算，
/// 后续玩家正常结算 → GameEnd 必然触发——房间不会因单笔回注失败卡 Playing。
#[allow(clippy::too_many_lines)] // 场景脚本三段断言一体
async fn record_fetch_failure_settles<F: RoomFactory>(factory: &F) {
    let mut room = factory.create(rid());
    setup_playing(&mut room).await;

    // user1 正常受理 + 回注成功
    let (resp, _) = room.handle(ctx(1), RoomCommand::Played { id: 1 }).await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    room.handle(
        sys_ctx(),
        RoomCommand::RecordFetched {
            user_id: 1,
            record_id: 1,
            record: record_ok_fn(1),
        },
    )
    .await;

    // user2 受理 Ok；回注 Err（回源重试耗尽）→ 结算为 aborted：
    // 无 Played 事件、产生 Abort 事件、GameEnd 未触发（另两人才触发）
    let (resp, _) = room.handle(ctx(2), RoomCommand::Played { id: 2 }).await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    let (resp, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::RecordFetched {
                user_id: 2,
                record_id: 2,
                record: Err(ApiError::Internal {
                    msg: "upstream down".into(),
                }),
            },
        )
        .await;
    assert!(resp.is_none());
    assert!(
        events.contains(&RoomEvent::Abort {
            room_id: rid(),
            user: 2
        }),
        "回注失败应结算为 aborted: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, RoomEvent::Played { user: 2, .. })),
        "回注失败不得入账 Played: {events:?}"
    );

    // user3 正常受理 + 回注 → 全员结算（1 成绩 / 2 aborted / 3 成绩）→ GameEnd
    let (resp, _) = room.handle(ctx(3), RoomCommand::Played { id: 3 }).await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    let (_, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::RecordFetched {
                user_id: 3,
                record_id: 3,
                record: record_ok_fn(3),
            },
        )
        .await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RoomEvent::GameEnd { room_id, .. } if room_id == &rid())),
        "全员结算应 GameEnd（回注失败不影响收尾）: {events:?}"
    );
}

/// P1 谱面反作弊（§6.5-10）：成绩谱面与本局所选不一致 → 提交者按"无有效成绩"结算；
/// 缺省 chart（fail-open）与一致成绩照常入账；全员结算不卡房间。
#[allow(clippy::too_many_lines)] // 场景脚本三段断言一体
async fn record_chart_mismatch_settles<F: RoomFactory>(factory: &F) {
    let mut room = factory.create(rid());
    setup_playing(&mut room).await; // 本局选图 = chart 1（SelectChart { id: 1 }）

    // 一致成绩（chart=1）→ 正常入账 + Played 广播
    let (resp, _) = room.handle(ctx(1), RoomCommand::Played { id: 1 }).await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    let (_, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::RecordFetched {
                user_id: 1,
                record_id: 1,
                record: record_ok_fn(1), // chart=1 == 本局
            },
        )
        .await;
    assert!(
        events.contains(&RoomEvent::Played {
            room_id: rid(),
            user: 1,
            score: 100,
            accuracy: 0.99,
            full_combo: true,
        }),
        "一致成绩应入账: {events:?}"
    );

    // 外谱面成绩（chart=999 ≠ 1）→ 结算为 aborted：无 Played、有 Abort
    let (resp, _) = room.handle(ctx(2), RoomCommand::Played { id: 2 }).await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    let (resp, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::RecordFetched {
                user_id: 2,
                record_id: 2,
                record: record_mismatch_fn(2), // chart=999 ≠ 本局 1
            },
        )
        .await;
    assert!(resp.is_none());
    assert!(
        events.contains(&RoomEvent::Abort {
            room_id: rid(),
            user: 2
        }),
        "外谱面成绩应结算为 aborted: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, RoomEvent::Played { user: 2, .. })),
        "外谱面成绩不得入账: {events:?}"
    );

    // fail-open：缺省 chart（None）成绩照常入账；全员结算（1 成绩 / 2 aborted /
    // 3 fail-open 入账）→ GameEnd 必然触发（反作弊失败也绝不卡房间）
    let (resp, _) = room.handle(ctx(3), RoomCommand::Played { id: 3 }).await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    let (_, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::RecordFetched {
                user_id: 3,
                record_id: 3,
                record: record_no_chart_fn(3), // chart=None → fail-open
            },
        )
        .await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RoomEvent::GameEnd { room_id, .. } if room_id == &rid())),
        "全员结算应 GameEnd（fail-open 成绩照常收尾）: {events:?}"
    );
}

/// 管理动作（阶段 2，docs/admin-api.md §4）：AdminKick/AdminBroadcast——仅系统 origin，
/// 复用 evict（UserLeft+迁移+空房自毁）与系统 Chat（user=0）语义；不在房 → NotInRoom。
#[allow(clippy::too_many_lines)] // 场景脚本多段断言一体
async fn admin_management<F: RoomFactory>(factory: &F) {
    let mut room = factory.create(rid());
    setup_playing(&mut room).await; // 房主 1 + 玩家 2/3，Playing 态

    // —— AdminBroadcast：系统 Chat（user=0）房内广播 ——
    let (resp, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::AdminBroadcast {
                content: "维护通知".to_owned(),
            },
        )
        .await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    assert!(
        events.contains(&RoomEvent::Chat {
            room_id: rid(),
            user: 0,
            content: "维护通知".to_owned(),
        }),
        "公告应产系统 Chat: {events:?}"
    );

    // —— AdminKick 普通玩家：UserLeft + Playing 下触发结算检查（2 被踢 → 剩 1/3，未全结算）——
    let (resp, events) = room
        .handle(sys_ctx(), RoomCommand::AdminKick { user_id: 2 })
        .await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    assert!(
        events.contains(&RoomEvent::UserLeft {
            room_id: rid(),
            user: 2,
            name: "user2".to_owned(),
        }),
        "踢出应广播 UserLeft: {events:?}"
    );
    // 被踢者不在房：GetClientState 查不到
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::GetClientState { user_id: 2 })
        .await;
    assert!(matches!(resp, Some(RoomResponse::ClientState(None))));

    // —— AdminKick 房主：NewHost 迁移（新 host 在剩余玩家中；房间不空）——
    let (resp, events) = room
        .handle(sys_ctx(), RoomCommand::AdminKick { user_id: 1 })
        .await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    assert!(events.iter().any(|e| matches!(
        e,
        RoomEvent::UserLeft {
            room_id,
            user: 1,
            ..
        } if room_id == &rid()
    )));
    assert!(
        events.iter().any(|e| matches!(
            e,
            RoomEvent::NewHost {
                room_id,
                new_host: 3,
                old_host: 1,
            } if room_id == &rid()
        )),
        "房主被踢应迁移给剩余玩家: {events:?}"
    );

    // —— 不在房用户 → NotInRoom（管理面拿到精确失败）——
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::AdminKick { user_id: 2 })
        .await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::NotInRoom);

    // —— 非 System origin（客户端伪装）：静默忽略（无响应无事件）——
    let (resp, events) = room
        .handle(ctx(3), RoomCommand::AdminKick { user_id: 3 })
        .await;
    assert!(
        resp.is_none() && events.is_empty(),
        "客户端 origin 的管理命令应被忽略"
    );
}

/// 断线重连（§6.5-5/12/20/21/22/23）
#[allow(clippy::too_many_lines)] // 测试脚本流程长是场景需求
async fn disconnect_reconnect<F: RoomFactory>(factory: &F) {
    // —— 窗口内重连：保留座位（§6.5-21）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    room.handle(
        ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await;
    let (_, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::UserDisconnected {
                user_id: 2,
                epoch: 1,
            },
        )
        .await;
    assert!(events.is_empty(), "断线标记缺席，无事件: {events:?}");
    let (_, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::UserReconnected {
                user_id: 2,
                epoch: 2,
            },
        )
        .await;
    assert!(events.is_empty(), "重连恢复座位，无事件: {events:?}");
    // 座位保留：GetClientState 仍能找到
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::GetClientState { user_id: 2 })
        .await;
    assert!(matches!(resp, Some(RoomResponse::ClientState(Some(_)))));

    // —— 窗口外驱逐（§6.5-21）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    room.handle(
        ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await;
    room.handle(
        sys_ctx(),
        RoomCommand::UserDisconnected {
            user_id: 2,
            epoch: 1,
        },
    )
    .await;
    let (_, events) = room
        .handle(sys_ctx(), RoomCommand::UserDangleExpired { user_id: 2 })
        .await;
    assert!(
        events.iter().any(
            |e| matches!(e, RoomEvent::UserLeft { room_id, user: 2, .. } if room_id == &rid())
        ),
        "窗口到期应驱逐: {events:?}"
    );
    assert!(
        !events.contains(&RoomEvent::RoomClosed { room_id: rid() }),
        "host 还在，不应自毁: {events:?}"
    );

    // —— 非缺席用户 DangleExpired → 忽略（已重连/已离开）——
    let (_, events) = room
        .handle(sys_ctx(), RoomCommand::UserDangleExpired { user_id: 2 })
        .await;
    assert!(events.is_empty(), "非缺席者忽略: {events:?}");

    // —— 房主断线驱逐 → 随机迁移新 host（§6.5-5；SeqRng 默认选 0）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    room.handle(
        ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await;
    room.handle(
        sys_ctx(),
        RoomCommand::UserDisconnected {
            user_id: 1,
            epoch: 1,
        },
    )
    .await;
    let (_, events) = room
        .handle(sys_ctx(), RoomCommand::UserDangleExpired { user_id: 1 })
        .await;
    assert!(
        events.contains(&RoomEvent::NewHost {
            room_id: rid(),
            new_host: 2,
            old_host: 1,
        }),
        "房主被驱逐应迁移: {events:?}"
    );

    // —— Playing 中断线：标记缺席，窗口由 core 分级（C-03/ADR-0012，原规则 22
    //    "立即驱逐、无重连窗口"取消——对局掉线不再 10s 弃赛）——
    let mut room = factory.create(rid());
    setup_playing(&mut room).await;
    let (_, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::UserDisconnected {
                user_id: 2,
                epoch: 1,
            },
        )
        .await;
    assert!(
        !events.iter().any(
            |e| matches!(e, RoomEvent::UserLeft { room_id, user: 2, .. } if room_id == &rid())
        ),
        "Playing 断线应标记缺席、不立即驱逐: {events:?}"
    );
    // 窗口到期（core 已按 Playing 分级为 playing_reconnect_window）→ 驱逐
    let (_, events) = room
        .handle(sys_ctx(), RoomCommand::UserDangleExpired { user_id: 2 })
        .await;
    assert!(
        events.iter().any(
            |e| matches!(e, RoomEvent::UserLeft { room_id, user: 2, .. } if room_id == &rid())
        ),
        "Playing 缺席窗口到期应驱逐: {events:?}"
    );
    // 已驱逐后再次 DangleExpired → 忽略
    let (_, events) = room
        .handle(sys_ctx(), RoomCommand::UserDangleExpired { user_id: 2 })
        .await;
    assert!(
        events.is_empty(),
        "已驱逐后 DangleExpired 应忽略: {events:?}"
    );
}

/// monitor 与热路径转发（§6.5-1/4/16/17）
#[allow(clippy::too_many_lines)] // monitor 权限/热路径脚本长是验收场景需求
async fn monitor_and_relay<F: RoomFactory>(factory: &F) {
    // —— 白名单 monitor：UpdateConfig 动态注入（§4.9-8）——
    let config = RoomConfig { monitors: vec![9] };
    let mut room = factory.create(rid());
    room.handle(
        sys_ctx(),
        RoomCommand::UpdateConfig {
            config: Arc::new(config),
        },
    )
    .await;
    create_room(&mut room).await;

    // —— 白名单内 monitor 入房 → live=true（§6.5-4）——
    let (resp, events) = room
        .handle(
            ctx(9),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: true,
                name: "user9".to_owned(),
            },
        )
        .await;
    let Some(RoomResponse::JoinRoom(jr)) = resp else {
        panic!("monitor 应能入房: {resp:?}");
    };
    assert!(jr.live, "monitor 入房后 live 应为 true");
    assert!(events.contains(&RoomEvent::UserJoined {
        room_id: rid(),
        user: UserInfo {
            id: 9,
            name: "user9".to_owned(),
            monitor: true
        },
    }));

    // —— 非白名单 monitor → CannotMonitor（§6.5-4）——
    let (resp, _) = room
        .handle(
            ctx(5),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: true,
                name: "user5".to_owned(),
            },
        )
        .await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::CannotMonitor);

    // —— live 下 Touches → RelayTouches 只投 monitor（§6.5-16/17）——
    let (resp, events) = room
        .handle(
            ctx(1),
            RoomCommand::Touches {
                frames: Arc::new(vec![]),
            },
        )
        .await;
    assert!(resp.is_none(), "Touches 无回话");
    // B6 观战聚合：入缓冲不立即转播（Tick 驱动 flush）
    assert!(events.is_empty(), "应先入聚合缓冲: {events:?}");

    // 同一玩家两批帧 → 一次 Tick flush 合并为一条 RelayTouches（B6 对齐 gooophira）
    let (_, _) = room
        .handle(
            ctx(1),
            RoomCommand::Touches {
                frames: Arc::new(vec![TouchFrame {
                    time: 0.5,
                    points: Vec::new(),
                }]),
            },
        )
        .await;
    let (resp, events) = room
        .handle(sys_ctx(), RoomCommand::Tick { now: 1_000 })
        .await;
    assert!(resp.is_none());
    assert_eq!(events.len(), 1, "flush 应产出合并后的单条转播: {events:?}");
    match &events[0] {
        RoomEvent::RelayTouches {
            targets,
            player,
            frames,
            ..
        } => {
            assert_eq!(player, &1);
            assert_eq!(targets, &Targets::Specific(vec![9]), "只投 monitor");
            // 空 frames 批 + 1 帧 = 合并后 1 帧
            assert_eq!(frames.len(), 1, "同玩家多批帧应拼接合并");
        }
        other => panic!("期望 RelayTouches: {other:?}"),
    }

    // —— 非 live 下 Touches 不转发，也不留缓冲残留 ——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    let (_, events) = room
        .handle(
            ctx(1),
            RoomCommand::Touches {
                frames: Arc::new(vec![]),
            },
        )
        .await;
    assert!(events.is_empty(), "非 live 不转发: {events:?}");
    let (_, events) = room
        .handle(sys_ctx(), RoomCommand::Tick { now: 2_000 })
        .await;
    assert!(events.is_empty(), "非 live 下 Tick 也无产出: {events:?}");

    // —— 8 人上限（§6.5-1）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    for u in 2..=8 {
        let (resp, _) = room
            .handle(
                ctx(u),
                RoomCommand::JoinRoom {
                    id: rid(),
                    monitor: false,
                    name: format!("user{u}"),
                },
            )
            .await;
        assert!(
            matches!(resp, Some(RoomResponse::JoinRoom(_))),
            "user{u} 应能入房，实际 {resp:?}"
        );
    }
    let (resp, _) = room
        .handle(
            ctx(9),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
                name: "user9".to_owned(),
            },
        )
        .await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::RoomFull);
}

/// 配置与重连状态（§6.5-23）
async fn config_and_client_state<F: RoomFactory>(factory: &F) {
    // —— GetClientState 恢复房间状态（§6.5-23）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    room.handle(
        ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await;
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::GetClientState { user_id: 2 })
        .await;
    let Some(RoomResponse::ClientState(Some(state))) = resp else {
        panic!("应返回房间状态: {resp:?}");
    };
    let ClientRoomState {
        state: s,
        users,
        is_host,
        is_ready,
        ..
    } = state;
    assert_eq!(s, RoomState::SelectChart(None));
    assert_eq!(users.len(), 2);
    assert!(!is_host);
    assert!(!is_ready);

    // 不在房间 → None
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::GetClientState { user_id: 99 })
        .await;
    assert!(matches!(resp, Some(RoomResponse::ClientState(None))));

    // —— is_ready / Playing 状态反映（§6.5-23）——
    let mut room = factory.create(rid());
    setup_playing(&mut room).await;
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::GetClientState { user_id: 2 })
        .await;
    let Some(RoomResponse::ClientState(Some(state))) = resp else {
        panic!("应返回房间状态: {resp:?}");
    };
    // 原版语义：is_ready 只在 WaitForReady 状态有意义；Playing 时 false（§6.5-23）
    assert_eq!(state.state, RoomState::Playing, "全员 ready 后应在 Playing");
    assert!(
        !state.is_ready,
        "Playing 状态下 is_ready 应为 false（原版语义）"
    );
    assert!(!state.live, "monitor 未加入时 live 应为 false");

    // —— Playing 态且聚合缓冲空时 Tick 无产出（B1 倒计时只挂 WaitForReady；B6 flush 空转）——
    let (resp, events) = room.handle(sys_ctx(), RoomCommand::Tick { now: 999 }).await;
    assert!(resp.is_none());
    assert!(events.is_empty());
}

/// 聊天与命令边界（§6.5 / §5.6：非穷尽匹配姿态）
async fn chat_and_edge_cases<F: RoomFactory>(factory: &F) {
    // —— Chat：任意状态可聊，产出 Chat 事件（§6.3 Message::Chat）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    let (resp, events) = room
        .handle(
            ctx(1),
            RoomCommand::Chat {
                message: Varchar::new("hello phira".to_owned()).unwrap(),
            },
        )
        .await;
    assert!(matches!(resp, Some(RoomResponse::Ok)));
    assert_eq!(
        events,
        vec![RoomEvent::Chat {
            room_id: rid(),
            user: 1,
            content: "hello phira".to_owned(),
        }]
    );

    // —— 非成员 Chat → NotInRoom（actor 层 in_room 检查）——
    let (resp, _) = room
        .handle(
            ctx(99),
            RoomCommand::Chat {
                message: Varchar::new("x".to_owned()).unwrap(),
            },
        )
        .await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::NotInRoom);

    // —— Ready 重复 → AlreadyReady（§6.5；3 人房避免 ready 即开局）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    room.handle(
        ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await;
    room.handle(
        ctx(3),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user3".to_owned(),
        },
    )
    .await;
    room.handle(ctx(1), RoomCommand::SelectChart { id: 1 })
        .await;
    room.handle(ctx(1), RoomCommand::RequestStart).await;
    room.handle(ctx(2), RoomCommand::Ready).await;
    let (resp, _) = room.handle(ctx(2), RoomCommand::Ready).await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::AlreadyReady);

    // —— CancelReady 但未 ready → NotReady（§6.5；user3 未 ready，CancelReady 报 NotReady）——
    let (resp, _) = room.handle(ctx(3), RoomCommand::CancelReady).await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::NotReady);

    // —— 非 WaitForReady 状态 Ready → InvalidState（§6.4 状态机）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    let (resp, _) = room.handle(ctx(1), RoomCommand::Ready).await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::InvalidState);

    // —— SelectChart 非 SelectChart 状态 → InvalidState（§6.4）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    room.handle(ctx(1), RoomCommand::SelectChart { id: 1 })
        .await;
    room.handle(ctx(1), RoomCommand::RequestStart).await; // 进入 WaitForReady
    let (resp, _) = room
        .handle(ctx(1), RoomCommand::SelectChart { id: 2 })
        .await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::InvalidState);

    // —— Played 非 Playing 状态：原版语义静默成功（成绩已回源校验）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    let (resp, _) = room.handle(ctx(1), RoomCommand::Played { id: 1 }).await;
    assert!(
        matches!(resp, Some(RoomResponse::Ok)),
        "非 Playing 上报应静默 Ok"
    );

    // —— Abort 非 Playing 状态 → InvalidState ——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    let (resp, _) = room.handle(ctx(1), RoomCommand::Abort).await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::InvalidState);

    // —— LeaveRoom 未入房 → NotInRoom（actor 层）——
    let mut room = factory.create(rid());
    let (resp, _) = room.handle(ctx(5), RoomCommand::LeaveRoom).await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::NotInRoom);
}

/// monitor 容量豁免与配置热更（§6.5-1/4，§4.9-8）
async fn monitor_capacity_and_config_hotswap<F: RoomFactory>(factory: &F) {
    // —— 8 人满后 monitor 仍可加入（不占名额，§6.5-1）——
    let config = RoomConfig { monitors: vec![99] };
    let mut room = factory.create(rid());
    room.handle(
        sys_ctx(),
        RoomCommand::UpdateConfig {
            config: Arc::new(config),
        },
    )
    .await;
    create_room(&mut room).await;
    for u in 2..=8 {
        room.handle(
            ctx(u),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
                name: format!("user{u}"),
            },
        )
        .await;
    }
    // 第 9 个玩家 → RoomFull
    let (resp, _) = room
        .handle(
            ctx(9),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
                name: "user9".to_owned(),
            },
        )
        .await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::RoomFull);
    // monitor（白名单 99）→ 可加入
    let (resp, _) = room
        .handle(
            ctx(99),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: true,
                name: "user99".to_owned(),
            },
        )
        .await;
    assert!(
        matches!(resp, Some(RoomResponse::JoinRoom(_))),
        "monitor 不占名额"
    );

    // —— 配置热更：白名单变更后权限即时生效（§4.9-8）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    // 初始白名单空 → 任何人都不能当 monitor
    let (resp, _) = room
        .handle(
            ctx(7),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: true,
                name: "user7".to_owned(),
            },
        )
        .await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::CannotMonitor);
    // 热更：把 7 加进白名单
    room.handle(
        sys_ctx(),
        RoomCommand::UpdateConfig {
            config: Arc::new(RoomConfig { monitors: vec![7] }),
        },
    )
    .await;
    let (resp, _) = room
        .handle(
            ctx(7),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: true,
                name: "user7".to_owned(),
            },
        )
        .await;
    assert!(
        matches!(resp, Some(RoomResponse::JoinRoom(_))),
        "热更后 7 可当 monitor"
    );
}

/// 房主主动离开的迁移（§6.5-5；与断线驱逐同路径）
async fn host_leave_migrates<F: RoomFactory>(factory: &F) {
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    room.handle(
        ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await;
    room.handle(
        ctx(3),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user3".to_owned(),
        },
    )
    .await;
    // host 主动离开 → 随机迁移新 host（SeqRng 默认选 0 = user2）
    let (_, events) = room.handle(ctx(1), RoomCommand::LeaveRoom).await;
    assert!(
        events.contains(&RoomEvent::NewHost {
            room_id: rid(),
            new_host: 2,
            old_host: 1,
        }),
        "host 离开应迁移: {events:?}"
    );
    assert!(
        !events.contains(&RoomEvent::RoomClosed { room_id: rid() }),
        "还有其他玩家，不应自毁: {events:?}"
    );

    // —— 新 host 具有 host 权限（§6.5-2）——
    let (resp, _) = room
        .handle(ctx(2), RoomCommand::CycleRoom { cycle: true })
        .await;
    assert!(matches!(resp, Some(RoomResponse::Ok)), "新 host 可循环房");
    let (resp, _) = room
        .handle(ctx(3), RoomCommand::CycleRoom { cycle: false })
        .await;
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::OnlyHost);

    // —— 全部离开 → RoomClosed（§6.5-6）——
    let (_, e1) = room.handle(ctx(2), RoomCommand::LeaveRoom).await;
    let (_, e2) = room.handle(ctx(3), RoomCommand::LeaveRoom).await;
    let mut all = e1;
    all.extend(e2);
    assert!(all.contains(&RoomEvent::RoomClosed { room_id: rid() }));
}

/// Playing 中玩家离开 → 剩余玩家完成即结算（§6.5-11 原版 on_user_leave 触发 check_all_ready）
async fn playing_leave_triggers_settle<F: RoomFactory>(factory: &F) {
    let mut room = factory.create(rid());
    setup_playing(&mut room).await; // 1,2,3 全员 Playing
    // 用户 3 离开 → 剩余 1,2
    let (_, events) = room.handle(ctx(3), RoomCommand::LeaveRoom).await;
    assert!(events.iter().any(|e| matches!(
        e,
        RoomEvent::UserLeft { room_id, user: 3, .. } if room_id == &rid()
    )));
    // 1,2 上报成绩 → 全员完成（users 只剩 1,2）→ GameEnd
    // A2 两段式：受理 + 回注；最后一笔回注触发结算。
    let record1 = record_ok_fn(1);
    room.handle(ctx(1), RoomCommand::Played { id: 1 }).await;
    room.handle(
        sys_ctx(),
        RoomCommand::RecordFetched {
            user_id: 1,
            record_id: 1,
            record: record1,
        },
    )
    .await;
    room.handle(ctx(2), RoomCommand::Played { id: 2 }).await;
    let (_, events) = room
        .handle(
            sys_ctx(),
            RoomCommand::RecordFetched {
                user_id: 2,
                record_id: 2,
                record: record_ok_fn(2),
            },
        )
        .await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RoomEvent::GameEnd { room_id, .. } if room_id == &rid())),
        "剩余玩家完成应结算: {events:?}"
    );
}

/// §6.5 规则 3：仅 SelectChart 状态可加入——对局中 / 等待就绪加入 → GameOngoing 拒绝（有提示）。
async fn join_during_game_rejected<F: RoomFactory>(factory: &F) {
    // —— Playing 中加入 ——
    let mut room = factory.create(rid());
    setup_playing(&mut room).await; // 3 人开局 → Playing
    let (resp, events) = room
        .handle(
            ctx(4),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
                name: "user4".to_owned(),
            },
        )
        .await;
    assert!(events.is_empty(), "拒绝加入不应有事件");
    let resp = resp.expect("应有响应");
    assert_business(&resp, RoomErrorCode::GameOngoing);

    // —— WaitForReady 中加入 ——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    room.handle(
        ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await;
    room.handle(ctx(1), RoomCommand::SelectChart { id: 1 })
        .await;
    room.handle(ctx(1), RoomCommand::RequestStart).await; // → WaitForReady（user2 未 ready 不开局）
    let (resp, events) = room
        .handle(
            ctx(3),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
                name: "user3".to_owned(),
            },
        )
        .await;
    assert!(events.is_empty());
    assert_business(resp.as_ref().unwrap(), RoomErrorCode::GameOngoing);
}

/// B1 玩法倒计时（§4.6 时间事实命令化 + §6.5-8 对照 gooophira 60s 强开）：
///
/// 1. 首 Tick 锚定 deadline（无事件、无响应）
/// 2. 全员已 ready → StartPlaying 后 Tick 无副作用
/// 3. 超时：未 ready 者被驱逐（UserLeft），剩余全员 ready → 强制 StartPlaying
#[allow(clippy::too_many_lines)] // 倒计时三场景长是契约验收需求（同 monitor_and_relay）
async fn ready_countdown_tick<F: RoomFactory>(factory: &F) {
    // —— 场景 A：全员 ready 进入 Playing → Tick 不再有副作用 ——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    room.handle(
        ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await;
    room.handle(ctx(1), RoomCommand::SelectChart { id: 1 })
        .await;
    let (_, events) = room.handle(ctx(1), RoomCommand::RequestStart).await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RoomEvent::GameStart { .. })),
        "RequestStart 应 GameStart"
    );

    // 首个 Tick：锚定 deadline，无事件
    let (resp, events) = room
        .handle(sys_ctx(), RoomCommand::Tick { now: 1_000 })
        .await;
    assert!(resp.is_none(), "锚定 Tick 无响应: {resp:?}");
    assert!(events.is_empty(), "锚定 Tick 无事件: {events:?}");

    // user2 ready → StartPlaying；此后 Tick 无副作用（Playing 不消费）
    let (_, _) = room.handle(ctx(2), RoomCommand::Ready).await;
    let (resp, events) = room
        .handle(sys_ctx(), RoomCommand::Tick { now: 70_000 })
        .await;
    assert!(resp.is_none());
    assert!(events.is_empty(), "Playing 态不应消费 Tick: {events:?}");

    // —— 场景 B：超时强开 —— host(1) 已 ready，user3 未 ready 到期被驱逐，
    //    剩余玩家全员 ready → StartPlaying（gooophira「未准备 Aborted」语义）——
    let mut room = factory.create(rid());
    create_room(&mut room).await;
    for (uid, name) in [(2, "user2"), (3, "user3")] {
        room.handle(
            ctx(uid),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
                name: name.to_owned(),
            },
        )
        .await;
    }
    room.handle(ctx(1), RoomCommand::SelectChart { id: 1 })
        .await;
    let (_, _) = room.handle(ctx(1), RoomCommand::RequestStart).await; // WaitForReady
    let (_, _) = room.handle(ctx(2), RoomCommand::Ready).await; // user2 就绪

    // 锚定 + 直接到期（60s 后的 Tick）
    let (_, _) = room
        .handle(sys_ctx(), RoomCommand::Tick { now: 1_000 })
        .await;
    let (resp, events) = room
        .handle(sys_ctx(), RoomCommand::Tick { now: 61_500 })
        .await;
    assert!(resp.is_none(), "超时驱逐无响应: {resp:?}");
    // user3 被驱逐（UserLeft）；随后剩余全员 ready → StartPlaying
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RoomEvent::UserLeft { user: 3, .. })),
        "未 ready 的 user3 应被驱逐: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RoomEvent::StartPlaying { .. })),
        "驱逐后剩余全员已 ready 应强制开局: {events:?}"
    );

    // 开局后的状态确认：Playing
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::GetClientState { user_id: 1 })
        .await;
    let Some(RoomResponse::ClientState(Some(state))) = resp else {
        panic!("应返回房间状态: {resp:?}");
    };
    assert_eq!(state.state, RoomState::Playing, "强制开局后应在 Playing");

    // —— 场景 C：仅 monitor 未 ready → 也被驱逐（check_all_ready 要求 users+monitors 全就绪）——
    let mut room = factory.create(rid());
    // monitor 白名单注入 id=9（§6.5-4，与 monitor_and_relay 同款手法）
    room.handle(
        sys_ctx(),
        RoomCommand::UpdateConfig {
            config: Arc::new(RoomConfig { monitors: vec![9] }),
        },
    )
    .await;
    create_room(&mut room).await;
    // monitor 先入房（仅 SelectChart 状态可加入，§6.5-3），再进 WaitForReady
    room.handle(
        ctx(9),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: true,
            name: "watcher".to_owned(),
        },
    )
    .await;
    room.handle(ctx(1), RoomCommand::SelectChart { id: 1 })
        .await;
    let (_, _) = room.handle(ctx(1), RoomCommand::RequestStart).await;
    let (_, _) = room
        .handle(sys_ctx(), RoomCommand::Tick { now: 1_000 })
        .await;
    let (resp, events) = room
        .handle(sys_ctx(), RoomCommand::Tick { now: 62_000 })
        .await;
    assert!(resp.is_none());
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RoomEvent::UserLeft { user: 9, .. })),
        "未 ready 的 monitor 也应被驱逐: {events:?}"
    );
}

/// B6 观战聚合缓冲（§6.5-17，对齐 gooophira AggregatingMonitorBuffer）：
///
/// 1. 不同玩家分命令（`SrvTouches.player` 语义——不跨玩家合并）
/// 2. Judges 同款聚合并与 Touches 分离（touch 先 judge 后）
/// 3. abort 玩家的残余帧不再播出（对局内离开即断流）
/// 4. monitor 全部离开 → 缓冲直接丢弃（Tick 无产出、无积压）
#[allow(clippy::too_many_lines)] // 四场景脚本长是验收需求
async fn relay_aggregation_buffer<F: RoomFactory>(factory: &F) {
    let frame = |t: f32| TouchFrame {
        time: t,
        points: Vec::new(),
    };
    // —— 场景 1+2：分玩家分命令 + judges 聚合 ——
    let mut room = factory.create(rid());
    room.handle(
        sys_ctx(),
        RoomCommand::UpdateConfig {
            config: Arc::new(RoomConfig { monitors: vec![9] }),
        },
    )
    .await;
    create_room(&mut room).await;
    for uid in [2, 3] {
        room.handle(
            ctx(uid),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
                name: format!("user{uid}"),
            },
        )
        .await;
    }
    room.handle(
        ctx(9),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: true,
            name: "mon".to_owned(),
        },
    )
    .await;
    // 进入 live 对局：选图 + RequestStart + 全员 ready（玩家 + monitor 同口径）
    room.handle(ctx(1), RoomCommand::SelectChart { id: 1 })
        .await;
    let (_, _) = room.handle(ctx(1), RoomCommand::RequestStart).await;
    let (_, _) = room.handle(ctx(2), RoomCommand::Ready).await;
    let (_, _) = room.handle(ctx(3), RoomCommand::Ready).await;
    let (_, events) = room.handle(ctx(9), RoomCommand::Ready).await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RoomEvent::StartPlaying { .. })),
        "全员 ready 应开局"
    );

    // 两玩家交错发帧；player2 同时发判定
    let (_, _) = room
        .handle(
            ctx(2),
            RoomCommand::Touches {
                frames: Arc::new(vec![frame(1.0)]),
            },
        )
        .await;
    let (_, _) = room
        .handle(
            ctx(3),
            RoomCommand::Touches {
                frames: Arc::new(vec![frame(2.0), frame(2.5)]),
            },
        )
        .await;
    let (_, _) = room
        .handle(
            ctx(2),
            RoomCommand::Judges {
                judges: Arc::new(vec![]),
            },
        )
        .await;
    let (_, _) = room
        .handle(
            ctx(3),
            RoomCommand::Touches {
                frames: Arc::new(vec![frame(3.0)]),
            },
        )
        .await;
    let (resp, events) = room
        .handle(sys_ctx(), RoomCommand::Tick { now: 10_000 })
        .await;
    assert!(resp.is_none());
    // player2 一条（含 judges 先行？不行——实现为 touches 全部先 flush），
    // 精确断言：player2 的 Touches 1 条（1 帧）、player3 的 Touches 1 条（3 帧）、judges 1 条
    let mut touch_players = std::collections::HashMap::new();
    let mut judge_count = 0usize;
    let mut judge_frames = 0usize;
    for ev in &events {
        match ev {
            RoomEvent::RelayTouches { player, frames, .. } => {
                *touch_players.entry(*player).or_insert(0) += frames.len();
            }
            RoomEvent::RelayJudges { player, judges, .. } => {
                if *player == 2 {
                    judge_count += 1;
                    judge_frames += judges.len();
                }
            }
            _ => panic!("flush 只应产出 Relay* 事件: {ev:?}"),
        }
    }
    assert_eq!(
        touch_players.get(&2),
        Some(&1),
        "player2 应合并为一条/帧数 1"
    );
    assert_eq!(
        touch_players.get(&3),
        Some(&3),
        "player3 应合并为一条/帧数 3"
    );
    assert_eq!(judge_count, 1, "judges 应聚为一条");
    assert_eq!(judge_frames, 0, "空 judges 批合并后仍为空向量");

    // —— 场景 3：abort 玩家残余不播出 ——
    let (_, _) = room
        .handle(
            ctx(2),
            RoomCommand::Touches {
                frames: Arc::new(vec![frame(4.0)]),
            },
        )
        .await; // 入缓冲
    let (_, _) = room.handle(ctx(2), RoomCommand::Abort).await; // 随即 abort
    let (_, events) = room
        .handle(sys_ctx(), RoomCommand::Tick { now: 11_000 })
        .await;
    assert!(
        !events.iter().any(|e| matches!(
            e,
            RoomEvent::RelayTouches { player: 2, .. } | RoomEvent::RelayJudges { player: 2, .. }
        )),
        "abort 后残余应被清理: {events:?}"
    );

    // —— 场景 4：monitor 全部离开 → 缓冲丢弃、无积压、live 变 false ——
    let (_, _) = room.handle(ctx(9), RoomCommand::LeaveRoom).await; // 唯一 monitor 走人
    let (_, _) = room
        .handle(
            ctx(3),
            RoomCommand::Touches {
                frames: Arc::new(vec![frame(5.0)]),
            },
        )
        .await;
    let (_, events) = room
        .handle(sys_ctx(), RoomCommand::Tick { now: 12_000 })
        .await;
    assert!(events.is_empty(), "无观战者不应产出转播: {events:?}");
    // live 关闭状态直接反映（GetClientState.live = monitor 存在性）
    let (resp, _) = room
        .handle(sys_ctx(), RoomCommand::GetClientState { user_id: 1 })
        .await;
    let Some(RoomResponse::ClientState(Some(state))) = resp else {
        panic!("应返回状态: {resp:?}");
    };
    assert!(!state.live, "monitor 走人后 live 应为 false");

    // —— 场景 4b：回 SelectChart 后 monitor 重入 → 转播恢复 ——
    // （Playing 中不能加入，§6.5-3，join_during_game_rejected 已覆盖；
    //   这里全员 abort 触发 GameEnd → 回 SelectChart 再重入）
    let (_, _) = room.handle(ctx(3), RoomCommand::Abort).await;
    let (_, events) = room.handle(ctx(1), RoomCommand::Abort).await; // host 补 abort → 全员完成
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RoomEvent::GameEnd { .. }))
    );
    let (_, _) = room
        .handle(
            ctx(9),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: true,
                name: "mon".to_owned(),
            },
        )
        .await;
    let (_, _) = room
        .handle(
            ctx(3),
            RoomCommand::Touches {
                frames: Arc::new(vec![frame(6.0)]),
            },
        )
        .await;
    let (_, events) = room
        .handle(sys_ctx(), RoomCommand::Tick { now: 13_000 })
        .await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RoomEvent::RelayTouches { player: 3, .. })),
        "monitor 回归后应恢复转播: {events:?}"
    );
}
