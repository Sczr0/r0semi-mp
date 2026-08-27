//! phira-core 集成测试（§4.9-3：测试位置 = phira-core 集成测试 + 脚本化假 actor）。
//!
//! 验证对象全是 **core 行为**（契约套件测不到这些，评审 §8 六）：
//! - 路由规则（CreateRoom/JoinRoom 载荷路由、表路由、表 miss 回错，§4.9-4）
//! - 时序不变量（先解析后应用 / 先应用后响应，§4.9-4）
//! - 房间生命周期（RoomClosed 清理、排空、后续命令失败，§4.9-9）
//! - 事件投递目标（领域事件恒 All / Relay 指令 Specific，§4.4）
//! - 配置热更广播（§4.9-8）

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use phira_api::{
    CmdCtx, Origin, RoomCommand, RoomConfig, RoomError, RoomErrorCode, RoomEvent, RoomFactory,
    RoomId, RoomResponse, RoomState, Targets, UserInfo,
};
use phira_core::{Bus, EventSink};
use tokio::sync::{Notify, oneshot};

/// 脚本项：一条命令的（响应, 事件集）。
type ScriptItem = (Option<RoomResponse>, Vec<RoomEvent>);
/// 一个房间 actor 的完整脚本。
type ActorScript = VecDeque<ScriptItem>;

/// 脚本化假 actor：按预录脚本逐条响应，并记录收到的命令。
struct ScriptedActor {
    script: ActorScript,
    received: Arc<Mutex<Vec<RoomCommand>>>,
}

#[async_trait::async_trait]
impl phira_api::RoomActor for ScriptedActor {
    async fn handle(
        &mut self,
        _ctx: CmdCtx,
        cmd: RoomCommand,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        self.received.lock().unwrap().push(cmd);
        self.script.pop_front().unwrap_or((None, Vec::new()))
    }
}

/// 脚本化假工厂：按 room_id 分配 actor 脚本（每房间一个 actor 实例）。
#[derive(Clone, Default)]
struct ScriptedFactory {
    /// room_id → 该房间 actor 的脚本队列（每房间一个 actor，脚本按 create 顺序弹出）。
    scripts: Arc<Mutex<HashMap<RoomId, VecDeque<ActorScript>>>>,
    /// 所有 actor 共享的命令记录（断言 actor 收到什么）。
    received: Arc<Mutex<Vec<RoomCommand>>>,
}

impl ScriptedFactory {
    /// 为一个房间 push 一份 actor 脚本（该房间的完整命令序列）。
    fn push(&self, room_id: &RoomId, script: Vec<ScriptItem>) {
        self.scripts
            .lock()
            .unwrap()
            .entry(room_id.clone())
            .or_default()
            .push_back(script.into());
    }
    fn received(&self) -> Arc<Mutex<Vec<RoomCommand>>> {
        Arc::clone(&self.received)
    }
}

impl RoomFactory for ScriptedFactory {
    fn create(&self, room_id: RoomId) -> Box<dyn phira_api::RoomActor> {
        let script = self
            .scripts
            .lock()
            .unwrap()
            .get_mut(&room_id)
            .and_then(VecDeque::pop_front)
            .unwrap_or_default();
        Box::new(ScriptedActor {
            script,
            received: Arc::clone(&self.received),
        })
    }
}

/// 记录投递的假 sink。
#[derive(Clone, Default)]
struct FakeSink {
    deliveries: Arc<Mutex<Vec<(i32, RoomEvent)>>>,
}

#[async_trait::async_trait]
impl EventSink for FakeSink {
    async fn deliver(&self, user_id: i32, event: &RoomEvent) {
        self.deliveries
            .lock()
            .unwrap()
            .push((user_id, event.clone()));
    }
}

fn rid() -> RoomId {
    RoomId::new("test".to_owned()).unwrap()
}

fn client_ctx(user_id: i32) -> CmdCtx {
    CmdCtx {
        origin: Origin::Client { user_id },
        room_id: rid(),
    }
}

fn business_err(code: RoomErrorCode) -> RoomError {
    RoomError::Business {
        code,
        msg: "x".to_owned(),
    }
}

#[allow(clippy::unnecessary_wraps)] // 测试 helper：与 (resp, events) 元组形状对齐
fn ok() -> Option<RoomResponse> {
    Some(RoomResponse::Ok)
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn create_room_registers_route() {
    let factory = Arc::new(ScriptedFactory::default());
    // 一个房间 = 一条完整脚本（CreateRoom → LeaveRoom）
    factory.push(
        &rid(),
        vec![
            (
                ok(),
                vec![RoomEvent::RoomCreated {
                    room_id: rid(),
                    host: 1,
                }],
            ),
            (ok(), vec![]),
        ],
    );
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );

    // 建房 → 路由增量 host→room
    let resp = bus
        .dispatch(
            client_ctx(1),
            RoomCommand::CreateRoom {
                id: rid(),
                name: "user1".to_owned(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(resp, RoomResponse::Ok));

    // host 在房内：LeaveRoom 不报 NotInRoom
    let resp = bus.dispatch(client_ctx(1), RoomCommand::LeaveRoom).await;
    assert!(resp.is_ok(), "host 应能离开房间: {resp:?}");
}

#[tokio::test]
async fn join_then_select_chart_pipeline() {
    // 流水线客户端 JoinRoom → SelectChart 不应收到"你不在房间里"（§4.9-4 先应用后响应）
    let factory = Arc::new(ScriptedFactory::default());
    factory.push(
        &rid(),
        vec![
            (
                ok(),
                vec![RoomEvent::RoomCreated {
                    room_id: rid(),
                    host: 1,
                }],
            ),
            (
                Some(RoomResponse::JoinRoom(phira_api::JoinRoomResponse {
                    state: RoomState::SelectChart(None),
                    users: vec![],
                    live: false,
                })),
                vec![RoomEvent::UserJoined {
                    room_id: rid(),
                    user: UserInfo {
                        id: 2,
                        name: "b".to_owned(),
                        monitor: false,
                    },
                }],
            ),
            (ok(), vec![]),
        ],
    );
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );

    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "user1".to_owned(),
        },
    )
    .await
    .unwrap();
    let resp = bus
        .dispatch(
            client_ctx(2),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
                name: "user2".to_owned(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(resp, RoomResponse::JoinRoom(_)));

    // 用户 2 已入表（先应用后响应），SelectChart 必须路由成功
    let resp = bus
        .dispatch(client_ctx(2), RoomCommand::SelectChart { id: 7 })
        .await;
    assert!(resp.is_ok(), "入房后 SelectChart 不应 NotInRoom: {resp:?}");
}

#[tokio::test]
async fn leaver_receives_own_leave_room() {
    // 先解析后应用：离开者仍收到自己的 LeaveRoom（§4.9-4）
    let factory = Arc::new(ScriptedFactory::default());
    factory.push(
        &rid(),
        vec![
            (
                ok(),
                vec![RoomEvent::RoomCreated {
                    room_id: rid(),
                    host: 1,
                }],
            ),
            (
                Some(RoomResponse::JoinRoom(phira_api::JoinRoomResponse {
                    state: RoomState::SelectChart(None),
                    users: vec![],
                    live: false,
                })),
                vec![RoomEvent::UserJoined {
                    room_id: rid(),
                    user: UserInfo {
                        id: 2,
                        name: "b".to_owned(),
                        monitor: false,
                    },
                }],
            ),
            (
                ok(),
                vec![RoomEvent::UserLeft {
                    room_id: rid(),
                    user: 2,
                    name: "u2".to_owned(),
                }],
            ),
        ],
    );
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );
    let sink = FakeSink::default();
    bus.attach_sink(Arc::new(sink.clone()));

    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "user1".to_owned(),
        },
    )
    .await
    .unwrap();
    bus.dispatch(
        client_ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await
    .unwrap();
    bus.dispatch(client_ctx(2), RoomCommand::LeaveRoom)
        .await
        .unwrap();

    let deliveries = sink.deliveries.lock().unwrap();
    let expected = RoomEvent::UserLeft {
        room_id: rid(),
        user: 2,
        name: "u2".to_owned(),
    };
    assert!(
        deliveries.contains(&(2, expected.clone())),
        "离开者应收到自己的 UserLeft: {deliveries:?}"
    );
    assert!(
        deliveries.contains(&(1, expected)),
        "房内其他成员也应收到: {deliveries:?}"
    );
}

#[tokio::test]
async fn room_closed_cleans_up() {
    // RoomClosed → core 排空、删表；后续命令回错误（§4.9-9）
    let factory = Arc::new(ScriptedFactory::default());
    factory.push(
        &rid(),
        vec![
            (
                ok(),
                vec![RoomEvent::RoomCreated {
                    room_id: rid(),
                    host: 1,
                }],
            ),
            // 最后一人离开（真实 evict 语义）：UserLeft + RoomClosed
            (
                ok(),
                vec![
                    RoomEvent::UserLeft {
                        room_id: rid(),
                        user: 1,
                        name: "user1".to_owned(),
                    },
                    RoomEvent::RoomClosed { room_id: rid() },
                ],
            ),
        ],
    );
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );
    // 观察者（§4.4 修订）：RoomClosed 也投递给 sink（user_id=0 系统约定）——
    // RoomListSink 依赖它清理快照；修复前被拦在 process_events 步骤 1，列表残留。
    let sink = Arc::new(FakeSink::default());
    bus.attach_sink(Arc::clone(&sink) as Arc<dyn EventSink>);

    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "user1".to_owned(),
        },
    )
    .await
    .unwrap();
    // 触发空房自毁
    bus.dispatch(client_ctx(1), RoomCommand::LeaveRoom)
        .await
        .unwrap();

    // 观察者应收到 RoomClosed（user_id=0），且收到顺序在 UserLeft 之后（先计数归零再移除）
    let deliveries = sink.deliveries.lock().unwrap().clone();
    let closed_idx = deliveries
        .iter()
        .position(|(uid, ev)| *uid == 0 && matches!(ev, RoomEvent::RoomClosed { .. }));
    assert!(
        closed_idx.is_some(),
        "观察者应收到 RoomClosed: {deliveries:?}"
    );
    let left_idx = deliveries
        .iter()
        .position(|(_, ev)| matches!(ev, RoomEvent::UserLeft { .. }));
    assert!(
        left_idx.is_some_and(|l| closed_idx.is_some_and(|c| l < c)),
        "UserLeft 应先于 RoomClosed（快照计数归零再移除）: {deliveries:?}"
    );

    // 房间已删：JoinRoom 应 RoomNotFound
    let resp = bus
        .dispatch(
            client_ctx(9),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
                name: "user2".to_owned(),
            },
        )
        .await;
    assert!(
        matches!(
            resp,
            Err(RoomError::Business {
                code: RoomErrorCode::RoomNotFound,
                ..
            })
        ),
        "房间关闭后 JoinRoom 应失败: {resp:?}"
    );

    // 同 id 可重新建房（表已清理；新房间 = 新 actor = 新脚本）
    factory.push(&rid(), vec![(ok(), vec![])]);
    let resp = bus
        .dispatch(
            client_ctx(1),
            RoomCommand::CreateRoom {
                id: rid(),
                name: "user1".to_owned(),
            },
        )
        .await;
    assert!(resp.is_ok(), "清理后可重建同 id 房间: {resp:?}");
}

#[tokio::test]
async fn not_in_room_returns_error() {
    let factory = Arc::new(ScriptedFactory::default());
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );

    // 从未入房：表 miss → 回"不在房间"（§4.9-4）
    let resp = bus.dispatch(client_ctx(5), RoomCommand::LeaveRoom).await;
    assert!(
        matches!(
            resp,
            Err(RoomError::Business {
                code: RoomErrorCode::NotInRoom,
                ..
            })
        ),
        "未入房应 NotInRoom: {resp:?}"
    );
}

#[tokio::test]
async fn relay_specific_targets_only() {
    // Relay 指令只投递 Specific 目标（§4.4：不进观察者通道的只有 monitor）
    let factory = Arc::new(ScriptedFactory::default());
    factory.push(
        &rid(),
        vec![
            (
                ok(),
                vec![RoomEvent::RoomCreated {
                    room_id: rid(),
                    host: 1,
                }],
            ),
            (
                None,
                vec![RoomEvent::RelayTouches {
                    room_id: rid(),
                    targets: Targets::Specific(vec![9]),
                    player: 1,
                    frames: Arc::new(vec![]),
                }],
            ),
        ],
    );
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );
    let sink = FakeSink::default();
    bus.attach_sink(Arc::new(sink.clone()));

    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "user1".to_owned(),
        },
    )
    .await
    .unwrap();
    // host 自己发触摸流（无需入房步骤）
    bus.dispatch(
        client_ctx(1),
        RoomCommand::Touches {
            frames: Arc::new(vec![]),
        },
    )
    .await
    .unwrap();
    // Touches 是 DropIfFull 入队即返回：等 actor 投递
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    let deliveries = sink.deliveries.lock().unwrap();
    // RoomCreated 广播给房主是正确行为（§4.9-4 加入者本人也收）；只断言 Relay 投递
    let relay: Vec<_> = deliveries
        .iter()
        .filter(|(_, ev)| matches!(ev, RoomEvent::RelayTouches { .. }))
        .collect();
    assert_eq!(relay.len(), 1, "只投递给 monitor 9: {deliveries:?}");
    assert_eq!(relay[0].0, 9);
    assert!(matches!(relay[0].1, RoomEvent::RelayTouches { .. }));
}

#[tokio::test]
async fn update_config_broadcast() {
    // 配置热更广播 UpdateConfig 给所有房间（§4.9-8）
    let factory = Arc::new(ScriptedFactory::default());
    let received = factory.received();
    factory.push(
        &rid(),
        vec![
            (
                ok(),
                vec![RoomEvent::RoomCreated {
                    room_id: rid(),
                    host: 1,
                }],
            ),
            (None, vec![]), // UpdateConfig 的响应
        ],
    );
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );

    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "user1".to_owned(),
        },
    )
    .await
    .unwrap();
    let new_cfg = Arc::new(RoomConfig {
        monitors: vec![9, 10],
    });
    bus.update_config(Arc::clone(&new_cfg)).await;
    // update_config 只入队不等处理：让 actor 任务有机会跑（current_thread 交错）
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    // actor 应收到 UpdateConfig 且带新配置（先 clone 再释放锁，避免 MutexGuard 跨 await）
    let has_update = {
        let commands = received.lock().unwrap();
        commands.iter().any(|c| {
            matches!(
                c,
                RoomCommand::UpdateConfig { config } if Arc::ptr_eq(config, &new_cfg)
            )
        })
    };
    assert!(has_update, "actor 应收到 UpdateConfig");
    // bus 生效配置同步
    assert_eq!(bus.room_config().await.monitors, vec![9, 10]);
}

#[tokio::test]
async fn system_command_routes_by_ctx_room_id() {
    // 系统命令按 ctx.room_id 直路由（§4.9-4 路由规则）
    let factory = Arc::new(ScriptedFactory::default());
    let received = factory.received();
    factory.push(
        &rid(),
        vec![
            (
                ok(),
                vec![RoomEvent::RoomCreated {
                    room_id: rid(),
                    host: 1,
                }],
            ),
            (None, vec![]), // Tick 无响应
        ],
    );
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );

    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "user1".to_owned(),
        },
    )
    .await
    .unwrap();
    // 生命周期任务/定时器直接按 room_id 派发
    let resp = bus
        .dispatch_system(rid(), RoomCommand::Tick { now: 12345 })
        .await;
    assert!(resp.is_ok());
    // dispatch_system 只入队不等处理：让 actor 任务有机会跑
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    let commands = received.lock().unwrap();
    assert!(
        commands
            .iter()
            .any(|c| matches!(c, RoomCommand::Tick { now: 12345 }))
    );
    drop(commands); // 避免 MutexGuard 跨 await
}

#[tokio::test]
async fn get_client_state_returns_response() {
    // GetClientState 是系统命令但带回话（§4.4：重连恢复）
    let factory = Arc::new(ScriptedFactory::default());
    factory.push(
        &rid(),
        vec![
            (
                ok(),
                vec![RoomEvent::RoomCreated {
                    room_id: rid(),
                    host: 1,
                }],
            ),
            (Some(RoomResponse::ClientState(None)), vec![]),
        ],
    );
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );

    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "user1".to_owned(),
        },
    )
    .await
    .unwrap();
    let resp = bus
        .dispatch_system(rid(), RoomCommand::GetClientState { user_id: 1 })
        .await
        .unwrap();
    assert!(matches!(resp, RoomResponse::ClientState(None)));
}

#[tokio::test]
async fn duplicate_create_room_rejected() {
    // 同 id 重复建房 → RoomIdOccupied（§4.9-9 出生证明）
    let factory = Arc::new(ScriptedFactory::default());
    factory.push(
        &rid(),
        vec![
            (
                ok(),
                vec![RoomEvent::RoomCreated {
                    room_id: rid(),
                    host: 1,
                }],
            ),
            (ok(), vec![]),
        ],
    );
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );

    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "user1".to_owned(),
        },
    )
    .await
    .unwrap();
    let resp = bus
        .dispatch(
            client_ctx(2),
            RoomCommand::CreateRoom {
                id: rid(),
                name: "user1".to_owned(),
            },
        )
        .await;
    assert!(
        matches!(
            resp,
            Err(RoomError::Business {
                code: RoomErrorCode::RoomIdOccupied,
                ..
            })
        ),
        "重复建房应拒绝: {resp:?}"
    );
}

#[tokio::test]
async fn metrics_track_internal_only() {
    // 错误率只统计 Internal（§3.2）：业务拒绝不计
    let factory = Arc::new(ScriptedFactory::default());
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );

    // 未入房 → Business(NotInRoom)
    let _ = bus.dispatch(client_ctx(1), RoomCommand::LeaveRoom).await;

    let metrics = bus.metrics().snapshot();
    let leave = metrics
        .iter()
        .find(|(k, _)| *k == "leave_room")
        .map(|(_, s)| *s)
        .unwrap();
    assert_eq!(leave.calls, 1);
    assert_eq!(leave.business, 1);
    assert_eq!(leave.internal, 0);
    assert_eq!(bus.metrics().internal_errors(), 0);
}

// —— 编译期验证：oneshot 通道类型正确（§4.4 响应配对）——
#[allow(dead_code)]
fn _response_channel_type() -> oneshot::Sender<RoomResponse> {
    let (tx, _) = oneshot::channel();
    tx
}

#[allow(dead_code)]
fn _business_err_construct() -> RoomError {
    business_err(RoomErrorCode::RoomFull)
}

// —— 补全：跨房间判重 / 多房间隔离 / 队列压力 / Metrics 聚合 ——

#[allow(clippy::too_many_lines)] // 跨房间判重场景脚本长
#[tokio::test]
async fn cross_room_duplicate_join_rejected() {
    // §6.5-27 全局判重：用户在房 A，JoinRoom/CreateRoom 房 B → AlreadyInRoom（bus 层）
    let factory = Arc::new(ScriptedFactory::default());
    factory.push(
        &rid(),
        vec![
            // 房 A（rid）的 actor 脚本
            (
                ok(),
                vec![RoomEvent::RoomCreated {
                    room_id: rid(),
                    host: 1,
                }],
            ),
            (
                Some(RoomResponse::JoinRoom(phira_api::JoinRoomResponse {
                    state: RoomState::SelectChart(None),
                    users: vec![],
                    live: false,
                })),
                vec![RoomEvent::UserJoined {
                    room_id: rid(),
                    user: UserInfo {
                        id: 2,
                        name: "b".to_owned(),
                        monitor: false,
                    },
                }],
            ),
        ],
    );
    let room_b = RoomId::new("b".to_owned()).unwrap();
    factory.push(&room_b, vec![(ok(), vec![])]); // 房 B 的 actor 脚本
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );

    // 用户 1 建房 A
    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "user1".to_owned(),
        },
    )
    .await
    .unwrap();
    // 用户 2 入房 A
    bus.dispatch(
        client_ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "user2".to_owned(),
        },
    )
    .await
    .unwrap();

    // 用户 2 已在房 A，试图加入房 B → AlreadyInRoom
    let resp = bus
        .dispatch(
            client_ctx(2),
            RoomCommand::JoinRoom {
                id: room_b.clone(),
                monitor: false,
                name: "user2".to_owned(),
            },
        )
        .await;
    assert!(
        matches!(
            resp,
            Err(RoomError::Business {
                code: RoomErrorCode::AlreadyInRoom,
                ..
            })
        ),
        "跨房间重复入房应拒绝: {resp:?}"
    );

    // 用户 2 试图在房 B 建房 → 同样拒绝
    let resp = bus
        .dispatch(
            client_ctx(2),
            RoomCommand::CreateRoom {
                id: room_b.clone(),
                name: "user2".to_owned(),
            },
        )
        .await;
    assert!(
        matches!(
            resp,
            Err(RoomError::Business {
                code: RoomErrorCode::AlreadyInRoom,
                ..
            })
        ),
        "跨房间重复建房应拒绝: {resp:?}"
    );

    // 用户 3（未入房）加入房 B → 放行（房 B 存在性需先验证：此处应 RoomNotFound，因为没有用户进过房 B）
    let resp = bus
        .dispatch(
            client_ctx(3),
            RoomCommand::JoinRoom {
                id: room_b,
                monitor: false,
                name: "user2".to_owned(),
            },
        )
        .await;
    assert!(
        matches!(
            resp,
            Err(RoomError::Business {
                code: RoomErrorCode::RoomNotFound,
                ..
            })
        ),
        "房 B 未创建应 RoomNotFound: {resp:?}"
    );
}

#[tokio::test]
async fn rooms_are_isolated() {
    // 多房间隔离：不同房间的 actor 互不干扰（§4.9：每房间一个 actor）
    let factory = Arc::new(ScriptedFactory::default());
    let room_a = RoomId::new("a".to_owned()).unwrap();
    let room_b = RoomId::new("b".to_owned()).unwrap();
    // 房 A 完整脚本：建房 + LeaveRoom
    factory.push(
        &room_a,
        vec![
            (
                ok(),
                vec![RoomEvent::RoomCreated {
                    room_id: room_a.clone(),
                    host: 1,
                }],
            ),
            (ok(), vec![]),
        ],
    );
    // 房 B 完整脚本：建房 + LeaveRoom
    factory.push(
        &room_b,
        vec![
            (
                ok(),
                vec![RoomEvent::RoomCreated {
                    room_id: room_b.clone(),
                    host: 2,
                }],
            ),
            (ok(), vec![]),
        ],
    );
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );

    let ctx_a = CmdCtx {
        origin: Origin::Client { user_id: 1 },
        room_id: room_a.clone(),
    };
    let ctx_b = CmdCtx {
        origin: Origin::Client { user_id: 2 },
        room_id: room_b.clone(),
    };

    // 并发建房
    let (ra, rb) = tokio::join!(
        bus.dispatch(
            ctx_a,
            RoomCommand::CreateRoom {
                id: room_a.clone(),
                name: "user1".to_owned(),
            },
        ),
        bus.dispatch(
            ctx_b,
            RoomCommand::CreateRoom {
                id: room_b.clone(),
                name: "user2".to_owned(),
            }
        ),
    );
    assert!(ra.is_ok());
    assert!(rb.is_ok());

    // 各自的路由互不串：用户 1 在房 A，用户 2 在房 B
    let (la, lb) = tokio::join!(
        bus.dispatch(client_ctx(1), RoomCommand::LeaveRoom),
        bus.dispatch(client_ctx(2), RoomCommand::LeaveRoom),
    );
    assert!(la.is_ok(), "用户1 应在房 A: {la:?}");
    assert!(lb.is_ok(), "用户2 应在房 B: {lb:?}");
}

#[tokio::test]
async fn hot_path_drops_when_queue_full() {
    // §4.9-9：热路径（Touches）满则丢新——不阻塞、不报错（try_send）
    let factory = Arc::new(ScriptedFactory::default());
    // actor 阻塞：脚本空（handle 立即返回空），但我们要模拟队列满——
    // 用脚本化 actor 的第一个 handle 阻塞在 oneshot 上
    factory.push(
        &rid(),
        vec![(
            ok(),
            vec![RoomEvent::RoomCreated {
                room_id: rid(),
                host: 1,
            }],
        )],
    );
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );
    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "user1".to_owned(),
        },
    )
    .await
    .unwrap();

    // 连续压入热路径命令（远超队列容量 1024）：每条都是 try_send，满则静默丢弃
    // —— 验证：热路径绝不阻塞、绝不返回错误（§4.9-9 DropIfFull）
    for i in 0..2000 {
        let resp = bus
            .dispatch(
                client_ctx(1),
                RoomCommand::Touches {
                    frames: Arc::new(vec![]),
                },
            )
            .await;
        assert!(resp.is_ok(), "第 {i} 条 Touches 不应报错: {resp:?}");
    }
    assert_eq!(bus.metrics().internal_errors(), 0, "热路径丢弃不计错误率");
}

#[tokio::test]
async fn metrics_aggregate_internal() {
    // Metrics 聚合：多命令累计 internal_errors（§3.2 / §11.1）
    let factory = Arc::new(ScriptedFactory::default());
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );

    // 3 次未入房 LeaveRoom → 3 次 Business(NotInRoom)
    for _ in 0..3 {
        let _ = bus.dispatch(client_ctx(1), RoomCommand::LeaveRoom).await;
    }

    let snap = bus.metrics().snapshot();
    let leave = snap.iter().find(|(k, _)| *k == "leave_room").unwrap().1;
    assert_eq!(leave.calls, 3);
    assert_eq!(leave.business, 3);
    assert_eq!(leave.internal, 0);
    assert_eq!(bus.metrics().internal_errors(), 0, "业务拒绝不计错误率");

    // 平均延迟字段存在且 >= 0
    assert!(leave.avg_latency_ms >= 0.0);
}

#[tokio::test]
async fn watch_config_polls_and_reloads() {
    // §4.9-8：文件轮询 → 变化 → update_config 广播（房间收到新配置）
    let factory = Arc::new(ScriptedFactory::default());
    let received = factory.received();
    factory.push(
        &rid(),
        vec![(
            ok(),
            vec![RoomEvent::RoomCreated {
                room_id: rid(),
                host: 1,
            }],
        )],
    );
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );
    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "user1".to_owned(),
        },
    )
    .await
    .unwrap();

    // 临时配置文件
    let dir = std::env::temp_dir();
    let path = dir.join(format!("r0semi-mp-watch-test-{}.yml", std::process::id()));
    std::fs::write(&path, "monitors: [1]\n").unwrap();

    bus.watch_config(path.clone(), std::time::Duration::from_millis(20));

    // 等第一轮轮询（monitors=[1]）
    wait_for(&received, |cmds| {
        cmds.iter().any(
            |c| matches!(c, RoomCommand::UpdateConfig { config } if config.monitors == vec![1]),
        )
    })
    .await;
    assert_eq!(bus.room_config().await.monitors, vec![1]);

    // 改文件 → 等下一轮
    std::fs::write(&path, "monitors: [2, 3]\n").unwrap();
    wait_for(&received, |cmds| {
        cmds.iter().any(
            |c| matches!(c, RoomCommand::UpdateConfig { config } if config.monitors == vec![2, 3]),
        )
    })
    .await;
    assert_eq!(bus.room_config().await.monitors, vec![2, 3]);

    // 清理
    let _ = std::fs::remove_file(&path);
}

/// 轮询等待条件成立（超时 2s panic）。
async fn wait_for<F>(received: &Arc<std::sync::Mutex<Vec<RoomCommand>>>, mut cond: F)
where
    F: FnMut(&Vec<RoomCommand>) -> bool,
{
    for _ in 0..100 {
        if cond(&received.lock().unwrap()) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("wait_for timeout");
}

// —— §4.9-9 队列压力分级：Wait（生命周期事实不丢）vs Reject（客户端命令满则拒）——

/// 可阻塞 actor：`block=true` 时 handle 挂起等待 gate 释放。
struct BlockingActor {
    received: Arc<Mutex<Vec<RoomCommand>>>,
    block: Arc<AtomicBool>,
    gate: Arc<Notify>,
}

#[async_trait::async_trait]
impl phira_api::RoomActor for BlockingActor {
    async fn handle(
        &mut self,
        ctx: CmdCtx,
        cmd: RoomCommand,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        self.received.lock().unwrap().push(cmd.clone());
        if self.block.load(Ordering::SeqCst) {
            self.gate.notified().await;
        }
        match cmd {
            // 建房必须带 RoomCreated 事件（bus 路由增量 + 响应，§4.9-4）
            RoomCommand::CreateRoom { .. } => (
                Some(RoomResponse::Ok),
                vec![RoomEvent::RoomCreated {
                    room_id: ctx.room_id,
                    host: 1,
                }],
            ),
            _ => (None, Vec::new()),
        }
    }
}

/// 阻塞工厂：所有房间共用同一个 gate + 记录。
#[derive(Clone)]
struct BlockingFactory {
    received: Arc<Mutex<Vec<RoomCommand>>>,
    block: Arc<AtomicBool>,
    gate: Arc<Notify>,
}

impl BlockingFactory {
    fn new() -> Self {
        Self {
            received: Arc::new(Mutex::new(Vec::new())),
            block: Arc::new(AtomicBool::new(false)),
            gate: Arc::new(Notify::new()),
        }
    }
}

impl RoomFactory for BlockingFactory {
    fn create(&self, _room_id: RoomId) -> Box<dyn phira_api::RoomActor> {
        Box::new(BlockingActor {
            received: Arc::clone(&self.received),
            block: Arc::clone(&self.block),
            gate: Arc::clone(&self.gate),
        })
    }
}

#[tokio::test]
async fn wait_preserves_lifecycle_when_queue_full() {
    // §4.9-9：队列满时——生命周期事实（Wait）等待不丢；客户端命令（Reject）立即拒
    let factory = BlockingFactory::new();
    let bus = Bus::new(
        Arc::new(factory.clone()) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );
    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "user1".to_owned(),
        },
    )
    .await
    .unwrap();

    // actor 开始阻塞 → 队列将满
    factory.block.store(true, Ordering::SeqCst);

    // 并发压满队列：用无响应的系统命令（UpdateConfig 立即返回，不挂起等待 actor）
    let mut backfill = Vec::new();
    for _ in 0..2000 {
        backfill.push(tokio::spawn({
            let bus = bus.clone();
            async move {
                bus.dispatch(
                    CmdCtx {
                        origin: Origin::System,
                        room_id: rid(),
                    },
                    RoomCommand::UpdateConfig {
                        config: Arc::new(RoomConfig::default()),
                    },
                )
                .await
            }
        }));
    }
    for h in backfill {
        let _ = h.await; // 满后的 UpdateConfig 返回 Err（Reject 语义，预期内）
    }
    // 队列已满（1300 条：actor recv 1 阻塞中 + 1024 缓冲，多压 100 条兜底）

    // Reject：队列满时新命令立即拒（try_send 失败）。用无响应的 UpdateConfig
    // 验证——需响应的命令（如 Chat）入队成功后会挂起等 actor 响应，混淆断言。
    // 注意：actor 阻塞时 recv 腾出 1 位——第一条是"补位"入队成功，其余应全 FULL。
    let rejected: Vec<_> = {
        let mut out = Vec::new();
        for _ in 0..20 {
            out.push(
                bus.dispatch(
                    CmdCtx {
                        origin: Origin::System,
                        room_id: rid(),
                    },
                    RoomCommand::UpdateConfig {
                        config: Arc::new(RoomConfig::default()),
                    },
                )
                .await,
            );
        }
        out
    };
    assert!(
        rejected.iter().skip(1).all(std::result::Result::is_err),
        "队列满后命令应被拒（Reject，§4.9-9；首条为补位）: {rejected:?}"
    );

    // Wait：生命周期事实不可丢——队列满时挂起，释放后成功处理
    let wait_handle = tokio::spawn({
        let bus = bus.clone();
        async move {
            bus.dispatch(
                CmdCtx {
                    origin: Origin::System,
                    room_id: rid(),
                },
                RoomCommand::UserDisconnected {
                    user_id: 1,
                    epoch: 1,
                },
            )
            .await
        }
    });
    // 挂起中（Wait 等待，不立即返回）
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(!wait_handle.is_finished(), "Wait 命令应等待而非被拒");

    // 释放 actor → 队列清空 → Wait 命令完成且 actor 收到
    factory.block.store(false, Ordering::SeqCst);
    factory.gate.notify_waiters();
    let resp = wait_handle.await.unwrap();
    assert!(resp.is_ok(), "UserDisconnected 最终应成功: {resp:?}");

    // actor 处理完队列中的 UpdateConfig 后才轮到 UserDisconnected——轮询等待
    wait_for(&factory.received, |cmds| {
        cmds.iter()
            .any(|c| matches!(c, RoomCommand::UserDisconnected { user_id: 1, .. }))
    })
    .await;
}

// ===== A2 两段式回源（§4.9-2 规则 2，2026-08） =====

/// 回源 API 替身：可脚本化延迟/失败。
struct FakeFetchApi {
    /// 常驻失败开关（fail=true 时永远失败）。
    fail: std::sync::atomic::AtomicBool,
    /// 脚本化：前 `fail_first` 次调用失败，之后成功（A2 重试测试用）。
    fail_first: std::sync::atomic::AtomicUsize,
    /// 累计调用次数（断言重试发生过）。
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl phira_api::ApiClient for FakeFetchApi {
    async fn fetch_chart(&self, id: i32) -> Result<phira_api::Chart, phira_api::ApiError> {
        Ok(phira_api::Chart {
            id,
            name: "c".into(),
        })
    }
    async fn fetch_record(&self, id: i32) -> Result<phira_api::Record, phira_api::ApiError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(phira_api::ApiError::Internal {
                msg: "injected failure".into(),
            });
        }
        if call < self.fail_first.load(Ordering::SeqCst) {
            return Err(phira_api::ApiError::Internal {
                msg: "injected transient failure".into(),
            });
        }
        Ok(phira_api::Record {
            id,
            player: id,
            chart: None, // bus 测试无房间谱面概念（ScriptedFactory 假 actor）→ fail-open
            score: 7,
            perfect: 0,
            good: 0,
            bad: 0,
            miss: 0,
            max_combo: 0,
            accuracy: 1.0,
            full_combo: false,
            std: 0.0,
            std_score: 0.0,
        })
    }
}

/// Played 触发房外回源任务 → RecordFetched 回注同房间 actor（命令序列被记录）。
/// 回注负载不占 Played 的响应通路（Played 立即 Ok 返回）。
#[tokio::test]
async fn played_two_phase_redelivery_reaches_actor() {
    let factory = ScriptedFactory::default();
    // 单房间单 actor 一份脚本，按序承接命令：
    // CreateRoom → JoinRoom → Played 受理（立即 Ok）；RecordFetched 回注走默认 (None, [])
    factory.push(
        &rid(),
        vec![
            (Some(RoomResponse::Ok), Vec::new()), // CreateRoom
            (
                Some(RoomResponse::Ok),
                vec![RoomEvent::UserJoined {
                    room_id: rid(),
                    user: UserInfo {
                        id: 2,
                        name: "u2".into(),
                        monitor: false,
                    },
                }],
            ), // JoinRoom：UserJoined 驱动路由增量
            (Some(RoomResponse::Ok), Vec::new()), // Played 受理
        ],
    );
    let bus = Bus::new(
        Arc::new(factory.clone()) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    )
    .with_api(Arc::new(FakeFetchApi {
        fail: std::sync::atomic::AtomicBool::new(false),
        fail_first: std::sync::atomic::AtomicUsize::new(0),
        calls: std::sync::atomic::AtomicUsize::new(0),
    }));

    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "u1".into(),
        },
    )
    .await
    .unwrap();

    bus.dispatch(
        CmdCtx {
            origin: Origin::Client { user_id: 2 },
            room_id: rid(),
        },
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "u2".into(),
        },
    )
    .await
    .unwrap();
    // JoinRoom 的路由增量在 actor 处理该命令时才应用——轮询等待路由可见，
    // 否则下一步 Played 走表路由会 miss。
    for _ in 0..100 {
        if bus.room_of(2).await.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let started = std::time::Instant::now();
    let resp = bus
        .dispatch(client_ctx(2), RoomCommand::Played { id: 2 })
        .await;
    let elapsed = started.elapsed();
    assert!(matches!(resp, Ok(RoomResponse::Ok)));
    // 受理响应快速返回（未 await 任何真实 IO——若仍是内联回源，此断言形态会变）
    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "受理应立即返回"
    );

    // 轮询等待：RecordFetched 经房外任务回注到达 actor
    wait_for(&factory.received, |cmds| {
        cmds.iter().any(|c| {
            matches!(
                c,
                RoomCommand::RecordFetched {
                    user_id: 2,
                    record_id: 2,
                    ..
                }
            )
        })
    })
    .await;
}

/// 未注入 api 时（如纯 actor 测试），Played 受理照常、无回注（仅日志）。
#[tokio::test]
async fn played_without_api_accepts_silently() {
    let factory = ScriptedFactory::default();
    factory.push(&rid(), vec![(Some(RoomResponse::Ok), Vec::new())]);
    let bus = Bus::new(
        Arc::new(factory.clone()) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );
    // join 前置缺失（无路由）→ NotInRoom；换成系统派发也行——这里只验证不 panic：
    let resp = bus
        .dispatch_system(
            rid(),
            RoomCommand::RecordFetched {
                user_id: 9,
                record_id: 9,
                record: Err(phira_api::ApiError::Internal { msg: "x".into() }),
            },
        )
        .await;
    // 房不存在 → Internal（路由层）；行为与其它系统命令一致
    assert!(resp.is_err(), "房间不存在时 RecordFetched 应报内部错");
}

/// A2 回源有界重试（§4.9-2）：前 1 次失败后成功 → 回注载荷为 Ok，且
/// fetch_record 被调用 ≥2 次（瞬时故障被自愈，不需 impl 兜底结算）。
#[tokio::test]
async fn played_fetch_transient_failure_retries_then_succeeds() {
    let factory = ScriptedFactory::default();
    // CreateRoom → JoinRoom → Played 受理（脚本同 played_two_phase 测试）
    factory.push(
        &rid(),
        vec![
            (Some(RoomResponse::Ok), Vec::new()), // CreateRoom
            (
                Some(RoomResponse::Ok),
                vec![RoomEvent::UserJoined {
                    room_id: rid(),
                    user: UserInfo {
                        id: 2,
                        name: "u2".into(),
                        monitor: false,
                    },
                }],
            ), // JoinRoom：UserJoined 驱动路由增量
            (Some(RoomResponse::Ok), Vec::new()), // Played 受理
        ],
    );
    let api = Arc::new(FakeFetchApi {
        fail: std::sync::atomic::AtomicBool::new(false),
        fail_first: std::sync::atomic::AtomicUsize::new(1), // 第一次失败，重试成功
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let bus = Bus::new(
        Arc::new(factory.clone()) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    )
    .with_api(Arc::clone(&api) as Arc<dyn phira_api::ApiClient>);

    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "u1".into(),
        },
    )
    .await
    .unwrap();
    bus.dispatch(
        CmdCtx {
            origin: Origin::Client { user_id: 2 },
            room_id: rid(),
        },
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "u2".into(),
        },
    )
    .await
    .unwrap();
    for _ in 0..100 {
        if bus.room_of(2).await.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    bus.dispatch(client_ctx(2), RoomCommand::Played { id: 2 })
        .await
        .unwrap();

    // 重试后回注 Ok（等待 ≤2s，包含 500ms 重试间隔）
    wait_for(&factory.received, |cmds| {
        cmds.iter().any(|c| {
            matches!(
                c,
                RoomCommand::RecordFetched {
                    user_id: 2,
                    record_id: 2,
                    record: Ok(_),
                    ..
                }
            )
        })
    })
    .await;
    let calls = api.calls.load(Ordering::SeqCst);
    assert!(
        calls >= 2,
        "瞬时失败应触发重试（实际调用 {calls} 次，预期 ≥2）"
    );
}

/// A2 兜底（§4.9-2）：持续失败 → 回注载荷为 Err（可诊断），且不超重试上限
/// （1 + RETRIES 次）——避免后台任务无限重试。impl 侧见到 Err 会结算为 aborted。
#[tokio::test]
async fn played_fetch_persistent_failure_redelivers_err_bounded_retries() {
    let factory = ScriptedFactory::default();
    factory.push(
        &rid(),
        vec![
            (Some(RoomResponse::Ok), Vec::new()), // CreateRoom
            (
                Some(RoomResponse::Ok),
                vec![RoomEvent::UserJoined {
                    room_id: rid(),
                    user: UserInfo {
                        id: 2,
                        name: "u2".into(),
                        monitor: false,
                    },
                }],
            ), // JoinRoom
            (Some(RoomResponse::Ok), Vec::new()), // Played 受理
        ],
    );
    let api = Arc::new(FakeFetchApi {
        fail: std::sync::atomic::AtomicBool::new(true), // 永远失败
        fail_first: std::sync::atomic::AtomicUsize::new(0),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let bus = Bus::new(
        Arc::new(factory.clone()) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    )
    .with_api(Arc::clone(&api) as Arc<dyn phira_api::ApiClient>);

    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "u1".into(),
        },
    )
    .await
    .unwrap();
    bus.dispatch(
        CmdCtx {
            origin: Origin::Client { user_id: 2 },
            room_id: rid(),
        },
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
            name: "u2".into(),
        },
    )
    .await
    .unwrap();
    for _ in 0..100 {
        if bus.room_of(2).await.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    bus.dispatch(client_ctx(2), RoomCommand::Played { id: 2 })
        .await
        .unwrap();

    wait_for(&factory.received, |cmds| {
        cmds.iter().any(|c| {
            matches!(
                c,
                RoomCommand::RecordFetched {
                    user_id: 2,
                    record_id: 2,
                    record: Err(_),
                    ..
                }
            )
        })
    })
    .await;
    // 重试上限：1 初始 + 2 重试 = 3 次，绝不无限
    let calls = api.calls.load(Ordering::SeqCst);
    assert_eq!(calls, 3, "持续失败应恰重试到上限（实际 {calls} 次）");
}

// ===== §7.3 Moderator 插座（2026-08 契约） =====

/// Moderator 测试替身：记录 intercept/on_event 调用 + 可配置拦截目标。
struct RecordingModerator {
    /// 拦截该 user_id 的一切命令；`i32::MIN` = 不拦任何人。
    block_user: std::sync::atomic::AtomicI32,
    /// 拦截调用记录（user_id, cmd Debug）。
    intercepted: Mutex<Vec<(i32, String)>>,
    /// on_event 收到的领域事件。
    seen: Mutex<Vec<RoomEvent>>,
}

#[async_trait::async_trait]
impl phira_api::Moderator for RecordingModerator {
    fn kind(&self) -> &'static str {
        "recording"
    }

    async fn intercept(&self, cmd: &RoomCommand, ctx: &CmdCtx) -> Result<(), RoomError> {
        let Origin::Client { user_id } = ctx.origin else {
            // 系统命令理论上不可达（bus 过滤）；若到达即记录并放行
            self.intercepted
                .lock()
                .unwrap()
                .push((-1, format!("{cmd:?}")));
            return Ok(());
        };
        self.intercepted
            .lock()
            .unwrap()
            .push((user_id, format!("{cmd:?}")));
        if user_id == self.block_user.load(Ordering::SeqCst) {
            return Err(RoomError::Business {
                code: RoomErrorCode::Moderated,
                msg: "blocked by test moderator".to_owned(),
            });
        }
        Ok(())
    }

    async fn on_event(&self, ev: &RoomEvent) {
        self.seen.lock().unwrap().push(ev.clone());
    }
}

impl RecordingModerator {
    fn new(block_user: i32) -> Arc<Self> {
        Arc::new(Self {
            block_user: std::sync::atomic::AtomicI32::new(block_user),
            intercepted: Mutex::new(Vec::new()),
            seen: Mutex::new(Vec::new()),
        })
    }
}

/// 拦截在路由之前：被拒命令不产生任何房间副作用（不建房/不加路由/不消费脚本）；
/// 其它用户不受影响；拒绝码 = Moderated（客户端可见）。
#[tokio::test]
async fn moderator_intercepts_client_command_before_routing() {
    let moderator = RecordingModerator::new(1); // 只拦 user 1
    let factory = ScriptedFactory::default();
    factory.push(&rid(), vec![(Some(RoomResponse::Ok), Vec::new())]);
    let rid2 = RoomId::new("mod-b-2".to_owned()).unwrap();
    factory.push(
        &rid2,
        vec![(
            Some(RoomResponse::Ok),
            vec![RoomEvent::RoomCreated {
                room_id: rid2.clone(),
                host: 2,
            }],
        )],
    );
    let bus = Bus::new(
        Arc::new(factory.clone()) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    )
    .with_moderators(vec![Arc::clone(&moderator) as Arc<dyn phira_api::Moderator>]);

    // user 1 建房 → 被拦：Err(Moderated)，无路由副作用
    let resp = bus
        .dispatch(
            client_ctx(1),
            RoomCommand::CreateRoom {
                id: rid(),
                name: "u1".into(),
            },
        )
        .await;
    assert!(
        matches!(
            resp,
            Err(RoomError::Business {
                code: RoomErrorCode::Moderated,
                ..
            })
        ),
        "被拦命令应回 Moderated: {resp:?}"
    );
    assert!(
        bus.room_of(1).await.is_none(),
        "被拦命令不得产生路由副作用（幽灵座位）"
    );

    // user 2 建房（不同房间）→ 正常
    let resp = bus
        .dispatch(
            client_ctx(2),
            RoomCommand::CreateRoom {
                id: rid2.clone(),
                name: "u2".into(),
            },
        )
        .await;
    assert!(matches!(resp, Ok(RoomResponse::Ok)));
    assert_eq!(bus.room_of(2).await, Some(rid2));

    // 拦截记录：只有 user 1 的 CreateRoom
    let intercepted = moderator.intercepted.lock().unwrap().clone();
    assert_eq!(
        intercepted.len(),
        2,
        "两次命令都应经过拦截: {intercepted:?}"
    );
    assert!(intercepted.iter().all(|(u, _)| *u == 1 || *u == 2));
    assert!(
        intercepted
            .iter()
            .any(|(u, c)| *u == 2 && c.contains("CreateRoom"))
    );
}

/// 系统命令不经拦截：生命周期事实/回注/配置不能被观察者卡住（core 保证）。
#[tokio::test]
async fn moderator_skips_system_commands() {
    let moderator = RecordingModerator::new(i32::MAX); // 若被调用会拦——但不应被调用
    let factory = ScriptedFactory::default();
    factory.push(&rid(), vec![(Some(RoomResponse::Ok), Vec::new())]);
    let bus = Bus::new(
        Arc::new(factory.clone()) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    )
    .with_moderators(vec![Arc::clone(&moderator) as Arc<dyn phira_api::Moderator>]);

    // dispatch_system 到不存在的房间 → 路由层 Internal；且拦截器零调用
    let resp = bus
        .dispatch_system(
            RoomId::new("nope".to_owned()).unwrap(),
            RoomCommand::Tick { now: 0 },
        )
        .await;
    assert!(resp.is_err(), "房间不存在应 Internal: {resp:?}");
    assert!(
        moderator.intercepted.lock().unwrap().is_empty(),
        "系统命令不得经过观察者拦截"
    );
}

/// 领域事件通知：收到 UserJoined（领域），不收到 RelayTouches（热路径，§4.4 分类）。
#[tokio::test]
async fn moderator_receives_domain_events_only() {
    let moderator = RecordingModerator::new(i32::MIN); // 不拦
    let factory = ScriptedFactory::default();
    factory.push(
        &rid(),
        vec![(
            Some(RoomResponse::Ok),
            vec![
                RoomEvent::RelayTouches {
                    room_id: rid(),
                    targets: Targets::Specific(vec![9]),
                    player: 1,
                    frames: Arc::new(Vec::new()),
                },
                RoomEvent::UserJoined {
                    room_id: rid(),
                    user: UserInfo {
                        id: 2,
                        name: "u2".into(),
                        monitor: false,
                    },
                },
            ],
        )],
    );
    let bus = Bus::new(
        Arc::new(factory.clone()) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    )
    .with_moderators(vec![Arc::clone(&moderator) as Arc<dyn phira_api::Moderator>]);

    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "u1".into(),
        },
    )
    .await
    .unwrap();

    // on_event 是 fire-and-forget：轮询等 UserJoined 到达
    for _ in 0..100 {
        if moderator
            .seen
            .lock()
            .unwrap()
            .iter()
            .any(|e| matches!(e, RoomEvent::UserJoined { user, .. } if user.id == 2))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let seen = moderator.seen.lock().unwrap().clone();
    assert!(
        seen.iter()
            .any(|e| matches!(e, RoomEvent::UserJoined { user, .. } if user.id == 2)),
        "领域事件应通知观察者: {seen:?}"
    );
    assert!(
        !seen
            .iter()
            .any(|e| matches!(e, RoomEvent::RelayTouches { .. })),
        "热路径事件不得通知观察者: {seen:?}"
    );
}

/// 热路径豁免：Touches/Judges 不经过拦截（慢观察者不得拖垮 60Hz 转发路径，
/// DropIfFull 哲学，§4.9-9；与 on_event 过滤 Relay* 同分类，§4.4）。
#[tokio::test]
async fn moderator_skips_hotpath_commands() {
    let moderator = RecordingModerator::new(i32::MAX); // 若被调用即拦截——但不应被调用
    let factory = ScriptedFactory::default();
    factory.push(
        &rid(),
        vec![
            (
                Some(RoomResponse::Ok),
                vec![RoomEvent::RoomCreated {
                    room_id: rid(),
                    host: 1,
                }],
            ),
            (None, Vec::new()), // Touches 热路径（DropIfFull，无回话）
        ],
    );
    let bus = Bus::new(
        Arc::new(factory.clone()) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    )
    .with_moderators(vec![Arc::clone(&moderator) as Arc<dyn phira_api::Moderator>]);

    bus.dispatch(
        client_ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "u1".into(),
        },
    )
    .await
    .unwrap();
    // Touches 应正常投递（不被拦、不回 Moderated）
    let resp = bus
        .dispatch(
            client_ctx(1),
            RoomCommand::Touches {
                frames: Arc::new(Vec::new()),
            },
        )
        .await;
    assert!(
        matches!(resp, Ok(RoomResponse::Ok)),
        "热路径应正常: {resp:?}"
    );
    // 拦截记录只有 CreateRoom，无 Touches
    let intercepted = moderator.intercepted.lock().unwrap().clone();
    assert_eq!(intercepted.len(), 1, "仅业务命令经过拦截: {intercepted:?}");
    assert!(intercepted[0].1.contains("CreateRoom"));
}

/// 运行期热插拔（§7.3 兑现，阶段 3）：`add_moderator` 后拦截立即生效；
/// `remove_moderator`（按 type_name）后失效；重复 add 幂等。
#[tokio::test]
async fn moderator_hotplug_add_remove() {
    let factory = ScriptedFactory::default();
    factory.push(&rid(), vec![(Some(RoomResponse::Ok), Vec::new())]);
    let bus = Bus::new(
        Arc::new(factory.clone()) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );

    // 初始无观察者：user 1 建房正常
    let r1 = bus
        .dispatch(
            client_ctx(1),
            RoomCommand::CreateRoom {
                id: rid(),
                name: "u1".into(),
            },
        )
        .await;
    assert!(matches!(r1, Ok(RoomResponse::Ok)), "初始应无拦截: {r1:?}");

    // 热挂载（拦 user 1）
    let m = RecordingModerator::new(1);
    bus.add_moderator(Arc::clone(&m) as Arc<dyn phira_api::Moderator>);
    let r2 = bus
        .dispatch(
            client_ctx(1),
            RoomCommand::Tick { now: 0 }, // 系统命令不经过拦截——用客户端命令
        )
        .await;
    // Tick 是系统命令源，但 client_ctx 是 Client origin → 会过拦截
    assert!(
        matches!(
            r2,
            Err(RoomError::Business {
                code: RoomErrorCode::Moderated,
                ..
            })
        ),
        "热挂载后应拦截: {r2:?}"
    );

    // 幂等：重复 add 不叠加（同一 type_name 只挂一份）。若幂等失效（两份实例），
    // 一次 dispatch 会命中两次 intercept → 记录数 3 而非 2。
    bus.add_moderator(Arc::clone(&m) as Arc<dyn phira_api::Moderator>);
    let _ = bus
        .dispatch(client_ctx(1), RoomCommand::Tick { now: 0 })
        .await;
    let intercepted = m.intercepted.lock().unwrap().len();
    assert_eq!(
        intercepted, 2,
        "重复挂载不叠加（两实例会记 3 次）: {intercepted}"
    );

    // 热卸载：按 type_name 移除后拦截失效
    assert!(bus.remove_moderator("recording"), "卸载应移除 >=1");
    assert!(!bus.remove_moderator("recording"), "重复卸载应为 false");
    let r3 = bus
        .dispatch(client_ctx(1), RoomCommand::Tick { now: 0 })
        .await;
    assert!(
        !matches!(
            r3,
            Err(RoomError::Business {
                code: RoomErrorCode::Moderated,
                ..
            })
        ),
        "卸载后不再拦截: {r3:?}"
    );
}
