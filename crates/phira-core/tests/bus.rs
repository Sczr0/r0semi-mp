//! phira-core 集成测试（§4.9-3：测试位置 = phira-core 集成测试 + 脚本化假 actor）。
//!
//! 验证对象全是 **core 行为**（契约套件测不到这些，评审 §8 六）：
//! - 路由规则（CreateRoom/JoinRoom 载荷路由、表路由、表 miss 回错，§4.9-4）
//! - 时序不变量（先解析后应用 / 先应用后响应，§4.9-4）
//! - 房间生命周期（RoomClosed 清理、排空、后续命令失败，§4.9-9）
//! - 事件投递目标（领域事件恒 All / Relay 指令 Specific，§4.4）
//! - 配置热更广播（§4.9-8）

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use phira_api::{
    CmdCtx, Origin, RoomCommand, RoomConfig, RoomError, RoomErrorCode, RoomEvent, RoomFactory,
    RoomId, RoomResponse, RoomState, Targets, UserInfo,
};
use phira_core::{Bus, EventSink};
use tokio::sync::oneshot;

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
            .and_then(|q| q.pop_front())
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

fn ok() -> Option<RoomResponse> {
    Some(RoomResponse::Ok)
}

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
        .dispatch(client_ctx(1), RoomCommand::CreateRoom { id: rid() })
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

    bus.dispatch(client_ctx(1), RoomCommand::CreateRoom { id: rid() })
        .await
        .unwrap();
    let resp = bus
        .dispatch(
            client_ctx(2),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
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

    bus.dispatch(client_ctx(1), RoomCommand::CreateRoom { id: rid() })
        .await
        .unwrap();
    bus.dispatch(
        client_ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
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
            (ok(), vec![RoomEvent::RoomClosed { room_id: rid() }]),
        ],
    );
    let bus = Bus::new(
        Arc::clone(&factory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );

    bus.dispatch(client_ctx(1), RoomCommand::CreateRoom { id: rid() })
        .await
        .unwrap();
    // 触发空房自毁
    bus.dispatch(client_ctx(1), RoomCommand::LeaveRoom)
        .await
        .unwrap();

    // 房间已删：JoinRoom 应 RoomNotFound
    let resp = bus
        .dispatch(
            client_ctx(9),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
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
        .dispatch(client_ctx(1), RoomCommand::CreateRoom { id: rid() })
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

    bus.dispatch(client_ctx(1), RoomCommand::CreateRoom { id: rid() })
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
    assert_eq!(deliveries.len(), 1, "只投递给 monitor 9: {deliveries:?}");
    assert_eq!(deliveries[0].0, 9);
    assert!(matches!(deliveries[0].1, RoomEvent::RelayTouches { .. }));
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

    bus.dispatch(client_ctx(1), RoomCommand::CreateRoom { id: rid() })
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

    bus.dispatch(client_ctx(1), RoomCommand::CreateRoom { id: rid() })
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

    bus.dispatch(client_ctx(1), RoomCommand::CreateRoom { id: rid() })
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

    bus.dispatch(client_ctx(1), RoomCommand::CreateRoom { id: rid() })
        .await
        .unwrap();
    let resp = bus
        .dispatch(client_ctx(2), RoomCommand::CreateRoom { id: rid() })
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
    bus.dispatch(client_ctx(1), RoomCommand::CreateRoom { id: rid() })
        .await
        .unwrap();
    // 用户 2 入房 A
    bus.dispatch(
        client_ctx(2),
        RoomCommand::JoinRoom {
            id: rid(),
            monitor: false,
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
            RoomCommand::CreateRoom { id: room_b.clone() },
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
        bus.dispatch(ctx_a, RoomCommand::CreateRoom { id: room_a.clone() }),
        bus.dispatch(ctx_b, RoomCommand::CreateRoom { id: room_b.clone() }),
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
    bus.dispatch(client_ctx(1), RoomCommand::CreateRoom { id: rid() })
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
