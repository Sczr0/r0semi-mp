//! 用户生命周期集成测试（§4.9-3）：会话纪元 + 窗口边界 + 单一生产者。
//!
//! 验证 core 行为（脚本化假 actor 断言收到什么命令）：
//! - 断线 → UserDisconnected 派发到用户房间
//! - 窗口内重连 → UserReconnected（座位恢复）
//! - 窗口到期 → UserDangleExpired（先查权威状态，§4.9-3 窗口边界）
//! - 旧会话死亡事实 / 过期定时器 → 忽略（epoch 校验）
//! - register epoch 递增

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use phira_api::{
    CmdCtx, Origin, RoomCommand, RoomConfig, RoomEvent, RoomFactory, RoomId, RoomResponse,
};
use phira_core::{
    Bus,
    lifecycle::{LifecycleEvent, LifecycleTask},
};
use tokio::sync::mpsc;

/// 脚本项：一条命令的（响应, 事件集）。（与 tests/bus.rs 同构的脚本化假 actor）
type ScriptItem = (Option<RoomResponse>, Vec<RoomEvent>);
/// 一个房间 actor 的完整脚本。
type ActorScript = VecDeque<ScriptItem>;

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

#[derive(Clone, Default)]
struct ScriptedFactory {
    scripts: Arc<Mutex<HashMap<RoomId, VecDeque<ActorScript>>>>,
    received: Arc<Mutex<Vec<RoomCommand>>>,
}

impl ScriptedFactory {
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

fn rid() -> RoomId {
    RoomId::new("test".to_owned()).unwrap()
}

fn ctx(user_id: i32) -> CmdCtx {
    CmdCtx {
        origin: Origin::Client { user_id },
        room_id: rid(),
    }
}

/// 测试环境：bus + 生命周期任务 + 用户 1 建房。返回 (bus, registry, fact_tx, received)。
async fn setup(
    window: Duration,
) -> (
    Bus,
    Arc<phira_core::lifecycle::SessionRegistry>,
    mpsc::Sender<LifecycleEvent>,
    Arc<Mutex<Vec<RoomCommand>>>,
) {
    let factory = Arc::new(ScriptedFactory::default());
    let received = factory.received();
    // 房间脚本：CreateRoom 应答 + RoomCreated 事件（驱动路由注册，§4.9-4）
    factory.push(
        &rid(),
        vec![(
            Some(RoomResponse::Ok),
            vec![RoomEvent::RoomCreated {
                room_id: rid(),
                host: 1,
            }],
        )],
    );
    let bus = Bus::new(
        factory as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig { monitors: vec![] }),
    );
    // 用户 1 建房（路由注册）
    bus.dispatch(
        ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "user1".to_owned(),
        },
    )
    .await
    .unwrap();

    let (task, registry, fact_tx) = LifecycleTask::new(bus.clone(), window);
    tokio::spawn(task.run());
    (bus, registry, fact_tx, received)
}

/// 断言 actor 收到过某命令（轮询等待，因无回话命令只入队不等处理）。
async fn wait_received(
    received: &Arc<Mutex<Vec<RoomCommand>>>,
    pred: impl Fn(&RoomCommand) -> bool,
) -> bool {
    for _ in 0..20 {
        if received.lock().unwrap().iter().any(&pred) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test]
async fn disconnect_dispatches_to_room() {
    let (_bus, registry, fact_tx, received) = setup(Duration::from_secs(10)).await;
    let e1 = registry.register(1, "u1".to_owned());

    fact_tx
        .send(LifecycleEvent::Connected {
            user_id: 1,
            epoch: e1,
        })
        .await
        .unwrap();
    fact_tx
        .send(LifecycleEvent::Disconnected {
            user_id: 1,
            epoch: e1,
        })
        .await
        .unwrap();

    assert!(
        wait_received(&received, |c| matches!(
            c,
            RoomCommand::UserDisconnected { user_id: 1, .. }
        ))
        .await,
        "断线应派发 UserDisconnected: {:?}",
        *received.lock().unwrap()
    );
}

#[tokio::test]
async fn reconnect_within_window_preserves_seat() {
    let (_bus, registry, fact_tx, received) = setup(Duration::from_secs(10)).await;
    let e1 = registry.register(1, "u1".to_owned());

    fact_tx
        .send(LifecycleEvent::Connected {
            user_id: 1,
            epoch: e1,
        })
        .await
        .unwrap();
    fact_tx
        .send(LifecycleEvent::Disconnected {
            user_id: 1,
            epoch: e1,
        })
        .await
        .unwrap();
    // 窗口内重连（epoch+1）
    let e2 = registry.register(1, "u1".to_owned());
    assert_eq!(e2, 2, "重连应分配新纪元");
    fact_tx
        .send(LifecycleEvent::Connected {
            user_id: 1,
            epoch: e2,
        })
        .await
        .unwrap();

    assert!(
        wait_received(&received, |c| matches!(
            c,
            RoomCommand::UserReconnected {
                user_id: 1,
                epoch: 2,
                ..
            }
        ))
        .await,
        "窗口内重连应恢复座位: {:?}",
        *received.lock().unwrap()
    );
}

#[tokio::test]
async fn dangle_expired_after_window() {
    let (_bus, registry, fact_tx, received) = setup(Duration::from_millis(50)).await;
    let e1 = registry.register(1, "u1".to_owned());

    fact_tx
        .send(LifecycleEvent::Connected {
            user_id: 1,
            epoch: e1,
        })
        .await
        .unwrap();
    fact_tx
        .send(LifecycleEvent::Disconnected {
            user_id: 1,
            epoch: e1,
        })
        .await
        .unwrap();

    assert!(
        wait_received(&received, |c| matches!(
            c,
            RoomCommand::UserDangleExpired { user_id: 1 }
        ))
        .await,
        "窗口到期应派发 UserDangleExpired: {:?}",
        *received.lock().unwrap()
    );
}

#[tokio::test]
async fn stale_disconnect_ignored_after_reconnect() {
    let (_bus, registry, fact_tx, received) = setup(Duration::from_millis(50)).await;
    let e1 = registry.register(1, "u1".to_owned());

    fact_tx
        .send(LifecycleEvent::Connected {
            user_id: 1,
            epoch: e1,
        })
        .await
        .unwrap();
    fact_tx
        .send(LifecycleEvent::Disconnected {
            user_id: 1,
            epoch: e1,
        })
        .await
        .unwrap();
    let e2 = registry.register(1, "u1".to_owned());
    fact_tx
        .send(LifecycleEvent::Connected {
            user_id: 1,
            epoch: e2,
        })
        .await
        .unwrap();

    // 旧会话的死亡事实（epoch=1 已不是当前）→ 忽略
    fact_tx
        .send(LifecycleEvent::Disconnected {
            user_id: 1,
            epoch: e1,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let got = received.lock().unwrap().clone();
    assert!(
        !got.iter().any(|c| matches!(
            c,
            RoomCommand::UserDisconnected {
                user_id: 1,
                epoch: 1
            }
        )),
        "旧会话死亡事实应被忽略: {got:?}"
    );
    // 且窗口到期（旧 epoch 定时器）也不驱逐（已重连）
    assert!(
        !got.iter()
            .any(|c| matches!(c, RoomCommand::UserDangleExpired { user_id: 1 })),
        "已重连不应被旧窗口驱逐: {got:?}"
    );
}

#[tokio::test]
async fn registry_epoch_sequence() {
    let registry = phira_core::lifecycle::SessionRegistry::new();
    assert_eq!(registry.register(1, "u1".to_owned()), 1);
    assert_eq!(
        registry.register(1, "u1".to_owned()),
        2,
        "同 id 再次注册 epoch+1"
    );
    assert_eq!(registry.register(2, "u2".to_owned()), 1, "不同用户独立计数");
    assert_eq!(registry.register(1, "u1".to_owned()), 3);
}

#[tokio::test]
async fn explicit_reconnected_fact() {
    // LifecycleEvent::Reconnected 显式事实（§4.9-3 预留语义）→ 恢复座位
    let (_bus, registry, fact_tx, received) = setup(Duration::from_secs(10)).await;
    let e1 = registry.register(1, "u1".to_owned());
    fact_tx
        .send(LifecycleEvent::Connected {
            user_id: 1,
            epoch: e1,
        })
        .await
        .unwrap();
    fact_tx
        .send(LifecycleEvent::Reconnected {
            user_id: 1,
            epoch: e1,
        })
        .await
        .unwrap();
    assert!(
        wait_received(&received, |c| matches!(
            c,
            RoomCommand::UserReconnected {
                user_id: 1,
                epoch: 1,
                ..
            }
        ))
        .await,
        "显式 Reconnected 应恢复座位: {:?}",
        *received.lock().unwrap()
    );
}

#[tokio::test]
async fn register_keeps_name() {
    let registry = phira_core::lifecycle::SessionRegistry::new();
    let e1 = registry.register(1, "alice".to_owned());
    assert_eq!(e1, 1);
    assert_eq!(registry.name_of(1).as_deref(), Some("alice"));
    // 重连替换名字（epoch+1）
    let e2 = registry.register(1, "alice2".to_owned());
    assert_eq!(e2, 2);
    assert_eq!(registry.name_of(1).as_deref(), Some("alice2"));
    assert_eq!(registry.name_of(99), None);
}

/// ISSUE-0012（方案 A）：`evict_name` 淘汰昵称但不触碰 epoch——
/// 用户彻底离线后重连，epoch 继续 +1（单调不回收），昵称重新注入。
/// 且淘汰后 `current_epoch` 仍可查（僵尸连接校验语义不因删除而失效）。
#[tokio::test]
async fn evict_name_keeps_monotonic_epoch() {
    let registry = phira_core::lifecycle::SessionRegistry::new();
    let e1 = registry.register(1, "alice".to_owned());
    assert_eq!(e1, 1);
    assert_eq!(registry.name_of(1).as_deref(), Some("alice"));

    // 用户彻底离线（dangle 到期）→ 淘汰昵称
    registry.evict_name(1);
    assert_eq!(registry.name_of(1), None, "昵称应被淘汰");
    // 但 epoch 保留（单调不变量），ISSUE-0009 校验读入口不失效
    assert_eq!(registry.current_epoch(1), Some(1), "epoch 保留");

    // 同用户重连：epoch 继续 +1（不回退），昵称重新注入
    let e2 = registry.register(1, "alice2".to_owned());
    assert_eq!(e2, 2, "epoch 单调递增，不回退");
    assert_eq!(registry.name_of(1).as_deref(), Some("alice2"), "昵称重注");
    assert_eq!(registry.current_epoch(1), Some(2));
}

// —— ISSUE-0001 修复：幽灵座位重放（§4.9-3 第四竞态）——

/// 慢入房 actor：JoinRoom 处理时 sleep，拉大"增量未应用"窗口（幽灵座位竞态模拟）。
struct SlowJoinActor {
    received: Arc<Mutex<Vec<RoomCommand>>>,
}

#[async_trait::async_trait]
impl phira_api::RoomActor for SlowJoinActor {
    async fn handle(
        &mut self,
        _ctx: CmdCtx,
        cmd: RoomCommand,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        self.received.lock().unwrap().push(cmd.clone());
        match cmd {
            RoomCommand::CreateRoom { .. } => (
                Some(RoomResponse::Ok),
                vec![RoomEvent::RoomCreated {
                    room_id: rid(),
                    host: 1,
                }],
            ),
            RoomCommand::JoinRoom { .. } => {
                // 拉大窗口：bus 忙（actor 处理慢）→ 增量应用前让出 30ms
                tokio::time::sleep(Duration::from_millis(30)).await;
                (
                    Some(RoomResponse::JoinRoom(phira_api::JoinRoomResponse {
                        state: phira_api::RoomState::SelectChart(None),
                        users: vec![],
                        live: false,
                    })),
                    vec![RoomEvent::UserJoined {
                        room_id: rid(),
                        user: phira_api::UserInfo {
                            id: 2,
                            name: "u2".to_owned(),
                            monitor: false,
                        },
                    }],
                )
            }
            _ => (None, Vec::new()),
        }
    }
}

#[derive(Clone, Default)]
struct SlowJoinFactory {
    received: Arc<Mutex<Vec<RoomCommand>>>,
}

impl SlowJoinFactory {
    fn received(&self) -> Arc<Mutex<Vec<RoomCommand>>> {
        Arc::clone(&self.received)
    }
}

impl RoomFactory for SlowJoinFactory {
    fn create(&self, _room_id: RoomId) -> Box<dyn phira_api::RoomActor> {
        Box::new(SlowJoinActor {
            received: Arc::clone(&self.received),
        })
    }
}

/// 幽灵座位竞态：客户端入房（JoinRoom 处理中，增量未应用）即断线，
/// 生命周期任务查表 miss → 挂起重放命中 → UserDisconnected 最终派发。
/// 修复前：事实被丢 → 座位留在房间无人驱逐（ISSUE-0001）。
#[tokio::test]
async fn ghost_seat_replay_recovers_missed_route() {
    let factory = Arc::new(SlowJoinFactory::default());
    let received = factory.received();
    let bus = Bus::new(
        factory as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig { monitors: vec![] }),
    );
    let (task, registry, fact_tx) = LifecycleTask::new(bus.clone(), Duration::from_secs(10));
    tokio::spawn(task.run());

    // 用户 1 建房（路由注册 host=1）
    bus.dispatch(
        ctx(1),
        RoomCommand::CreateRoom {
            id: rid(),
            name: "u1".to_owned(),
        },
    )
    .await
    .unwrap();
    // 用户 2 注册（epoch=1）
    let e2 = registry.register(2, "u2".to_owned());

    // 并发：JoinRoom（actor 慢 30ms，增量未应用窗口）+ 主流程立即发断线事实
    let bus2 = bus.clone();
    let jh = tokio::spawn(async move {
        bus2.dispatch(
            ctx(2),
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: false,
                name: "u2".to_owned(),
            },
        )
        .await
    });
    fact_tx
        .send(LifecycleEvent::Disconnected {
            user_id: 2,
            epoch: e2,
        })
        .await
        .unwrap();
    jh.await.unwrap().unwrap();

    // 断言：UserDisconnected 最终派发（重放兜底；修复前 = 事实被丢 → 幽灵座位）
    assert!(
        wait_received(&received, |c| matches!(
            c,
            RoomCommand::UserDisconnected { user_id: 2, .. }
        ))
        .await,
        "幽灵座位场景：Disconnected 应经重放派发，不得丢失"
    );
}

/// 正常路径：用户从未入房 → 重放耗尽仍 miss → 事实丢弃（不误派发、不误报）。
#[tokio::test]
async fn replay_gives_up_when_user_never_in_room() {
    let factory = Arc::new(ScriptedFactory::default());
    let received = factory.received();
    let bus = Bus::new(
        factory as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig { monitors: vec![] }),
    );
    let (task, registry, fact_tx) = LifecycleTask::new(bus.clone(), Duration::from_secs(10));
    tokio::spawn(task.run());

    let e3 = registry.register(3, "u3".to_owned());
    fact_tx
        .send(LifecycleEvent::Disconnected {
            user_id: 3,
            epoch: e3,
        })
        .await
        .unwrap();
    // 等重放耗尽（3×20ms）+ 余量
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !received
            .lock()
            .unwrap()
            .iter()
            .any(|c| matches!(c, RoomCommand::UserDisconnected { user_id: 3, .. })),
        "用户从未入房：重放后应放弃，不得误派发"
    );
}
