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
    bus.dispatch(ctx(1), RoomCommand::CreateRoom { id: rid() })
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
    let e1 = registry.register(1);

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
    let e1 = registry.register(1);

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
    let e2 = registry.register(1);
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
    let e1 = registry.register(1);

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
    let e1 = registry.register(1);

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
    let e2 = registry.register(1);
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
    assert_eq!(registry.register(1), 1);
    assert_eq!(registry.register(1), 2, "同 id 再次注册 epoch+1");
    assert_eq!(registry.register(2), 1, "不同用户独立计数");
    assert_eq!(registry.register(1), 3);
}
