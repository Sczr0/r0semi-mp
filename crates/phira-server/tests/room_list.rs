//! 房间列表观察者（§7.3 / §运营）：事件驱动快照 + 私密前缀过滤。

use std::sync::Arc;

use phira_api::{RoomEvent, RoomId, UserInfo};
use phira_core::EventSink;
use phira_server::server::RoomListSink;
fn rid(id: &str) -> RoomId {
    RoomId::new(id.to_owned()).unwrap()
}

fn ui(id: i32, name: &str) -> UserInfo {
    UserInfo {
        id,
        name: name.to_owned(),
        monitor: false,
    }
}

#[tokio::test]
async fn snapshot_tracks_room_lifecycle() {
    let sink = Arc::new(RoomListSink::new(Vec::new()));

    // 建房
    sink.deliver(
        1,
        &RoomEvent::RoomCreated {
            room_id: rid("abc"),
            host: 1,
        },
    )
    .await;
    // 加入
    sink.deliver(
        2,
        &RoomEvent::UserJoined {
            room_id: rid("abc"),
            user: ui(2, "p2"),
        },
    )
    .await;
    // 状态推进
    sink.deliver(
        1,
        &RoomEvent::GameStart {
            room_id: rid("abc"),
            user: 1,
        },
    )
    .await;
    sink.deliver(
        1,
        &RoomEvent::LockRoom {
            room_id: rid("abc"),
            lock: true,
        },
    )
    .await;

    let rooms = sink.snapshot().await;
    assert_eq!(rooms.len(), 1, "一个房间");
    let r = &rooms[0];
    assert_eq!(r.id, "abc");
    assert_eq!(r.players, vec![1, 2], "房主（建房即入列）+ 加入者");
    assert_eq!(r.state, "wait_for_ready");
    assert!(r.locked);
    assert_eq!(r.host, 1);

    // 离开 + 关闭
    sink.deliver(
        2,
        &RoomEvent::UserLeft {
            room_id: rid("abc"),
            user: 2,
            name: "p2".to_owned(),
        },
    )
    .await;
    sink.deliver(
        1,
        &RoomEvent::RoomClosed {
            room_id: rid("abc"),
        },
    )
    .await;
    assert!(sink.snapshot().await.is_empty(), "房间关闭后列表为空");
}

#[tokio::test]
async fn hidden_prefix_rooms_excluded() {
    let sink = Arc::new(RoomListSink::new(vec!["solo".to_owned()]));

    // 公开房间
    sink.deliver(
        1,
        &RoomEvent::RoomCreated {
            room_id: rid("pub1"),
            host: 1,
        },
    )
    .await;
    // 私密房间（solo 前缀）
    sink.deliver(
        3,
        &RoomEvent::RoomCreated {
            room_id: rid("solo-9f3a"),
            host: 3,
        },
    )
    .await;

    let rooms = sink.snapshot().await;
    assert_eq!(rooms.len(), 1, "私密房间不进入公开列表");
    assert_eq!(rooms[0].id, "pub1");
}

#[tokio::test]
async fn chart_select_and_game_end_states() {
    let sink = Arc::new(RoomListSink::new(Vec::new()));
    sink.deliver(
        1,
        &RoomEvent::RoomCreated {
            room_id: rid("g1"),
            host: 1,
        },
    )
    .await;

    // 选图 → select_chart + 谱面记录（name+id）
    sink.deliver(
        1,
        &RoomEvent::SelectChart {
            room_id: rid("g1"),
            user: 1,
            name: "chart".to_owned(),
            id: 7,
        },
    )
    .await;
    let r = &sink.snapshot().await[0];
    assert_eq!(r.state, "select_chart");
    assert_eq!(r.chart, Some(("chart".to_owned(), 7)));

    // 开局 → playing（谱面保留）
    sink.deliver(1, &RoomEvent::StartPlaying { room_id: rid("g1") })
        .await;
    let r = &sink.snapshot().await[0];
    assert_eq!(r.state, "playing");
    assert_eq!(r.chart, Some(("chart".to_owned(), 7)));

    // 结算 → 回 select_chart（保留谱面）
    sink.deliver(
        1,
        &RoomEvent::GameEnd {
            room_id: rid("g1"),
            chart: Some(7),
        },
    )
    .await;
    let r = &sink.snapshot().await[0];
    assert_eq!(r.state, "select_chart");
    assert_eq!(r.chart, Some(("chart".to_owned(), 7)), "结算后谱面保留");

    // 取消开局且无谱面 → 清空 chart
    sink.deliver(
        1,
        &RoomEvent::CancelGame {
            room_id: rid("g1"),
            user: 1,
            chart: None,
        },
    )
    .await;
    let r = &sink.snapshot().await[0];
    assert_eq!(r.state, "select_chart");
    assert_eq!(r.chart, None, "无谱面事件应清空 chart");
}

/// 2026-08 对齐拍板：monitor（观战者）不进 players 名单；离开只移除真实玩家。
#[tokio::test]
async fn monitor_join_never_enters_players() {
    let sink = Arc::new(RoomListSink::new(Vec::new()));
    sink.deliver(
        1,
        &RoomEvent::RoomCreated {
            room_id: rid("m1"),
            host: 1,
        },
    )
    .await;
    let mut m = ui(2, "watcher");
    m.monitor = true;
    sink.deliver(
        2,
        &RoomEvent::UserJoined {
            room_id: rid("m1"),
            user: m,
        },
    )
    .await;
    sink.deliver(
        3,
        &RoomEvent::UserJoined {
            room_id: rid("m1"),
            user: ui(3, "p3"),
        },
    )
    .await;
    assert_eq!(
        sink.snapshot().await[0].players,
        vec![1, 3],
        "monitor 不入列，普通玩家入列"
    );

    // monitor 离开对名单无影响；玩家离开移除
    sink.deliver(
        2,
        &RoomEvent::UserLeft {
            room_id: rid("m1"),
            user: 2,
            name: "watcher".to_owned(),
        },
    )
    .await;
    assert_eq!(sink.snapshot().await[0].players, vec![1, 3]);
    sink.deliver(
        3,
        &RoomEvent::UserLeft {
            room_id: rid("m1"),
            user: 3,
            name: "p3".to_owned(),
        },
    )
    .await;
    assert_eq!(sink.snapshot().await[0].players, vec![1], "玩家离开被移除");
}

/// 集成测试：真实 bus → RoomListSink 链路——最后一人离开后快照必须清空。
///
/// 回归（用户实测）：空房残留列表（`users: 0` 僵尸条目）。根因：bus 把 `RoomClosed`
/// 拦在 `process_events` 步骤 1（§4.4 旧语义"core 信号不投递"），RoomListSink 的
/// `RoomClosed => remove` 分支是死代码。修复：bus 步骤 4 对观察者补投 RoomClosed。
#[tokio::test]
async fn bus_room_closed_clears_snapshot() {
    use phira_api::{
        CmdCtx, Origin, RoomActor, RoomCommand, RoomConfig, RoomFactory, RoomResponse,
    };
    use phira_core::Bus;

    // 回声 actor：建房 → RoomCreated；离开（最后一人）→ UserLeft + RoomClosed
    struct EchoActor;
    #[async_trait::async_trait]
    impl RoomActor for EchoActor {
        async fn handle(
            &mut self,
            _ctx: CmdCtx,
            cmd: RoomCommand,
        ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
            match cmd {
                RoomCommand::CreateRoom { .. } => (
                    Some(RoomResponse::Ok),
                    vec![RoomEvent::RoomCreated {
                        room_id: rid("abc"),
                        host: 1,
                    }],
                ),
                RoomCommand::LeaveRoom => (
                    Some(RoomResponse::Ok),
                    vec![
                        RoomEvent::UserLeft {
                            room_id: rid("abc"),
                            user: 1,
                            name: "p1".to_owned(),
                        },
                        RoomEvent::RoomClosed {
                            room_id: rid("abc"),
                        },
                    ],
                ),
                _ => (Some(RoomResponse::Ok), vec![]),
            }
        }
    }
    struct EchoFactory;
    impl RoomFactory for EchoFactory {
        fn create(&self, _room_id: RoomId) -> Box<dyn RoomActor> {
            Box::new(EchoActor)
        }
    }

    let bus = Bus::new(
        Arc::new(EchoFactory) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );
    let list = Arc::new(RoomListSink::new(Vec::new()));
    bus.attach_sink(Arc::clone(&list) as Arc<dyn EventSink>);

    let ctx = CmdCtx {
        origin: Origin::Client { user_id: 1 },
        room_id: rid("abc"),
    };
    bus.dispatch(
        ctx.clone(),
        RoomCommand::CreateRoom {
            id: rid("abc"),
            name: "p1".to_owned(),
        },
    )
    .await
    .unwrap();
    assert_eq!(list.snapshot().await.len(), 1, "建房后列表有房");

    // 最后一人离开 → 快照必须清空（修复前残留 users:0 僵尸条目）
    bus.dispatch(ctx, RoomCommand::LeaveRoom).await.unwrap();
    assert!(
        list.snapshot().await.is_empty(),
        "最后一人离开后列表必须清空"
    );
}
