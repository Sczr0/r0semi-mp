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
    assert_eq!(r.users, 2, "房主 + 加入者");
    assert_eq!(r.state, "WaitingForReady");
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

    // 选图 → SelectChart(7)
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
    assert_eq!(sink.snapshot().await[0].state, "SelectChart(7)");

    // 开局 → Playing
    sink.deliver(1, &RoomEvent::StartPlaying { room_id: rid("g1") })
        .await;
    assert_eq!(sink.snapshot().await[0].state, "Playing");

    // 结算 → 回 SelectChart（保留谱面）
    sink.deliver(
        1,
        &RoomEvent::GameEnd {
            room_id: rid("g1"),
            chart: Some(7),
        },
    )
    .await;
    assert_eq!(sink.snapshot().await[0].state, "SelectChart(7)");
}
