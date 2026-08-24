//! 转换层测试（§6.6 表 1 / 表 2）。
#![allow(clippy::unwrap_used)] // 测试断言失败=panic 是预期语义（柜台限制针对生产代码）
//!
//! 表 1：ClientCommand → RoomCommand 映射（Ping/Authenticate 归 core）
//! 表 2：RoomEvent → (Targets, ServerCommand) 列表（含非机械映射断言）

use std::sync::Arc;

use phira_api::{
    ClientCommand, Message, RoomCommand, RoomError, RoomEvent, RoomId, RoomState, ServerCommand,
    Targets, TouchFrame, UserInfo, Varchar,
};
use phira_core::convert::{client_to_room, event_to_server};

fn rid() -> RoomId {
    RoomId::new("test".into()).unwrap()
}

// —— 表 1：ClientCommand → RoomCommand ——

#[test]
fn table1_full_mapping() {
    let cases = vec![
        (
            ClientCommand::Chat {
                message: Varchar::new("hi".into()).unwrap(),
            },
            RoomCommand::Chat {
                message: Varchar::new("hi".into()).unwrap(),
            },
        ),
        (
            ClientCommand::Touches {
                frames: Arc::new(Vec::new()),
            },
            RoomCommand::Touches {
                frames: Arc::new(Vec::new()),
            },
        ),
        (
            ClientCommand::CreateRoom { id: rid() },
            RoomCommand::CreateRoom { id: rid() },
        ),
        (
            ClientCommand::JoinRoom {
                id: rid(),
                monitor: true,
            },
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: true,
            },
        ),
        (ClientCommand::LeaveRoom, RoomCommand::LeaveRoom),
        (
            ClientCommand::LockRoom { lock: false },
            RoomCommand::LockRoom { lock: false },
        ),
        (
            ClientCommand::CycleRoom { cycle: true },
            RoomCommand::CycleRoom { cycle: true },
        ),
        (
            ClientCommand::SelectChart { id: 7 },
            RoomCommand::SelectChart { id: 7 },
        ),
        (ClientCommand::RequestStart, RoomCommand::RequestStart),
        (ClientCommand::Ready, RoomCommand::Ready),
        (ClientCommand::CancelReady, RoomCommand::CancelReady),
        (
            ClientCommand::Played { id: 3 },
            RoomCommand::Played { id: 3 },
        ),
        (ClientCommand::Abort, RoomCommand::Abort),
    ];
    for (client, expected) in cases {
        assert_eq!(client_to_room(client), Some(expected));
    }
}

#[test]
fn table1_ping_auth_are_core() {
    // 心跳 / 鉴权归 core，不派发房间（§4.9-3）
    assert_eq!(
        client_to_room(ClientCommand::Ping),
        None,
        "Ping 不应派发房间"
    );
    assert_eq!(
        client_to_room(ClientCommand::Authenticate {
            token: Varchar::new("t".into()).unwrap(),
        }),
        None,
        "Authenticate 不应派发房间"
    );
}

// —— 表 2：RoomEvent → (Targets, ServerCommand) ——

#[test]
fn table2_chat() {
    let out = event_to_server(RoomEvent::Chat {
        room_id: rid(),
        user: 1,
        content: "hi".into(),
    });
    assert_eq!(
        out,
        vec![(
            Targets::All,
            ServerCommand::Message(Message::Chat {
                user: 1,
                content: "hi".into()
            })
        )]
    );
}

#[test]
fn table2_user_joined_double_broadcast() {
    let ui = UserInfo {
        id: 5,
        name: "p5".into(),
        monitor: false,
    };
    let out = event_to_server(RoomEvent::UserJoined {
        room_id: rid(),
        user: ui.clone(),
    });
    assert_eq!(
        out,
        vec![
            (Targets::All, ServerCommand::OnJoinRoom(ui.clone())),
            (
                Targets::All,
                ServerCommand::Message(Message::JoinRoom {
                    user: 5,
                    name: "p5".into()
                })
            ),
        ]
    );
}

#[test]
fn table2_user_left_with_name() {
    // 非机械点：LeaveRoom 广播带 name（事件携带，§6.6 表 2）
    let out = event_to_server(RoomEvent::UserLeft {
        room_id: rid(),
        user: 2,
        name: "p2".into(),
    });
    assert_eq!(
        out,
        vec![(
            Targets::All,
            ServerCommand::Message(Message::LeaveRoom {
                user: 2,
                name: "p2".into()
            })
        )]
    );
}

#[test]
fn table2_new_host_bidirectional() {
    // 非机械点：NewHost → Message + ChangeHost(true) 单播新 + ChangeHost(false) 单播旧
    let out = event_to_server(RoomEvent::NewHost {
        room_id: rid(),
        new_host: 3,
        old_host: 1,
    });
    assert_eq!(
        out,
        vec![
            (
                Targets::All,
                ServerCommand::Message(Message::NewHost { user: 3 })
            ),
            (Targets::Specific(vec![3]), ServerCommand::ChangeHost(true)),
            (Targets::Specific(vec![1]), ServerCommand::ChangeHost(false)),
        ]
    );
}

#[test]
fn table2_select_chart_with_state() {
    let out = event_to_server(RoomEvent::SelectChart {
        room_id: rid(),
        user: 1,
        name: "chart".into(),
        id: 9,
    });
    assert_eq!(
        out,
        vec![
            (
                Targets::All,
                ServerCommand::Message(Message::SelectChart {
                    user: 1,
                    name: "chart".into(),
                    id: 9
                })
            ),
            (
                Targets::All,
                ServerCommand::ChangeState(RoomState::SelectChart(Some(9)))
            ),
        ]
    );
}

#[test]
fn table2_game_start_state() {
    let out = event_to_server(RoomEvent::GameStart {
        room_id: rid(),
        user: 1,
    });
    assert_eq!(
        out,
        vec![
            (
                Targets::All,
                ServerCommand::Message(Message::GameStart { user: 1 })
            ),
            (
                Targets::All,
                ServerCommand::ChangeState(RoomState::WaitingForReady)
            ),
        ]
    );
}

#[test]
fn table2_cancel_game_preserves_chart() {
    // 非机械点：CancelGame 回 SelectChart 且谱面保留（原版语义，§6.6 表 2 注）
    let out = event_to_server(RoomEvent::CancelGame {
        room_id: rid(),
        user: 1,
        chart: Some(4),
    });
    assert_eq!(
        out,
        vec![
            (
                Targets::All,
                ServerCommand::Message(Message::CancelGame { user: 1 })
            ),
            (
                Targets::All,
                ServerCommand::ChangeState(RoomState::SelectChart(Some(4)))
            ),
        ]
    );
}

#[test]
fn table2_start_playing_state() {
    let out = event_to_server(RoomEvent::StartPlaying { room_id: rid() });
    assert_eq!(
        out,
        vec![
            (Targets::All, ServerCommand::Message(Message::StartPlaying)),
            (Targets::All, ServerCommand::ChangeState(RoomState::Playing)),
        ]
    );
}

#[test]
fn table2_game_end_preserves_chart() {
    // 非机械点：GameEnd 回 SelectChart 且谱面保留
    let out = event_to_server(RoomEvent::GameEnd {
        room_id: rid(),
        chart: Some(2),
    });
    assert_eq!(
        out,
        vec![
            (Targets::All, ServerCommand::Message(Message::GameEnd)),
            (
                Targets::All,
                ServerCommand::ChangeState(RoomState::SelectChart(Some(2)))
            ),
        ]
    );
}

#[test]
fn table2_played() {
    let out = event_to_server(RoomEvent::Played {
        room_id: rid(),
        user: 2,
        score: 100,
        accuracy: 0.99,
        full_combo: true,
    });
    assert_eq!(
        out,
        vec![(
            Targets::All,
            ServerCommand::Message(Message::Played {
                user: 2,
                score: 100,
                accuracy: 0.99,
                full_combo: true
            })
        )]
    );
}

#[test]
fn table2_relay_targets_passthrough() {
    // 热路径：RelayTouches → Touches，targets 原样透传（§6.5-17）
    let frames = Arc::new(vec![TouchFrame {
        time: 1.0,
        points: vec![],
    }]);
    let out = event_to_server(RoomEvent::RelayTouches {
        room_id: rid(),
        targets: Targets::Specific(vec![99]),
        player: 2,
        frames: Arc::clone(&frames),
    });
    assert_eq!(
        out,
        vec![(
            Targets::Specific(vec![99]),
            ServerCommand::Touches {
                player: 2,
                frames: Arc::clone(&frames)
            }
        )]
    );
}

#[test]
fn table2_room_closed_no_output() {
    // core 内部信号，无协议输出（§4.9-9）
    assert!(event_to_server(RoomEvent::RoomClosed { room_id: rid() }).is_empty());
}

#[test]
fn table2_abort_lock_cycle() {
    let abort = event_to_server(RoomEvent::Abort {
        room_id: rid(),
        user: 3,
    });
    assert_eq!(
        abort,
        vec![(
            Targets::All,
            ServerCommand::Message(Message::Abort { user: 3 })
        )]
    );
    let lock = event_to_server(RoomEvent::LockRoom {
        room_id: rid(),
        lock: true,
    });
    assert_eq!(
        lock,
        vec![(
            Targets::All,
            ServerCommand::Message(Message::LockRoom { lock: true })
        )]
    );
    let cycle = event_to_server(RoomEvent::CycleRoom {
        room_id: rid(),
        cycle: false,
    });
    assert_eq!(
        cycle,
        vec![(
            Targets::All,
            ServerCommand::Message(Message::CycleRoom { cycle: false })
        )]
    );
}

#[test]
fn table2_room_created() {
    let out = event_to_server(RoomEvent::RoomCreated {
        room_id: rid(),
        host: 1,
    });
    assert_eq!(
        out,
        vec![(
            Targets::All,
            ServerCommand::Message(Message::CreateRoom { user: 1 })
        )]
    );
}

// RoomEvent 构造辅助：确认未使用的导入不报错
#[allow(dead_code)]
fn _unused(_: RoomError) {}
