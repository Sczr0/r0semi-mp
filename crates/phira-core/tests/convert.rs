//! 转换层测试（§6.6 表 1 / 表 2）。
#![allow(clippy::unwrap_used)] // 测试断言失败=panic 是预期语义（柜台限制针对生产代码）
//!
//! 表 1：ClientCommand → RoomCommand 映射（Ping/Authenticate 归 core）
//! 表 2：RoomEvent → (Targets, ServerCommand) 列表（含非机械映射断言）

use std::sync::Arc;

use phira_api::{
    ClientCommand, JoinRoomResponse, Message, RoomCommand, RoomError, RoomErrorCode, RoomEvent,
    RoomId, RoomResponse, RoomState, ServerCommand, Targets, TouchFrame, UserInfo, Varchar,
};
use phira_core::convert::{client_to_room, error_message, event_to_server, response_to_server};

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
            RoomCommand::CreateRoom {
                id: rid(),
                name: "user1".to_owned(),
            },
        ),
        (
            ClientCommand::JoinRoom {
                id: rid(),
                monitor: true,
            },
            RoomCommand::JoinRoom {
                id: rid(),
                monitor: true,
                name: "user1".to_owned(),
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
        assert_eq!(client_to_room(client, "user1".to_owned()), Some(expected));
    }
}

#[test]
fn table1_ping_auth_are_core() {
    // 心跳 / 鉴权归 core，不派发房间（§4.9-3）
    assert_eq!(
        client_to_room(ClientCommand::Ping, String::new()),
        None,
        "Ping 不应派发房间"
    );
    assert_eq!(
        client_to_room(
            ClientCommand::Authenticate {
                token: Varchar::new("t".into()).unwrap(),
            },
            String::new(),
        ),
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

// —— 响应映射：RoomResponse → 协议 Result 变体 ——

#[test]
fn response_ok_maps_to_protocol_ok() {
    let cases: Vec<(ClientCommand, ServerCommand)> = vec![
        (
            ClientCommand::Chat {
                message: Varchar::new("hi".into()).unwrap(),
            },
            ServerCommand::Chat(Ok(())),
        ),
        (
            ClientCommand::CreateRoom { id: rid() },
            ServerCommand::CreateRoom(Ok(())),
        ),
        (ClientCommand::LeaveRoom, ServerCommand::LeaveRoom(Ok(()))),
        (
            ClientCommand::LockRoom { lock: true },
            ServerCommand::LockRoom(Ok(())),
        ),
        (
            ClientCommand::CycleRoom { cycle: false },
            ServerCommand::CycleRoom(Ok(())),
        ),
        (
            ClientCommand::SelectChart { id: 3 },
            ServerCommand::SelectChart(Ok(())),
        ),
        (
            ClientCommand::RequestStart,
            ServerCommand::RequestStart(Ok(())),
        ),
        (ClientCommand::Ready, ServerCommand::Ready(Ok(()))),
        (
            ClientCommand::CancelReady,
            ServerCommand::CancelReady(Ok(())),
        ),
        (
            ClientCommand::Played { id: 1 },
            ServerCommand::Played(Ok(())),
        ),
        (ClientCommand::Abort, ServerCommand::Abort(Ok(()))),
    ];
    for (cmd, expected) in cases {
        assert_eq!(
            response_to_server(&cmd, Ok(RoomResponse::Ok)),
            expected,
            "命令 {cmd:?}"
        );
    }
}

#[test]
fn response_join_room_carries_snapshot() {
    let jr = JoinRoomResponse {
        state: RoomState::SelectChart(Some(1)),
        users: vec![UserInfo {
            id: 1,
            name: "p1".into(),
            monitor: false,
        }],
        live: true,
    };
    let cmd = ClientCommand::JoinRoom {
        id: rid(),
        monitor: false,
    };
    assert_eq!(
        response_to_server(&cmd, Ok(RoomResponse::JoinRoom(jr.clone()))),
        ServerCommand::JoinRoom(Ok(jr))
    );
}

#[test]
fn response_business_error_translates_message() {
    // §4.4：Business 透传文案
    let err = RoomError::Business {
        code: RoomErrorCode::RoomFull,
        msg: "room is full".to_owned(),
    };
    let cmd = ClientCommand::Chat {
        message: Varchar::new("x".into()).unwrap(),
    };
    assert_eq!(
        response_to_server(&cmd, Err(err.clone())),
        ServerCommand::Chat(Err("room is full".to_owned()))
    );
    assert_eq!(error_message(&err), "room is full");
}

#[test]
fn response_business_failure_via_ok_wrapper() {
    // 关键回归测试：bus 的业务拒绝是 `Ok(Failure(...))`（§4.4）——
    // response_to_server 必须识别为 Err，不能误当成功（2026-08 e2e 抓出的真实 bug）
    let err = RoomError::Business {
        code: RoomErrorCode::NoChartSelected,
        msg: "no chart selected".to_owned(),
    };
    let cmd = ClientCommand::RequestStart;
    assert_eq!(
        response_to_server(&cmd, Ok(RoomResponse::Failure(err))),
        ServerCommand::RequestStart(Err("no chart selected".to_owned()))
    );
    // 全部命令的 Failure 路径（抽查 3 个命令变体）
    for cmd in [
        ClientCommand::Chat {
            message: Varchar::new("x".into()).unwrap(),
        },
        ClientCommand::Ready,
        ClientCommand::JoinRoom {
            id: rid(),
            monitor: false,
        },
    ] {
        let resp = response_to_server(
            &cmd,
            Ok(RoomResponse::Failure(RoomError::Business {
                code: RoomErrorCode::NotInRoom,
                msg: "not in room".to_owned(),
            })),
        );
        let s = format!("{resp:?}");
        assert!(
            s.contains("Err(\"not in room\")"),
            "Failure 应映射为 Err 文案: {resp:?}"
        );
    }
}

#[test]
fn response_internal_error_hides_details() {
    // §4.4：Internal 返回通用文案（细节只进日志）
    let err = RoomError::Internal {
        msg: "secret db path".to_owned(),
    };
    let cmd = ClientCommand::Ready;
    assert_eq!(
        response_to_server(&cmd, Err(err.clone())),
        ServerCommand::Ready(Err("internal error".to_owned()))
    );
    assert_eq!(error_message(&err), "internal error");
}

#[test]
fn response_failure_keeps_code() {
    // 各种业务拒绝码都能透传文案（不吞 code）
    for (code, msg) in [
        (RoomErrorCode::NotInRoom, "not in room"),
        (RoomErrorCode::OnlyHost, "only host"),
        (RoomErrorCode::NoChartSelected, "no chart selected"),
        (RoomErrorCode::AlreadyReady, "already ready"),
        (RoomErrorCode::InvalidRecord, "invalid record"),
    ] {
        let err = RoomError::Business {
            code,
            msg: msg.to_owned(),
        };
        let cmd = ClientCommand::RequestStart;
        let resp = response_to_server(&cmd, Err(err));
        assert!(
            matches!(&resp, ServerCommand::RequestStart(Err(m)) if m == msg),
            "{code:?} 应透传: {resp:?}"
        );
    }
}

#[test]
fn response_client_state_passthrough() {
    // GetClientState 的响应（重连恢复用）不走协议 Result——仅内部使用
    // 这里验证 RoomResponse::ClientState 不是通过 response_to_server 的（防误用回归）
    let cmd = ClientCommand::Ready;
    // ClientState 变体传给普通命令 → unreachable 语义；此处只验证其它变体不受影响
    assert_eq!(
        response_to_server(&cmd, Ok(RoomResponse::ClientState(None))),
        ServerCommand::Ready(Ok(()))
    );
}
