//! 协议层测试（阶段 1 验收：Oracle 一致性，§14 阶段 1）。
//!
//! 三层断言：
//! 1. **Golden 字节**（Oracle 第一形态）：手工按 §6.1/§6.2/§6.3 编码规则推导的字节流，
//!    逐字节比对——编码器输出 = 协议规范（原版 phira-mp-common 宏生成行为一致）。
//! 2. **Roundtrip**：encode → decode → 值一致（读写对称，§6.2）。
//! 3. **错误路径**：截断 / 非法 tag / Varchar 超长 / ULEB 溢出 / 数组超长防攻击。

use std::collections::HashMap;
use std::sync::Arc;

use phira_api::{
    ClientCommand, ClientRoomState, CompactPos, DecodeError, JoinRoomResponse, JudgeEvent,
    Judgement, Message, RoomId, RoomState, ServerCommand, TouchFrame, UserInfo, Varchar,
    decode_packet, encode_packet,
};

fn enc<T: phira_api::BinaryData>(v: &T) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_packet(v, &mut buf);
    buf
}

// —— 1. Golden 字节（Oracle 第一形态） ——

#[test]
fn golden_client_ping() {
    assert_eq!(enc(&ClientCommand::Ping), vec![0x00]);
}

#[test]
fn golden_client_authenticate() {
    let cmd = ClientCommand::Authenticate {
        token: Varchar::new("abc".into()).unwrap(),
    };
    // tag=1, Varchar: uleb(3)+"abc"
    assert_eq!(enc(&cmd), vec![0x01, 0x03, 0x61, 0x62, 0x63]);
}

#[test]
fn golden_client_chat() {
    let cmd = ClientCommand::Chat {
        message: Varchar::new("hi".into()).unwrap(),
    };
    // tag=2, uleb(2)+"hi"
    assert_eq!(enc(&cmd), vec![0x02, 0x02, 0x68, 0x69]);
}

#[test]
fn golden_client_touches_empty() {
    // tag=3, Vec: uleb(0)
    assert_eq!(
        enc(&ClientCommand::Touches {
            frames: Arc::new(Vec::new()),
        }),
        vec![0x03, 0x00]
    );
}

#[test]
fn golden_client_touches_frame() {
    let cmd = ClientCommand::Touches {
        frames: Arc::new(vec![TouchFrame {
            time: 1.5,
            points: vec![(1, CompactPos::new(2.0, 3.0))],
        }]),
    };
    // tag=3, uleb(1), f32 1.5 LE(3FC00000), points uleb(1), i8=1,
    // f16(2.0)=4000 LE, f16(3.0)=4200 LE
    assert_eq!(
        enc(&cmd),
        vec![
            0x03, 0x01, 0x00, 0x00, 0xC0, 0x3F, 0x01, 0x01, 0x00, 0x40, 0x00, 0x42
        ]
    );
}

#[test]
fn golden_client_judges() {
    let cmd = ClientCommand::Judges {
        judges: Arc::new(vec![JudgeEvent {
            time: 0.25,
            line_id: 2,
            note_id: 3,
            judgement: Judgement::Perfect,
        }]),
    };
    // tag=4, uleb(1), f32 0.25 LE(3E800000), u32 2, u32 3, Judgement tag 0
    assert_eq!(
        enc(&cmd),
        vec![
            0x04, 0x01, 0x00, 0x00, 0x80, 0x3E, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
            0x00
        ]
    );
}

#[test]
fn golden_client_create_room() {
    let cmd = ClientCommand::CreateRoom {
        id: RoomId::new("room1".into()).unwrap(),
    };
    // tag=5, uleb(5)+"room1"
    assert_eq!(enc(&cmd), vec![0x05, 0x05, 0x72, 0x6F, 0x6F, 0x6D, 0x31]);
}

#[test]
fn golden_client_join_room() {
    let cmd = ClientCommand::JoinRoom {
        id: RoomId::new("ab".into()).unwrap(),
        monitor: true,
    };
    // tag=6, uleb(2)+"ab", bool=1
    assert_eq!(enc(&cmd), vec![0x06, 0x02, 0x61, 0x62, 0x01]);
}

#[test]
fn golden_client_simple_commands() {
    // tag 7-15 的定长命令
    assert_eq!(enc(&ClientCommand::LeaveRoom), vec![0x07]);
    assert_eq!(
        enc(&ClientCommand::LockRoom { lock: false }),
        vec![0x08, 0x00]
    );
    assert_eq!(
        enc(&ClientCommand::CycleRoom { cycle: true }),
        vec![0x09, 0x01]
    );
    assert_eq!(
        enc(&ClientCommand::SelectChart { id: 7 }),
        vec![0x0A, 0x07, 0x00, 0x00, 0x00]
    );
    assert_eq!(enc(&ClientCommand::RequestStart), vec![0x0B]);
    assert_eq!(enc(&ClientCommand::Ready), vec![0x0C]);
    assert_eq!(enc(&ClientCommand::CancelReady), vec![0x0D]);
    assert_eq!(
        enc(&ClientCommand::Played { id: 42 }),
        vec![0x0E, 0x2A, 0x00, 0x00, 0x00]
    );
    assert_eq!(enc(&ClientCommand::Abort), vec![0x0F]);
}

#[test]
fn golden_server_commands() {
    // Pong
    assert_eq!(enc(&ServerCommand::Pong), vec![0x00]);
    // Chat(Ok(())): tag=2, Result Ok: bool=1, ()
    assert_eq!(enc(&ServerCommand::Chat(Ok(()))), vec![0x02, 0x01]);
    // Chat(Err("bad")): tag=2, bool=0, uleb(3)+"bad"
    assert_eq!(
        enc(&ServerCommand::Chat(Err("bad".to_owned()))),
        vec![0x02, 0x00, 0x03, 0x62, 0x61, 0x64]
    );
    // ChangeState(WaitingForReady): tag=6, RoomState tag=1
    assert_eq!(
        enc(&ServerCommand::ChangeState(RoomState::WaitingForReady)),
        vec![0x06, 0x01]
    );
    // ChangeState(SelectChart(Some(5))): tag=6, RoomState tag=0, Option Some: 1, i32 5
    assert_eq!(
        enc(&ServerCommand::ChangeState(RoomState::SelectChart(Some(5)))),
        vec![0x06, 0x00, 0x01, 0x05, 0x00, 0x00, 0x00]
    );
    // ChangeHost(true): tag=7, bool=1
    assert_eq!(enc(&ServerCommand::ChangeHost(true)), vec![0x07, 0x01]);
}

#[test]
fn golden_server_message() {
    // Message(GameEnd): tag=5, Message tag=12
    assert_eq!(
        enc(&ServerCommand::Message(Message::GameEnd)),
        vec![0x05, 0x0C]
    );
    // Message(Chat{user:1, content:"yo"}): tag=5, Message tag=0, i32 1, uleb(2)+"yo"
    assert_eq!(
        enc(&ServerCommand::Message(Message::Chat {
            user: 1,
            content: "yo".to_owned(),
        })),
        vec![0x05, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x79, 0x6F]
    );
}

#[test]
fn golden_authenticate_response() {
    // Authenticate(Ok((UserInfo, Some(ClientRoomState)))) 全链路
    let resp = ServerCommand::Authenticate(Ok((
        UserInfo {
            id: 100,
            name: "p1".to_owned(),
            monitor: false,
        },
        Some(ClientRoomState {
            id: RoomId::new("ab".into()).unwrap(),
            state: RoomState::SelectChart(None),
            live: false,
            locked: false,
            cycle: false,
            is_host: true,
            is_ready: false,
            users: HashMap::from([(
                100,
                UserInfo {
                    id: 100,
                    name: "p1".to_owned(),
                    monitor: false,
                },
            )]),
            last_game_time: f32::NEG_INFINITY,
        }),
    )));
    // tag=1, Result Ok: 1, UserInfo(i32 100, uleb(2)+"p1", bool 0),
    // Option Some: 1, ClientRoomState: RoomId(uleb(2)+"ab"), RoomState tag=0 +
    //   Option<i32> None: 0, bool: live=0 locked=0 cycle=0 is_host=1 is_ready=0,
    //   HashMap uleb(1) + (key i32 100, UserInfo{100,"p1",false})
    let expected = vec![
        0x01, // Authenticate
        0x01, // Ok
        0x64, 0x00, 0x00, 0x00, // user.id = 100 LE
        0x02, 0x70, 0x31, // name "p1"
        0x00, // monitor=false
        0x01, // Option Some
        0x02, 0x61, 0x62, // room id "ab"
        0x00, // RoomState::SelectChart
        0x00, // chart id None
        0x00, 0x00, 0x00, 0x01, 0x00, // live locked cycle is_host is_ready
        0x01, // HashMap len=1
        0x64, 0x00, 0x00, 0x00, // key: i32 100
        0x64, 0x00, 0x00, 0x00, // UserInfo.id = 100
        0x02, 0x70, 0x31, // name "p1"
        0x00, // monitor=false
        0x00, 0x00, 0x80,
        0xFF, // last_game_time = f32::NEG_INFINITY LE（尾追加，ISSUE-0007）
    ];
    assert_eq!(enc(&resp), expected);
}

// —— 2. Roundtrip（读写对称，§6.2） ——

#[test]
fn roundtrip_all_client_commands() {
    let cases = vec![
        ClientCommand::Ping,
        ClientCommand::Authenticate {
            token: Varchar::new("tok123".into()).unwrap(),
        },
        ClientCommand::Chat {
            message: Varchar::new("你好 hello".into()).unwrap(),
        },
        ClientCommand::Touches {
            frames: Arc::new(vec![TouchFrame {
                time: 1.25,
                points: vec![(0, CompactPos::new(-1.5, 2.75))],
            }]),
        },
        ClientCommand::Judges {
            judges: Arc::new(vec![JudgeEvent {
                time: 0.5,
                line_id: 7,
                note_id: 8,
                judgement: Judgement::HoldGood,
            }]),
        },
        ClientCommand::CreateRoom {
            id: RoomId::new("a-b_c9".into()).unwrap(),
        },
        ClientCommand::JoinRoom {
            id: RoomId::new("x".into()).unwrap(),
            monitor: true,
        },
        ClientCommand::LeaveRoom,
        ClientCommand::LockRoom { lock: true },
        ClientCommand::CycleRoom { cycle: false },
        ClientCommand::SelectChart { id: -3 },
        ClientCommand::RequestStart,
        ClientCommand::Ready,
        ClientCommand::CancelReady,
        ClientCommand::Played { id: 0 },
        ClientCommand::Abort,
    ];
    for cmd in cases {
        let bytes = enc(&cmd);
        let decoded: ClientCommand = decode_packet(&bytes).unwrap();
        assert_eq!(decoded, cmd, "roundtrip failed for {cmd:?}");
    }
}

#[test]
fn roundtrip_all_server_commands() {
    let ui = UserInfo {
        id: 1,
        name: "n".into(),
        monitor: true,
    };
    let state = ClientRoomState {
        id: RoomId::new("r".into()).unwrap(),
        state: RoomState::Playing,
        live: true,
        locked: true,
        cycle: true,
        is_host: false,
        is_ready: true,
        users: HashMap::new(),
        last_game_time: 12.5,
    };
    let cases = vec![
        ServerCommand::Pong,
        ServerCommand::Authenticate(Ok((ui.clone(), Some(state.clone())))),
        ServerCommand::Authenticate(Err("token invalid".into())),
        ServerCommand::Chat(Ok(())),
        ServerCommand::Chat(Err("no room".into())),
        ServerCommand::Touches {
            player: 2,
            frames: Arc::new(vec![]),
        },
        ServerCommand::Judges {
            player: 3,
            judges: Arc::new(vec![]),
        },
        ServerCommand::Message(Message::StartPlaying),
        ServerCommand::Message(Message::Played {
            user: 9,
            score: 1000000,
            accuracy: 0.99,
            full_combo: true,
        }),
        ServerCommand::ChangeState(RoomState::Playing),
        ServerCommand::ChangeHost(false),
        ServerCommand::CreateRoom(Ok(())),
        ServerCommand::JoinRoom(Ok(JoinRoomResponse {
            state: RoomState::SelectChart(Some(1)),
            users: vec![ui.clone()],
            live: false,
        })),
        ServerCommand::OnJoinRoom(ui.clone()),
        ServerCommand::LeaveRoom(Ok(())),
        ServerCommand::LockRoom(Err("not host".into())),
        ServerCommand::CycleRoom(Ok(())),
        ServerCommand::SelectChart(Ok(())),
        ServerCommand::RequestStart(Err("no chart".into())),
        ServerCommand::Ready(Ok(())),
        ServerCommand::CancelReady(Ok(())),
        ServerCommand::Played(Ok(())),
        ServerCommand::Abort(Ok(())),
    ];
    for cmd in cases {
        let bytes = enc(&cmd);
        let decoded: ServerCommand = decode_packet(&bytes).unwrap();
        assert_eq!(decoded, cmd, "roundtrip failed for {cmd:?}");
    }
}

#[test]
fn roundtrip_all_messages() {
    let cases = vec![
        Message::Chat {
            user: 1,
            content: "hi".into(),
        },
        Message::CreateRoom { user: 2 },
        Message::JoinRoom {
            user: 3,
            name: "a".into(),
        },
        Message::LeaveRoom {
            user: 4,
            name: "b".into(),
        },
        Message::NewHost { user: 5 },
        Message::SelectChart {
            user: 6,
            name: "c".into(),
            id: 7,
        },
        Message::GameStart { user: 8 },
        Message::Ready { user: 9 },
        Message::CancelReady { user: 10 },
        Message::CancelGame { user: 11 },
        Message::StartPlaying,
        Message::Played {
            user: 12,
            score: 100,
            accuracy: 0.5,
            full_combo: false,
        },
        Message::GameEnd,
        Message::Abort { user: 13 },
        Message::LockRoom { lock: true },
        Message::CycleRoom { cycle: false },
    ];
    for msg in cases {
        let bytes = enc(&msg);
        let decoded: Message = decode_packet(&bytes).unwrap();
        assert_eq!(decoded, msg, "roundtrip failed for {msg:?}");
    }
}

#[test]
fn roundtrip_uleb_multi_byte() {
    // ULEB128 多字节只在**长度字段**（§6.2：整数固定小端，容器长度 ULEB）：
    // Chat 消息 128 字节 → uleb(128) = [0x80, 0x01]
    let msg = "x".repeat(128);
    let cmd = ClientCommand::Chat {
        message: Varchar::new(msg.clone()).unwrap(),
    };
    let bytes = enc(&cmd);
    assert_eq!(&bytes[..3], &[0x02, 0x80, 0x01]);
    assert_eq!(bytes.len(), 3 + 128);
    let decoded: ClientCommand = decode_packet(&bytes).unwrap();
    assert_eq!(decoded, cmd);

    // 127 字节 → uleb(127) = [0x7F]（单字节边界）
    let msg = "y".repeat(127);
    let cmd = ClientCommand::Chat {
        message: Varchar::new(msg).unwrap(),
    };
    let bytes = enc(&cmd);
    assert_eq!(&bytes[..2], &[0x02, 0x7F]);
    assert_eq!(bytes.len(), 2 + 127);
}

// —— 3. 错误路径（防攻击 / 防御性） ——

#[test]
fn err_truncated_packet() {
    let bytes = [0x01, 0x05, 0x61, 0x62]; // Authenticate 声明 len=5 但只有 2 字节
    assert_eq!(
        decode_packet::<ClientCommand>(&bytes),
        Err(DecodeError::Eof)
    );
    assert_eq!(decode_packet::<ClientCommand>(&[]), Err(DecodeError::Eof));
}

#[test]
fn err_unknown_tag() {
    assert_eq!(
        decode_packet::<ClientCommand>(&[0x10]),
        Err(DecodeError::InvalidTag(16))
    );
    assert_eq!(
        decode_packet::<ServerCommand>(&[0x14]),
        Err(DecodeError::InvalidTag(20))
    );
    assert_eq!(
        decode_packet::<Message>(&[0x10]),
        Err(DecodeError::InvalidTag(16))
    );
    assert_eq!(
        decode_packet::<Judgement>(&[0x06]),
        Err(DecodeError::InvalidTag(6))
    );
}

#[test]
fn err_chat_message_too_long() {
    // Chat 的 message 是 Varchar<200>（§6.2）；喂 201 字节 → StringTooLong
    let mut bytes = vec![0x02, 0xC9, 0x01]; // tag=2, uleb(201)
    bytes.extend(std::iter::repeat_n(b'a', 201));
    assert_eq!(
        decode_packet::<ClientCommand>(&bytes),
        Err(DecodeError::StringTooLong { max: 200, len: 201 })
    );
}

#[test]
fn err_varchar_too_long() {
    // Authenticate 的 token 是 Varchar<32>；喂 33 字节
    let mut bytes = vec![0x01, 0x21]; // tag=1, uleb(33)
    bytes.extend(std::iter::repeat_n(b'a', 33));
    assert_eq!(
        decode_packet::<ClientCommand>(&bytes),
        Err(DecodeError::StringTooLong { max: 32, len: 33 })
    );
}

#[test]
fn err_uleb_overflow() {
    // Touches 后 Vec 长度的 uleb 连续 0x80（10 字节）→ shift 超过 64 位
    let bytes = [
        0x03u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
    ];
    assert_eq!(
        decode_packet::<ClientCommand>(&bytes),
        Err(DecodeError::UlebOverflow)
    );
}

#[test]
fn err_array_too_large() {
    // Judges 的数组长度 1000 但无后续字节 → 防分配攻击
    let bytes = [0x04u8, 0xE8, 0x07];
    assert_eq!(
        decode_packet::<ClientCommand>(&bytes),
        Err(DecodeError::ArrayTooLarge {
            len: 1000,
            remaining: 0
        })
    );
}

#[test]
fn err_invalid_room_id_chars() {
    // CreateRoom 的 id 含非法字符 `*`
    let bytes = vec![0x05u8, 0x01, 0x2A]; // tag=5, uleb(1), "*"=0x2A
    assert!(matches!(
        decode_packet::<ClientCommand>(&bytes),
        Err(DecodeError::InvalidRoomId(_))
    ));
}

#[test]
fn err_empty_room_id() {
    // CreateRoom 的 id 为空
    let bytes = vec![0x05u8, 0x00]; // tag=5, uleb(0)
    assert!(matches!(
        decode_packet::<ClientCommand>(&bytes),
        Err(DecodeError::InvalidRoomId(_))
    ));
}

// —— 3. 尾部字节容忍（演进约束，client-behavior-review §6） ——

/// 服务端在结构体**尾部追加字段**后，旧读端（逐字段读、不校验剩余）必须静默忽略
/// 多余字节——这是 game_time（ISSUE-0007）等尾追加演进的兼容前提：
/// 新服务端发的帧可能被旧客户端消费（原版 derive 读端同样不校验剩余）。
#[test]
fn trailing_bytes_after_struct_fields_tolerated() {
    // 最小合法 Authenticate(Ok) 响应 + 尾部 6 字节垃圾（模拟未来版本追加的字段）
    // 布尔走 1 字节（0/1）；Result::Ok = 1；Option::None = 0
    let mut bytes = vec![
        0x01u8, // tag = Authenticate
        0x01,   // Result::Ok（bool 1 字节）
        // UserInfo: id=1(LE i32), name uleb(1)="p", monitor=false
        0x01, 0x00, 0x00, 0x00, 0x01, b'p', 0x00, // Option<ClientRoomState> = None
        0x00,
        // —— 未来版本的尾追加字段（此处为垃圾占位，如 f32 game_time bits）——
        0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02,
    ];
    let decoded: ServerCommand = decode_packet(&bytes).unwrap();
    match decoded {
        ServerCommand::Authenticate(Ok((ui, None))) => {
            assert_eq!(ui.id, 1);
            assert_eq!(ui.name, "p");
        }
        other => panic!("expected Authenticate(Ok((user, None))), got {other:?}"),
    }

    // 同理：JoinRoomResponse 结构体后有尾部字节也必须容忍。
    // ServerCommand::JoinRoom tag=9：ok=1, state tag=2(Playing), users uleb(0), live=1
    bytes = vec![0x09, 0x01, 0x02, 0x00, 0x01, 0xFF, 0xEE];
    let decoded: ServerCommand = decode_packet(&bytes).unwrap();
    assert!(matches!(
        decoded,
        ServerCommand::JoinRoom(Ok(JoinRoomResponse {
            state: RoomState::Playing,
            live: true,
            ..
        }))
    ));
}

/// 反向锚点：枚举变体的未知 tag **不容忍**（bail invalid enum → 客户端断连，真 SDK 行为）
/// ——这条不测本 crate 会 bail 就够（已有 err_invalid_enum 覆盖解码器侧）；此测试钉住的是
/// "trailing = OK, unknown tag = Err"的不对称性，防止未来有人把读端改成"校验剩余长度"。
#[test]
fn unknown_enum_tag_still_rejected() {
    let bytes = vec![0xC8u8]; // tag=200，不存在
    assert!(decode_packet::<ServerCommand>(&bytes).is_err());
}
