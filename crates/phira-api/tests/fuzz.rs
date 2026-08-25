//! 解码器模糊测试（文档 §9 模糊层承诺兑现）。
//!
//! **威胁模型**：黑客脚本往服务器端口无脑灌垃圾字节流——解码器必须在**任何输入**下
//! 不 panic（返回 `Ok`/`Err` 均可；panic = 服务器当场下线）。
//!
//! 三层强度：
//! 1. **纯随机**（proptest，10000 cases/类型）：任意字节 → 各协议类型解码不 panic
//! 2. **截断穷举**（确定性）：合法编码的每个截断点——半包是真实网络常态（丢包/RST）
//! 3. **结构感知变异**（proptest）：合法编码 + 随机偏移注入/替换——比纯随机更深，
//!    直接命中"长度字段/标签/tag 被篡改"的解析路径

use phira_api::{
    ClientCommand, Message, RoomId, ServerCommand, Varchar, decode_packet, encode_packet,
};
use proptest::prelude::*;

/// 纯随机字节（0-1024 字节——覆盖单字节空输入到跨帧大小）。
fn arbitrary_bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..1024)
}

/// 代表性合法命令（变异/截断基座——覆盖各 tag 与变体形状）。
fn base_client_commands() -> Vec<Vec<u8>> {
    let cmds = [
        ClientCommand::Ping,
        ClientCommand::CreateRoom {
            id: RoomId::new("abc123_-".to_owned()).unwrap(),
        },
        ClientCommand::JoinRoom {
            id: RoomId::new("abc".to_owned()).unwrap(),
            monitor: true,
        },
        ClientCommand::Authenticate {
            token: Varchar::new("t".repeat(32)).unwrap(),
        },
        ClientCommand::Chat {
            message: Varchar::new("hello".to_owned()).unwrap(),
        },
        ClientCommand::Touches {
            frames: std::sync::Arc::new(vec![phira_api::TouchFrame {
                time: 1.5,
                points: vec![
                    (1i8, phira_api::CompactPos::new(0.5, -0.25)),
                    (2i8, phira_api::CompactPos::new(100.0, -100.0)),
                ],
            }]),
        },
        ClientCommand::Judges {
            judges: std::sync::Arc::new(vec![phira_api::JudgeEvent {
                time: 0.0,
                line_id: 1,
                note_id: 2,
                judgement: phira_api::Judgement::Perfect,
            }]),
        },
        ClientCommand::SelectChart { id: i32::MAX },
        ClientCommand::Played { id: i32::MIN },
        ClientCommand::LeaveRoom,
        ClientCommand::LockRoom { lock: true },
        ClientCommand::CycleRoom { cycle: false },
        ClientCommand::RequestStart,
        ClientCommand::Ready,
        ClientCommand::CancelReady,
        ClientCommand::Abort,
    ];
    cmds.iter()
        .map(|c| {
            let mut buf = Vec::new();
            encode_packet(c, &mut buf);
            buf
        })
        .collect()
}

/// 代表性合法 ServerCommand / Message 编码。
fn base_server_commands() -> Vec<Vec<u8>> {
    let cmds = [
        ServerCommand::Pong,
        ServerCommand::Authenticate(Ok((
            phira_api::UserInfo {
                id: 1,
                name: "u".to_owned(),
                monitor: false,
            },
            None,
        ))),
        ServerCommand::Message(Message::Chat {
            user: 1,
            content: "hi".to_owned(),
        }),
        ServerCommand::Message(Message::Played {
            user: 2,
            score: 1_000_000,
            accuracy: f32::NAN, // 特殊浮点值：解码路径不得 panic
            full_combo: true,
        }),
        ServerCommand::ChangeState(phira_api::RoomState::SelectChart(Some(42))),
        ServerCommand::ChangeHost(true),
        ServerCommand::Touches {
            player: 3,
            frames: std::sync::Arc::new(vec![phira_api::TouchFrame {
                time: -0.0,
                points: vec![],
            }]),
        },
    ];
    cmds.iter()
        .map(|c| {
            let mut buf = Vec::new();
            encode_packet(c, &mut buf);
            buf
        })
        .collect()
}

// —— 第 1 层：纯随机字节 ——

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10_000))]

    /// 客户端命令解码：任意字节不 panic（垃圾流攻击主路径）
    #[test]
    fn client_command_never_panics(data in arbitrary_bytes()) {
        let _ = decode_packet::<ClientCommand>(&data);
    }

    /// 服务端命令解码：任意字节不 panic
    #[test]
    fn server_command_never_panics(data in arbitrary_bytes()) {
        let _ = decode_packet::<ServerCommand>(&data);
    }

    /// 消息解码：任意字节不 panic
    #[test]
    fn message_never_panics(data in arbitrary_bytes()) {
        let _ = decode_packet::<Message>(&data);
    }

    /// RoomId 解码（长度前缀 + 字符校验路径）
    #[test]
    fn room_id_never_panics(data in arbitrary_bytes()) {
        let _ = decode_packet::<RoomId>(&data);
    }
}

// —— 第 2 层：截断穷举（半包 / 丢包常态）——

#[test]
fn truncated_client_packets_never_panic() {
    for base in base_client_commands() {
        // 每个截断点（含空、含全量）：半包不得 panic
        for cut in 0..=base.len() {
            let _ = decode_packet::<ClientCommand>(&base[..cut]);
        }
    }
}

#[test]
fn truncated_server_packets_never_panic() {
    for base in base_server_commands() {
        for cut in 0..=base.len() {
            let _ = decode_packet::<ServerCommand>(&base[..cut]);
        }
    }
}

// —— 第 3 层：结构感知变异（合法编码 + 注入/替换）——

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5_000))]

    /// 合法 ClientCommand 编码 + 随机偏移注入随机字节——命中长度字段/嵌套结构被篡改
    #[test]
    fn client_mutation_never_panics(
        base in 0..16usize,   // 基座索引
        inject_at in 0..256usize,
        byte in any::<u8>(),
    ) {
        let bases = base_client_commands();
        let mut data = bases[base % bases.len()].clone();
        let pos = inject_at % (data.len() + 1);
        data.insert(pos, byte);
        let _ = decode_packet::<ClientCommand>(&data);
    }

    /// 合法 ServerCommand 编码 + 随机偏移**替换**——命中 tag/长度前缀被篡改
    #[test]
    fn server_mutation_never_panics(
        base in 0..16usize,
        flip_at in 0..256usize,
        byte in any::<u8>(),
    ) {
        let bases = base_server_commands();
        let mut data = bases[base % bases.len()].clone();
        if !data.is_empty() {
            let pos = flip_at % data.len();
            data[pos] = byte;
        }
        let _ = decode_packet::<ServerCommand>(&data);
    }
}
