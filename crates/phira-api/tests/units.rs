//! phira-api 单元测试（§9 第一层：协议类型与契约类型的边界行为）。

use std::collections::HashMap;

use phira_api::*;

// —— RoomId（协议 §6.2：Varchar<20>，字符 [A-Za-z0-9_-]，非空） ——

#[test]
fn room_id_accepts_valid_chars() {
    for id in [
        "a",
        "A",
        "0",
        "-",
        "_",
        "a-b_c",
        "ABC-123_xyz",
        "___",
        "----",
    ] {
        assert!(RoomId::new(id.to_owned()).is_ok(), "{id:?} 应合法");
    }
}

#[test]
fn room_id_rejects_invalid() {
    for id in ["", " ", "a b", "a/b", "a.b", "中文", "a@b", "a\nb"] {
        assert!(RoomId::new(id.to_owned()).is_err(), "{id:?} 应非法");
    }
}

#[test]
fn room_id_rejects_too_long() {
    // ≤20 合法
    let ok = "a".repeat(20);
    assert!(RoomId::new(ok).is_ok());
    // >20 非法
    let too_long = "a".repeat(21);
    assert!(RoomId::new(too_long).is_err());
}

#[test]
fn room_id_display_and_eq() {
    let a = RoomId::new("room-1".to_owned()).unwrap();
    let b = RoomId::new("room-1".to_owned()).unwrap();
    let c = RoomId::new("room-2".to_owned()).unwrap();
    assert_eq!(a, b);
    assert_ne!(a, c);
    assert_eq!(a.to_string(), "room-1");
    assert_eq!(a.as_str(), "room-1");
}

// —— Varchar（协议 §6.2：长度受限字符串，token ≤32 / chat ≤200 / room ≤20） ——

#[test]
fn varchar_length_boundary() {
    // N 字节内 OK
    let ok = Varchar::<32>::new("t".repeat(32)).unwrap();
    assert_eq!(ok.as_str(), &"t".repeat(32));
    // 超 N 拒绝
    assert!(Varchar::<32>::new("t".repeat(33)).is_err());
    // 空串允许（Varchar 本身不校验非空；RoomId 层才校验）
    assert!(Varchar::<32>::new(String::new()).is_ok());
}

#[test]
fn varchar_byte_len_not_char_len() {
    // 长度按**字节**计（§6.2）：3 字节中文占 9 字节
    let three_chinese = "哈哈哈";
    assert_eq!(three_chinese.len(), 9);
    assert!(Varchar::<8>::new(three_chinese.to_owned()).is_err());
    assert!(Varchar::<9>::new(three_chinese.to_owned()).is_ok());
}

#[test]
fn varchar_into_inner() {
    let v = Varchar::<200>::new("hello".to_owned()).unwrap();
    assert_eq!(v.into_inner(), "hello");
}

// —— CompactPos（协议 §6.2 / §4.8-1：f16 半精度 ×2） ——

#[test]
fn compact_pos_f16_roundtrip() {
    // f16 精度内精确往返
    let p = CompactPos::new(1.5, -2.25);
    assert_eq!(p.x(), 1.5);
    assert_eq!(p.y(), -2.25);
}

#[test]
fn compact_pos_clamps_to_f16_precision() {
    // f16 半精度：小于 0.001 的差值被量化（不要求精确，只要求不 panic 且有限）
    let p = CompactPos::new(0.0001, 0.1);
    assert!(p.x().is_finite());
    assert!(p.y().is_finite());
}

// —— RoomError（§4.4：Business/Internal 两分类；§3.2 错误率只统计 Internal） ——

#[test]
fn room_error_two_classes_display() {
    let business = RoomError::Business {
        code: RoomErrorCode::RoomFull,
        msg: "room is full".to_owned(),
    };
    let internal = RoomError::Internal {
        msg: "boom".to_owned(),
    };
    assert_eq!(business.to_string(), "room is full");
    assert_eq!(internal.to_string(), "internal error: boom");
}

#[test]
fn room_error_code_exhaustive_values() {
    // 所有业务拒绝码可区分（契约测试断言依赖这些判别）
    let codes = [
        RoomErrorCode::AlreadyInRoom,
        RoomErrorCode::RoomIdOccupied,
        RoomErrorCode::RoomNotFound,
        RoomErrorCode::RoomLocked,
        RoomErrorCode::GameOngoing,
        RoomErrorCode::CannotMonitor,
        RoomErrorCode::RoomFull,
        RoomErrorCode::OnlyHost,
        RoomErrorCode::NotInRoom,
        RoomErrorCode::InvalidState,
        RoomErrorCode::NoChartSelected,
        RoomErrorCode::AlreadyReady,
        RoomErrorCode::NotReady,
        RoomErrorCode::InvalidRecord,
        RoomErrorCode::AlreadyUploaded,
        RoomErrorCode::AlreadyAborted,
        RoomErrorCode::TooManyRequests,
    ];
    let mut seen = std::collections::HashSet::new();
    for c in codes {
        assert!(seen.insert(c), "{c:?} 重复");
    }
    assert_eq!(seen.len(), codes.len());
}

// —— 类型可构造性（薄缝形状的编译期验证） ——

#[test]
fn cmd_ctx_and_origin_construct() {
    let ctx = CmdCtx {
        origin: Origin::Client { user_id: 42 },
        room_id: RoomId::new("r".to_owned()).unwrap(),
    };
    assert!(matches!(ctx.origin, Origin::Client { user_id: 42 }));
    assert_eq!(ctx.room_id.as_str(), "r");
}

#[test]
fn room_event_targets_construct() {
    let all = Targets::All;
    let specific = Targets::Specific(vec![1, 2]);
    assert_ne!(all, specific);
}

#[test]
fn client_room_state_construct() {
    let users = HashMap::from([(
        1,
        UserInfo {
            id: 1,
            name: "u1".to_owned(),
            monitor: false,
        },
    )]);
    let state = ClientRoomState {
        id: RoomId::new("r".to_owned()).unwrap(),
        state: RoomState::WaitingForReady,
        live: true,
        locked: false,
        cycle: true,
        is_host: true,
        is_ready: false,
        users,
        last_game_time: f32::NEG_INFINITY,
    };
    assert!(state.is_host);
    assert!(state.live);
    assert!(state.cycle);
}

#[test]
fn room_config_default_is_empty() {
    let cfg = RoomConfig::default();
    assert!(cfg.monitors.is_empty());
}

#[test]
fn touch_frame_and_judge_event_construct() {
    let frame = TouchFrame {
        time: 1.5,
        points: vec![(0, CompactPos::new(0.0, 0.0))],
    };
    assert_eq!(frame.points.len(), 1);

    let judge = JudgeEvent {
        time: 2.0,
        line_id: 3,
        note_id: 4,
        judgement: Judgement::Perfect,
    };
    assert_eq!(judge.judgement, Judgement::Perfect);
}

// —— auth（§4.4） ——

#[test]
fn auth_error_classes() {
    let business = AuthError::Business {
        code: AuthErrorCode::InvalidToken,
        msg: "bad token".to_owned(),
    };
    let internal = AuthError::Internal {
        msg: "api down".to_owned(),
    };
    assert_eq!(business.to_string(), "bad token");
    assert_eq!(internal.to_string(), "internal error: api down");
}

#[test]
fn user_identity_construct() {
    let id = UserIdentity {
        user_id: 7,
        name: "phira".to_owned(),
        lang: "zh".to_owned(),
    };
    assert_eq!(id.user_id, 7);
}

// —— HashMap（§6.3：长度前缀 + K/V 对；入库断言具体错误变体，见 mutants 体检） ——

#[test]
fn hashmap_declared_len_too_large_rejected() {
    // len=2 但剩余仅 1 字节：长度校验应**先行**拒收 ArrayTooLarge——
    // 断言变体而非仅 is_err，确保不是靠后续读取撞出 Eof（两变异方向都要挡住）
    let data = [0x02, 0x2a];
    let err = decode_packet::<HashMap<u8, ()>>(&data).unwrap_err();
    assert!(
        matches!(err, DecodeError::ArrayTooLarge { .. }),
        "长度校验应先行拒绝：{err:?}"
    );
}

#[test]
fn hashmap_exact_remaining_boundary_ok() {
    // len == remaining（每条目恰好 1 字节）：边界成功——
    // 抓住 `>` 误变异为 `>=`/`==`（此时会误拒收）
    let data = [0x01, 0x2a];
    let map = decode_packet::<HashMap<u8, ()>>(&data).unwrap();
    assert_eq!(map.get(&42), Some(&()));
}
