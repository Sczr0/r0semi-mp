// SPDX-License-Identifier: Apache-2.0
// 本文件移植自 phira-mp（Apache-2.0，TeamFlos）：phira-mp-common/src/command.rs 的
// 命令字典投影（含协议约束的字段形状与 tag 序）。Apache-2.0 全文见 LICENSE.Apache-2.0，
// 归属声明见 NOTICE。
//! 协议命令层（§6.3）——协议直接投影，无猜测成分。
//!
//! 原版 `phira-mp-common` 的 `command.rs` 移植（Apache-2.0，TeamFlos）。
//!
//! ## tag 顺序即协议（§6.3，评审 §8 二-2）
//!
//! 枚举变体 tag = 声明索引（`u8`，从 0 起）。**中间插入 = 破坏性变更**（后续所有
//! tag 后移，旧客户端解码错位）；**末尾追加 = 兼容**（旧客户端遇到未知 tag 报错）。
//! 手写 `BinaryData` impl 使每个 tag 显式可见；改枚举必须同步改这里 + 契约测试 +
//! 转换层（§6.6），三处联动（评审 §8 二-2）。
//!
//! 心跳常量（§6.1）：客户端每 3s 发 `Ping`，2s 未收 `Pong` 计 1 次失败；
//! 服务端以 10s 无任何包判定断线（判定逻辑在 core 会话层，阶段 2 接线）。

use std::sync::Arc;
use std::time::Duration;

use half::f16;

use crate::binary::{BinaryData, BinaryReader, BinaryWriter, DecodeError, ProtoResult};
use crate::rooms::{
    ClientRoomState, CompactPos, JudgeEvent, Judgement, RoomId, RoomState, TouchFrame, UserInfo,
    Varchar,
};

/// 客户端心跳间隔：每 3s 发一次 `Ping`（§6.1）。
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(3);

/// 客户端心跳超时：2s 未收到 `Pong` 计 1 次失败（§6.1）。
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(2);

/// 服务端断线判定：10s 无任何包判定断线（§6.1；重连窗口同为 10s，§6.5-24）。
pub const HEARTBEAT_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// 协议层结果类型（§6.3）：`Ok` 或错误文案（`Err(String)` 由 core 从 `RoomError`
/// 生成——Business 透传文案，Internal 返回通用文案 + 日志，§4.4）。
pub type SResult<T> = std::result::Result<T, String>;

/// 客户端 → 服务端命令（§6.3；tag 0-15，顺序即协议）。
/// 变体字段名即协议字段名（§6.3 协议直接投影），枚举级文档已覆盖各变体语义——
/// 字段级 missing_docs 豁免（§5.1 红线针对"有语义但没文档"，此处无额外语义可写）。
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq)]
pub enum ClientCommand {
    /// 心跳（客户端每 3s 发，§6.1）。
    Ping,
    /// 鉴权：token（≤32 字节，§6.2 Varchar）。
    Authenticate { token: Varchar<32> },
    /// 聊天：message（≤200 字节）。
    Chat { message: Varchar<200> },
    /// 触摸帧流（热路径，§6.5-16/17；`Arc` 共享避免深拷贝）。
    Touches { frames: Arc<Vec<TouchFrame>> },
    /// 判定流（热路径）。
    Judges { judges: Arc<Vec<JudgeEvent>> },
    /// 创建房间（含房主注册，§6.5-1）。
    CreateRoom { id: RoomId },
    /// 加入房间（`monitor` = 观战者身份，§6.5-1/4）。
    JoinRoom { id: RoomId, monitor: bool },
    /// 离开房间（房主离开触发顺延，§6.5-5）。
    LeaveRoom,
    /// 锁房（仅 host，§6.5-2）。
    LockRoom { lock: bool },
    /// 循环房（仅 host，§6.5-2）。
    CycleRoom { cycle: bool },
    /// 选谱（仅 host，须先选图才能 RequestStart，§6.5-7）。
    SelectChart { id: i32 },
    /// 请求开始（仅 host，§6.5-7）。
    RequestStart,
    /// 准备（§6.5-8）。
    Ready,
    /// 取消准备（host 触发 CancelGame，§6.5-9）。
    CancelReady,
    /// 上报游玩成绩（回源校验 `record.player == id`，§6.5-10）。
    Played { id: i32 },
    /// 中止本局（§6.5-11）。
    Abort,
}

/// 服务端 → 客户端命令（§6.3；tag 0-19，顺序即协议）。
/// 变体字段名即协议字段名（§6.3 协议直接投影），枚举级文档已覆盖各变体语义——
/// 字段级 missing_docs 豁免（§5.1 红线针对"有语义但没文档"，此处无额外语义可写）。
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq)]
pub enum ServerCommand {
    /// 心跳应答（服务端不发 Ping，只回 Pong，§6.1）。
    Pong,
    /// 鉴权结果：成功携带用户信息 + 房间快照（重连恢复用，§6.5-23）。
    Authenticate(SResult<(UserInfo, Option<ClientRoomState>)>),
    /// 聊天结果。
    Chat(SResult<()>),
    /// 触摸帧转播（player 源 + frames，§6.5-16/17）。
    Touches {
        player: i32,
        frames: Arc<Vec<TouchFrame>>,
    },
    /// 判定转播。
    Judges {
        player: i32,
        judges: Arc<Vec<JudgeEvent>>,
    },
    /// 房间广播消息（§6.3 Message）。
    Message(Message),
    /// 房间状态切换（§6.6 表 2：GameStart→WaitingForReady 等，非 Message 变体）。
    ChangeState(RoomState),
    /// 房主变更广播（bool = 当前用户是否为新 host，§6.5-5）。
    ChangeHost(bool),
    /// 创建房间结果。
    CreateRoom(SResult<()>),
    /// 加入房间结果（成功携带房间快照，§6.5-23）。
    JoinRoom(SResult<JoinRoomResponse>),
    /// 新成员加入通知（房内广播，§6.3）。
    OnJoinRoom(UserInfo),
    /// 离开房间结果。
    LeaveRoom(SResult<()>),
    /// 锁房结果。
    LockRoom(SResult<()>),
    /// 循环房结果。
    CycleRoom(SResult<()>),
    /// 选谱结果。
    SelectChart(SResult<()>),
    /// 请求开始结果。
    RequestStart(SResult<()>),
    /// 准备结果。
    Ready(SResult<()>),
    /// 取消准备结果。
    CancelReady(SResult<()>),
    /// 成绩上报结果。
    Played(SResult<()>),
    /// 中止结果。
    Abort(SResult<()>),
}

/// 房间广播消息（§6.3；tag 0-15，顺序即协议；所有变体均为房内广播，§6.5-26）。
/// 变体字段名即协议字段名（§6.3 协议直接投影），枚举级文档已覆盖各变体语义——
/// 字段级 missing_docs 豁免（§5.1 红线针对"有语义但没文档"，此处无额外语义可写）。
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    /// 聊天消息。
    Chat { user: i32, content: String },
    /// 房间创建。
    CreateRoom { user: i32 },
    /// 成员加入（携带昵称）。
    JoinRoom { user: i32, name: String },
    /// 成员离开。
    LeaveRoom { user: i32, name: String },
    /// 房主变更。
    NewHost { user: i32 },
    /// 选谱。
    SelectChart { user: i32, name: String, id: i32 },
    /// 开局（host RequestStart 成功）。
    GameStart { user: i32 },
    /// 成员准备。
    Ready { user: i32 },
    /// 成员取消准备。
    CancelReady { user: i32 },
    /// 取消开局（host CancelReady）。
    CancelGame { user: i32 },
    /// 进入游玩（全员 ready）。
    StartPlaying,
    /// 成绩上报。
    Played {
        user: i32,
        score: i32,
        accuracy: f32,
        full_combo: bool,
    },
    /// 本局结束。
    GameEnd,
    /// 中止本局。
    Abort { user: i32 },
    /// 锁房状态。
    LockRoom { lock: bool },
    /// 循环房状态。
    CycleRoom { cycle: bool },
}

use crate::rooms::JoinRoomResponse;

// —— 协议类型的 BinaryData 实现（tag = 变体声明索引，§6.3） ——

impl BinaryData for CompactPos {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(Self {
            x: f16::from_bits(r.read()?),
            y: f16::from_bits(r.read()?),
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.write_val(self.x.to_bits())?;
        w.write_val(self.y.to_bits())?;
        Ok(())
    }
}

impl<const N: usize> BinaryData for Varchar<N> {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        let len = r.uleb()? as usize;
        if len > N {
            return Err(DecodeError::StringTooLong { max: N, len });
        }
        Ok(Self(String::from_utf8_lossy(r.take(len)?).into_owned()))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.write(&self.0)
    }
}

impl BinaryData for RoomId {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        let v = Varchar::<20>::read_binary(r)?;
        if v.as_str().is_empty()
            || !v
                .as_str()
                .chars()
                .all(|it| it == '-' || it == '_' || it.is_ascii_alphanumeric())
        {
            return Err(DecodeError::InvalidRoomId(v.into_inner()));
        }
        Ok(Self(v))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        self.0.write_binary(w)
    }
}

impl BinaryData for TouchFrame {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(Self {
            time: r.read()?,
            points: r.read()?,
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.write(&self.time)?;
        w.write(&self.points)?;
        Ok(())
    }
}

impl BinaryData for Judgement {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(match r.read::<u8>()? {
            0 => Self::Perfect,
            1 => Self::Good,
            2 => Self::Bad,
            3 => Self::Miss,
            4 => Self::HoldPerfect,
            5 => Self::HoldGood,
            x => return Err(DecodeError::InvalidTag(x)),
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.write_val(match self {
            Self::Perfect => 0u8,
            Self::Good => 1,
            Self::Bad => 2,
            Self::Miss => 3,
            Self::HoldPerfect => 4,
            Self::HoldGood => 5,
        })
    }
}

impl BinaryData for JudgeEvent {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(Self {
            time: r.read()?,
            line_id: r.read()?,
            note_id: r.read()?,
            judgement: r.read()?,
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.write(&self.time)?;
        w.write(&self.line_id)?;
        w.write(&self.note_id)?;
        w.write(&self.judgement)?;
        Ok(())
    }
}

impl BinaryData for RoomState {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(match r.read::<u8>()? {
            0 => Self::SelectChart(r.read()?),
            1 => Self::WaitingForReady,
            2 => Self::Playing,
            x => return Err(DecodeError::InvalidTag(x)),
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        match self {
            Self::SelectChart(id) => {
                w.write_val(0u8)?;
                w.write(id)?;
            }
            Self::WaitingForReady => w.write_val(1u8)?,
            Self::Playing => w.write_val(2u8)?,
        }
        Ok(())
    }
}

impl BinaryData for UserInfo {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(Self {
            id: r.read()?,
            name: r.read()?,
            monitor: r.read()?,
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.write(&self.id)?;
        w.write(&self.name)?;
        w.write(&self.monitor)?;
        Ok(())
    }
}

impl BinaryData for ClientRoomState {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(Self {
            id: r.read()?,
            state: r.read()?,
            live: r.read()?,
            locked: r.read()?,
            cycle: r.read()?,
            is_host: r.read()?,
            is_ready: r.read()?,
            users: r.read()?,
            // ISSUE-0007：尾部追加字段——读端容忍"缺失"不可能（本端总是收到自己写出的
            // 帧格式）；容忍的是**旧对端多发**的尾随数据（见 tests trailing_bytes_*）。
            last_game_time: r.read()?,
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.write(&self.id)?;
        w.write(&self.state)?;
        w.write(&self.live)?;
        w.write(&self.locked)?;
        w.write(&self.cycle)?;
        w.write(&self.is_host)?;
        w.write(&self.is_ready)?;
        w.write(&self.users)?;
        // 尾部追加（ISSUE-0007）：旧客户端读到此即停，剩余字节静默忽略
        w.write(&self.last_game_time)?;
        Ok(())
    }
}

impl BinaryData for JoinRoomResponse {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(Self {
            state: r.read()?,
            users: r.read()?,
            live: r.read()?,
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.write(&self.state)?;
        w.write(&self.users)?;
        w.write(&self.live)?;
        Ok(())
    }
}

impl BinaryData for ClientCommand {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(match r.read::<u8>()? {
            0 => Self::Ping,
            1 => Self::Authenticate { token: r.read()? },
            2 => Self::Chat { message: r.read()? },
            3 => Self::Touches { frames: r.read()? },
            4 => Self::Judges { judges: r.read()? },
            5 => Self::CreateRoom { id: r.read()? },
            6 => Self::JoinRoom {
                id: r.read()?,
                monitor: r.read()?,
            },
            7 => Self::LeaveRoom,
            8 => Self::LockRoom { lock: r.read()? },
            9 => Self::CycleRoom { cycle: r.read()? },
            10 => Self::SelectChart { id: r.read()? },
            11 => Self::RequestStart,
            12 => Self::Ready,
            13 => Self::CancelReady,
            14 => Self::Played { id: r.read()? },
            15 => Self::Abort,
            x => return Err(DecodeError::InvalidTag(x)),
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        match self {
            Self::Ping => w.write_val(0u8)?,
            Self::Authenticate { token } => {
                w.write_val(1u8)?;
                w.write(token)?;
            }
            Self::Chat { message } => {
                w.write_val(2u8)?;
                w.write(message)?;
            }
            Self::Touches { frames } => {
                w.write_val(3u8)?;
                w.write(frames)?;
            }
            Self::Judges { judges } => {
                w.write_val(4u8)?;
                w.write(judges)?;
            }
            Self::CreateRoom { id } => {
                w.write_val(5u8)?;
                w.write(id)?;
            }
            Self::JoinRoom { id, monitor } => {
                w.write_val(6u8)?;
                w.write(id)?;
                w.write(monitor)?;
            }
            Self::LeaveRoom => w.write_val(7u8)?,
            Self::LockRoom { lock } => {
                w.write_val(8u8)?;
                w.write(lock)?;
            }
            Self::CycleRoom { cycle } => {
                w.write_val(9u8)?;
                w.write(cycle)?;
            }
            Self::SelectChart { id } => {
                w.write_val(10u8)?;
                w.write(id)?;
            }
            Self::RequestStart => w.write_val(11u8)?,
            Self::Ready => w.write_val(12u8)?,
            Self::CancelReady => w.write_val(13u8)?,
            Self::Played { id } => {
                w.write_val(14u8)?;
                w.write(id)?;
            }
            Self::Abort => w.write_val(15u8)?,
        }
        Ok(())
    }
}

impl BinaryData for ServerCommand {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(match r.read::<u8>()? {
            0 => Self::Pong,
            1 => Self::Authenticate(r.read()?),
            2 => Self::Chat(r.read()?),
            3 => Self::Touches {
                player: r.read()?,
                frames: r.read()?,
            },
            4 => Self::Judges {
                player: r.read()?,
                judges: r.read()?,
            },
            5 => Self::Message(r.read()?),
            6 => Self::ChangeState(r.read()?),
            7 => Self::ChangeHost(r.read()?),
            8 => Self::CreateRoom(r.read()?),
            9 => Self::JoinRoom(r.read()?),
            10 => Self::OnJoinRoom(r.read()?),
            11 => Self::LeaveRoom(r.read()?),
            12 => Self::LockRoom(r.read()?),
            13 => Self::CycleRoom(r.read()?),
            14 => Self::SelectChart(r.read()?),
            15 => Self::RequestStart(r.read()?),
            16 => Self::Ready(r.read()?),
            17 => Self::CancelReady(r.read()?),
            18 => Self::Played(r.read()?),
            19 => Self::Abort(r.read()?),
            x => return Err(DecodeError::InvalidTag(x)),
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        match self {
            Self::Pong => w.write_val(0u8)?,
            Self::Authenticate(v) => {
                w.write_val(1u8)?;
                w.write(v)?;
            }
            Self::Chat(v) => {
                w.write_val(2u8)?;
                w.write(v)?;
            }
            Self::Touches { player, frames } => {
                w.write_val(3u8)?;
                w.write(player)?;
                w.write(frames)?;
            }
            Self::Judges { player, judges } => {
                w.write_val(4u8)?;
                w.write(player)?;
                w.write(judges)?;
            }
            Self::Message(v) => {
                w.write_val(5u8)?;
                w.write(v)?;
            }
            Self::ChangeState(v) => {
                w.write_val(6u8)?;
                w.write(v)?;
            }
            Self::ChangeHost(v) => {
                w.write_val(7u8)?;
                w.write(v)?;
            }
            Self::CreateRoom(v) => {
                w.write_val(8u8)?;
                w.write(v)?;
            }
            Self::JoinRoom(v) => {
                w.write_val(9u8)?;
                w.write(v)?;
            }
            Self::OnJoinRoom(v) => {
                w.write_val(10u8)?;
                w.write(v)?;
            }
            Self::LeaveRoom(v) => {
                w.write_val(11u8)?;
                w.write(v)?;
            }
            Self::LockRoom(v) => {
                w.write_val(12u8)?;
                w.write(v)?;
            }
            Self::CycleRoom(v) => {
                w.write_val(13u8)?;
                w.write(v)?;
            }
            Self::SelectChart(v) => {
                w.write_val(14u8)?;
                w.write(v)?;
            }
            Self::RequestStart(v) => {
                w.write_val(15u8)?;
                w.write(v)?;
            }
            Self::Ready(v) => {
                w.write_val(16u8)?;
                w.write(v)?;
            }
            Self::CancelReady(v) => {
                w.write_val(17u8)?;
                w.write(v)?;
            }
            Self::Played(v) => {
                w.write_val(18u8)?;
                w.write(v)?;
            }
            Self::Abort(v) => {
                w.write_val(19u8)?;
                w.write(v)?;
            }
        }
        Ok(())
    }
}

impl BinaryData for Message {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(match r.read::<u8>()? {
            0 => Self::Chat {
                user: r.read()?,
                content: r.read()?,
            },
            1 => Self::CreateRoom { user: r.read()? },
            2 => Self::JoinRoom {
                user: r.read()?,
                name: r.read()?,
            },
            3 => Self::LeaveRoom {
                user: r.read()?,
                name: r.read()?,
            },
            4 => Self::NewHost { user: r.read()? },
            5 => Self::SelectChart {
                user: r.read()?,
                name: r.read()?,
                id: r.read()?,
            },
            6 => Self::GameStart { user: r.read()? },
            7 => Self::Ready { user: r.read()? },
            8 => Self::CancelReady { user: r.read()? },
            9 => Self::CancelGame { user: r.read()? },
            10 => Self::StartPlaying,
            11 => Self::Played {
                user: r.read()?,
                score: r.read()?,
                accuracy: r.read()?,
                full_combo: r.read()?,
            },
            12 => Self::GameEnd,
            13 => Self::Abort { user: r.read()? },
            14 => Self::LockRoom { lock: r.read()? },
            15 => Self::CycleRoom { cycle: r.read()? },
            x => return Err(DecodeError::InvalidTag(x)),
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        match self {
            Self::Chat { user, content } => {
                w.write_val(0u8)?;
                w.write(user)?;
                w.write(content)?;
            }
            Self::CreateRoom { user } => {
                w.write_val(1u8)?;
                w.write(user)?;
            }
            Self::JoinRoom { user, name } => {
                w.write_val(2u8)?;
                w.write(user)?;
                w.write(name)?;
            }
            Self::LeaveRoom { user, name } => {
                w.write_val(3u8)?;
                w.write(user)?;
                w.write(name)?;
            }
            Self::NewHost { user } => {
                w.write_val(4u8)?;
                w.write(user)?;
            }
            Self::SelectChart { user, name, id } => {
                w.write_val(5u8)?;
                w.write(user)?;
                w.write(name)?;
                w.write(id)?;
            }
            Self::GameStart { user } => {
                w.write_val(6u8)?;
                w.write(user)?;
            }
            Self::Ready { user } => {
                w.write_val(7u8)?;
                w.write(user)?;
            }
            Self::CancelReady { user } => {
                w.write_val(8u8)?;
                w.write(user)?;
            }
            Self::CancelGame { user } => {
                w.write_val(9u8)?;
                w.write(user)?;
            }
            Self::StartPlaying => w.write_val(10u8)?,
            Self::Played {
                user,
                score,
                accuracy,
                full_combo,
            } => {
                w.write_val(11u8)?;
                w.write(user)?;
                w.write(score)?;
                w.write(accuracy)?;
                w.write(full_combo)?;
            }
            Self::GameEnd => w.write_val(12u8)?,
            Self::Abort { user } => {
                w.write_val(13u8)?;
                w.write(user)?;
            }
            Self::LockRoom { lock } => {
                w.write_val(14u8)?;
                w.write(lock)?;
            }
            Self::CycleRoom { cycle } => {
                w.write_val(15u8)?;
                w.write(cycle)?;
            }
        }
        Ok(())
    }
}
