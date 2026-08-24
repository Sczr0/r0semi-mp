//! 协议 ↔ 内部契约转换层（§6.6）。
//!
//! 纯函数，无 IO 无状态。**不是纯机械映射**（评审 §8 二-2）：
//! - `UserLeft` 广播带 name（事件已携带，§6.6 表 2）
//! - `CancelGame`/`GameEnd` 回 SelectChart 且**谱面保留**（事件携带 chart，原版语义）
//! - `NewHost` 拆出双向 `ChangeHost`（true→新 host，false→旧 host）
//! - `SelectChart`/`GameStart`/`StartPlaying` 附带 `ChangeState`
//!
//! 每次协议变更须联动：api（命令/事件变体）+ 本转换层 + 契约测试三处（§14 阶段 2）。
//!
//! 红线程：零 tokio、零运行时（§4.3-1）；phira-core 禁 unwrap/expect（柜台不 panic）。

use phira_api::{
    ClientCommand, Message, RoomCommand, RoomError, RoomEvent, RoomResponse, RoomState,
    ServerCommand, Targets,
};

/// 表 1：客户端命令 → 房间命令（§6.6 表 1）。
///
/// `name` = 发送者昵称（core 从身份注册表填，§4.9-3）；`CreateRoom`/`JoinRoom` 需要。
///
/// 返回 `None` = 归 core 处理、不派发房间：
/// - `Ping`：心跳应答（core 直接回 Pong）
/// - `Authenticate`：鉴权编排（core，§4.9-3）
///
/// 载荷携带 `room_id` 的命令（`CreateRoom`/`JoinRoom`）在此直通；
/// 其余命令的 `room_id` 由调用方（bus 路由，§4.9-4）填进 `CmdCtx`。
#[must_use]
pub fn client_to_room(cmd: ClientCommand, name: String) -> Option<RoomCommand> {
    Some(match cmd {
        ClientCommand::Ping | ClientCommand::Authenticate { .. } => return None,
        ClientCommand::Chat { message } => RoomCommand::Chat { message },
        ClientCommand::Touches { frames } => RoomCommand::Touches { frames },
        ClientCommand::Judges { judges } => RoomCommand::Judges { judges },
        ClientCommand::CreateRoom { id } => RoomCommand::CreateRoom {
            id,
            name: name.clone(),
        },
        ClientCommand::JoinRoom { id, monitor } => RoomCommand::JoinRoom { id, monitor, name },
        ClientCommand::LeaveRoom => RoomCommand::LeaveRoom,
        ClientCommand::LockRoom { lock } => RoomCommand::LockRoom { lock },
        ClientCommand::CycleRoom { cycle } => RoomCommand::CycleRoom { cycle },
        ClientCommand::SelectChart { id } => RoomCommand::SelectChart { id },
        ClientCommand::RequestStart => RoomCommand::RequestStart,
        ClientCommand::Ready => RoomCommand::Ready,
        ClientCommand::CancelReady => RoomCommand::CancelReady,
        ClientCommand::Played { id } => RoomCommand::Played { id },
        ClientCommand::Abort => RoomCommand::Abort,
    })
}

/// 错误文案（§4.4）：Business 透传文案，Internal 返回通用文案 + 日志（调用方记）。
#[must_use]
pub fn error_message(err: &RoomError) -> String {
    match err {
        RoomError::Business { msg, .. } => msg.clone(),
        RoomError::Internal { msg } => {
            // 内部故障不暴露细节（§4.4）；调用方（session）负责日志
            tracing::warn!("internal room error: {msg}");
            "internal error".to_owned()
        }
    }
}

/// 响应映射：`(ClientCommand, RoomResponse)` → 协议 `Result` 变体（§6.6 表 1 逆）。
///
/// 每命令的 Ok 载荷形态不同（`JoinRoom` 带房间快照，其余 `()`）；
/// `Failure` 按 [`error_message`] 生成 `Err(String)`。
///
/// **注意**：bus 的业务拒绝是 `Ok(RoomResponse::Failure)`（不是 `Err`，§4.4）——
/// 此处必须先归一化为 `Err(String)`，否则失败路径被误当成功（2026-08 修复，
/// e2e 游戏流程抓出）。
#[must_use]
pub fn response_to_server(
    cmd: &ClientCommand,
    resp: Result<RoomResponse, RoomError>,
) -> ServerCommand {
    // 归一化：成功载荷 或 Err 文案（业务 Failure 与内部 Err 殊途同归）
    let result: Result<RoomResponse, String> = match resp {
        // 业务 Failure 与内部 Err 都转 Err 文案（错误分类只影响日志，§4.4）
        Ok(RoomResponse::Failure(err)) | Err(err) => Err(error_message(&err)),
        Ok(other) => Ok(other),
    };
    match cmd {
        ClientCommand::Chat { .. } => ServerCommand::Chat(result.map(|_| ())),
        ClientCommand::CreateRoom { .. } => ServerCommand::CreateRoom(result.map(|_| ())),
        ClientCommand::JoinRoom { .. } => ServerCommand::JoinRoom(result.map(|r| match r {
            RoomResponse::JoinRoom(jr) => jr,
            _ => unreachable!("JoinRoom 命令的响应恒为 JoinRoom 变体（bus 契约）"),
        })),
        ClientCommand::LeaveRoom => ServerCommand::LeaveRoom(result.map(|_| ())),
        ClientCommand::LockRoom { .. } => ServerCommand::LockRoom(result.map(|_| ())),
        ClientCommand::CycleRoom { .. } => ServerCommand::CycleRoom(result.map(|_| ())),
        ClientCommand::SelectChart { .. } => ServerCommand::SelectChart(result.map(|_| ())),
        ClientCommand::RequestStart => ServerCommand::RequestStart(result.map(|_| ())),
        ClientCommand::Ready => ServerCommand::Ready(result.map(|_| ())),
        ClientCommand::CancelReady => ServerCommand::CancelReady(result.map(|_| ())),
        ClientCommand::Played { .. } => ServerCommand::Played(result.map(|_| ())),
        ClientCommand::Abort => ServerCommand::Abort(result.map(|_| ())),
        // 热路径无响应（§6.5-17：只转发给 monitor，不回答发者）；心跳/鉴权不走房间派发
        ClientCommand::Touches { .. } | ClientCommand::Judges { .. } => {
            unreachable!("Touches/Judges 无响应（热路径只转发）")
        }
        // 心跳/鉴权不走房间派发（core 处理），此处不可达
        ClientCommand::Ping | ClientCommand::Authenticate { .. } => {
            unreachable!("Ping/Authenticate 不派发房间（client_to_room 返回 None）")
        }
    }
}

/// 表 2：房间事件 → （投递目标, 服务端命令）列表（§6.6 表 2）。
///
/// 一个事件可产出多条命令（`NewHost` 的双向 `ChangeHost`、`ChangeState` 附加等）。
/// `RoomClosed` 无协议输出（core 内部信号，§4.9-9）。
#[must_use]
#[allow(clippy::too_many_lines)] // 表 2 全事件完整呈现优于拆碎（§6.6 是单一权威表）
pub fn event_to_server(event: RoomEvent) -> Vec<(Targets, ServerCommand)> {
    let mut out = Vec::new();
    match event {
        RoomEvent::Chat { user, content, .. } => out.push((
            Targets::All,
            ServerCommand::Message(Message::Chat { user, content }),
        )),
        RoomEvent::RoomCreated { host, .. } => out.push((
            Targets::All,
            ServerCommand::Message(Message::CreateRoom { user: host }),
        )),
        RoomEvent::UserJoined { user, .. } => {
            // 双广播：OnJoinRoom（成员列表增量）+ Message(JoinRoom)（房内广播）
            out.push((Targets::All, ServerCommand::OnJoinRoom(user.clone())));
            out.push((
                Targets::All,
                ServerCommand::Message(Message::JoinRoom {
                    user: user.id,
                    name: user.name,
                }),
            ));
        }
        RoomEvent::UserLeft { user, name, .. } => out.push((
            Targets::All,
            ServerCommand::Message(Message::LeaveRoom { user, name }),
        )),
        RoomEvent::NewHost {
            new_host, old_host, ..
        } => {
            out.push((
                Targets::All,
                ServerCommand::Message(Message::NewHost { user: new_host }),
            ));
            // ChangeHost 是单播（原版 try_send 语义）：true→新 host，false→旧 host
            out.push((
                Targets::Specific(vec![new_host]),
                ServerCommand::ChangeHost(true),
            ));
            out.push((
                Targets::Specific(vec![old_host]),
                ServerCommand::ChangeHost(false),
            ));
        }
        RoomEvent::SelectChart { user, name, id, .. } => {
            out.push((
                Targets::All,
                ServerCommand::Message(Message::SelectChart { user, name, id }),
            ));
            out.push((
                Targets::All,
                ServerCommand::ChangeState(RoomState::SelectChart(Some(id))),
            ));
        }
        RoomEvent::GameStart { user, .. } => {
            out.push((
                Targets::All,
                ServerCommand::Message(Message::GameStart { user }),
            ));
            out.push((
                Targets::All,
                ServerCommand::ChangeState(RoomState::WaitingForReady),
            ));
        }
        RoomEvent::Ready { user, .. } => out.push((
            Targets::All,
            ServerCommand::Message(Message::Ready { user }),
        )),
        RoomEvent::CancelReady { user, .. } => out.push((
            Targets::All,
            ServerCommand::Message(Message::CancelReady { user }),
        )),
        RoomEvent::CancelGame { user, chart, .. } => {
            out.push((
                Targets::All,
                ServerCommand::Message(Message::CancelGame { user }),
            ));
            out.push((
                Targets::All,
                ServerCommand::ChangeState(RoomState::SelectChart(chart)),
            ));
        }
        RoomEvent::StartPlaying { .. } => {
            out.push((Targets::All, ServerCommand::Message(Message::StartPlaying)));
            out.push((Targets::All, ServerCommand::ChangeState(RoomState::Playing)));
        }
        RoomEvent::Played {
            user,
            score,
            accuracy,
            full_combo,
            ..
        } => out.push((
            Targets::All,
            ServerCommand::Message(Message::Played {
                user,
                score,
                accuracy,
                full_combo,
            }),
        )),
        RoomEvent::GameEnd { chart, .. } => {
            out.push((Targets::All, ServerCommand::Message(Message::GameEnd)));
            // 谱面保留（原版：结算后 self.chart 未清，§6.6 表 2 注）
            out.push((
                Targets::All,
                ServerCommand::ChangeState(RoomState::SelectChart(chart)),
            ));
        }
        RoomEvent::Abort { user, .. } => out.push((
            Targets::All,
            ServerCommand::Message(Message::Abort { user }),
        )),
        RoomEvent::LockRoom { lock, .. } => out.push((
            Targets::All,
            ServerCommand::Message(Message::LockRoom { lock }),
        )),
        RoomEvent::CycleRoom { cycle, .. } => out.push((
            Targets::All,
            ServerCommand::Message(Message::CycleRoom { cycle }),
        )),
        RoomEvent::RelayTouches {
            targets,
            player,
            frames,
            ..
        } => out.push((targets, ServerCommand::Touches { player, frames })),
        RoomEvent::RelayJudges {
            targets,
            player,
            judges,
            ..
        } => out.push((targets, ServerCommand::Judges { player, judges })),
        // RoomClosed（core 内部信号，无协议输出）与契约 non_exhaustive 兜底（§5.6）均无输出
        RoomEvent::RoomClosed { .. } | _ => {}
    }
    out
}
