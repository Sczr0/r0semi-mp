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
    ClientCommand, Message, RoomCommand, RoomEvent, RoomState, ServerCommand, Targets,
};

/// 表 1：客户端命令 → 房间命令（§6.6 表 1）。
///
/// 返回 `None` = 归 core 处理、不派发房间：
/// - `Ping`：心跳应答（core 直接回 Pong）
/// - `Authenticate`：鉴权编排（core，§4.9-3）
///
/// 载荷携带 `room_id` 的命令（`CreateRoom`/`JoinRoom`）在此直通；
/// 其余命令的 `room_id` 由调用方（bus 路由，§4.9-4）填进 `CmdCtx`。
#[must_use]
pub fn client_to_room(cmd: ClientCommand) -> Option<RoomCommand> {
    Some(match cmd {
        ClientCommand::Ping | ClientCommand::Authenticate { .. } => return None,
        ClientCommand::Chat { message } => RoomCommand::Chat { message },
        ClientCommand::Touches { frames } => RoomCommand::Touches { frames },
        ClientCommand::Judges { judges } => RoomCommand::Judges { judges },
        ClientCommand::CreateRoom { id } => RoomCommand::CreateRoom { id },
        ClientCommand::JoinRoom { id, monitor } => RoomCommand::JoinRoom { id, monitor },
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

/// 表 2：房间事件 → （投递目标, 服务端命令）列表（§6.6 表 2）。
///
/// 一个事件可产出多条命令（`NewHost` 的双向 `ChangeHost`、`ChangeState` 附加等）。
/// `RoomClosed` 无协议输出（core 内部信号，§4.9-9）。
#[must_use]
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
        // RoomClosed：core 内部信号（拆任务、排空 channel），无协议输出
        RoomEvent::RoomClosed { .. } => {}
        // 契约 non_exhaustive 兜底（§5.6）：新事件默认无协议输出，转换层随 api 演进
        _ => {}
    }
    out
}
