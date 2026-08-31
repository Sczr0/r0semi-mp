// SPDX-License-Identifier: AGPL-3.0-only
//! 事件投递/观察者（C1 拆分第一步，2026-08-31）——从 server.rs 抽出的独立 sink 组件。
//!
//! 属于 C1 蓝图"`sink/`（session_sink/encode_cache/room_list_sink）"组的**独立部分**：
//! `RoomInfo`/`RoomListSink`（房间列表观察者）/`CompositeSink`（多个 EventSink 的扇出）。
//! 零依赖 server.rs 重组件（SessionSink/EncodeCache/Backpressure 等），纯结构搬移，
//! 不改变执行语义（`pub use` 由 server.rs 重导出维持对外 API 不变）。

use std::sync::Arc;

use phira_api::{RoomEvent, RoomId};
use phira_core::EventSink;

/// 公开房间列表快照事实（§运营 `/rooms` + 管理读面 `admin.rs room_json`）。
///
/// 2026-08 对齐公开房间列表标准格式（用户拍板：id 统一 int、monitor 不进名单）：
/// `roomid`/`lock`/`host{name,id}`/`state`(select_chart|playing|wait_for_ready)/
/// `chart{name,id}`/`players[{name,id}]`。本结构只存事实（id 集合 + 谱面 + 三态）；
/// 名字在渲染时经 `SessionRegistry` 解析（admin.rs `room_json`）——存活期成立：
/// `evict_name` 发生在 `UserLeft` 之后（lifecycle.rs，ISSUE-0012）。
#[derive(Debug, Clone)]
pub struct RoomInfo {
    /// 房间 id。
    pub id: String,
    /// 房主用户 id。
    pub host: i32,
    /// 房间状态（三态 snake_case：`select_chart` / `playing` / `wait_for_ready`）。
    pub state: &'static str,
    /// 是否锁定。
    pub locked: bool,
    /// 循环对局（admin 详情用，阶段 1：RoomListSink 维护 CycleRoom 事件）。
    pub cycle: bool,
    /// 玩家 id 名单（加入序；**不含 monitor**——观察者不是玩家，2026-08 拍板）。
    pub players: Vec<i32>,
    /// 当前谱面（`SelectChart` 事件记录 name+id；`GameEnd`/`CancelGame(None)` 清空）。
    pub chart: Option<(String, i32)>,
}

/// 房间列表观察者（§7.3 观察者模式）：订阅事件维护活动房间快照。
///
/// 纯观察者——不碰核心（bus/actor），数据源 = EventSink 事件流。
/// 隐私过滤：房间 id 匹配 `hidden_prefixes` 任一前缀 → 不进入公开列表。
pub struct RoomListSink {
    rooms: tokio::sync::RwLock<std::collections::HashMap<RoomId, RoomInfo>>,
    /// 私密房间 id 前缀（yml `hidden_room_prefixes`，如 `["solo"]`）。
    hidden_prefixes: Vec<String>,
}

impl RoomListSink {
    /// 构造。`hidden_prefixes` = 私密房间 id 前缀（命中则不公开展示）。
    #[must_use]
    pub fn new(hidden_prefixes: Vec<String>) -> Self {
        Self {
            rooms: tokio::sync::RwLock::new(std::collections::HashMap::new()),
            hidden_prefixes,
        }
    }

    fn hidden(&self, id: &RoomId) -> bool {
        self.hidden_prefixes
            .iter()
            .any(|p| id.as_str().starts_with(p))
    }

    /// 公开房间列表快照（已过滤私密房间）。
    pub async fn snapshot(&self) -> Vec<RoomInfo> {
        let mut list: Vec<_> = self.rooms.read().await.values().cloned().collect();
        list.sort_by(|a, b| a.id.cmp(&b.id));
        list
    }
}

#[async_trait::async_trait]
impl EventSink for RoomListSink {
    async fn deliver(&self, _user_id: i32, event: &RoomEvent) {
        use phira_api::RoomEvent as E;
        match event {
            E::RoomCreated { room_id, host } => {
                if !self.hidden(room_id) {
                    self.rooms.write().await.insert(
                        room_id.clone(),
                        RoomInfo {
                            id: room_id.as_str().to_owned(),
                            host: *host,
                            state: "select_chart",
                            locked: false,
                            cycle: false,
                            players: vec![*host],
                            chart: None,
                        },
                    );
                }
            }
            E::RoomClosed { room_id } => {
                self.rooms.write().await.remove(room_id);
            }
            E::UserJoined { room_id, user } => {
                // monitor 不进 players（观察者不是玩家，2026-08 拍板）
                if let Some(r) = self.rooms.write().await.get_mut(room_id)
                    && !user.monitor
                {
                    r.players.push(user.id);
                }
            }
            E::UserLeft { room_id, user, .. } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.players.retain(|p| p != user);
                }
            }
            E::NewHost {
                room_id, new_host, ..
            } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.host = *new_host;
                }
            }
            E::SelectChart {
                room_id, name, id, ..
            } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.state = "select_chart";
                    r.chart = Some((name.clone(), *id));
                }
            }
            E::GameStart { room_id, .. } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.state = "wait_for_ready";
                }
            }
            E::StartPlaying { room_id } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.state = "playing";
                }
            }
            E::GameEnd { room_id, chart } | E::CancelGame { room_id, chart, .. } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.state = "select_chart";
                    // 谱面保留（Some(id) 与存量一致）；None = 图已被取消 → 清空
                    if chart.is_none() {
                        r.chart = None;
                    }
                }
            }
            E::LockRoom { room_id, lock } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.locked = *lock;
                }
            }
            E::CycleRoom { room_id, cycle } => {
                if let Some(r) = self.rooms.write().await.get_mut(room_id) {
                    r.cycle = *cycle;
                }
            }
            // 热路径（RelayTouches/Judges）与不改变列表展示的（Chat/Ready/Played/Abort）
            // 不更新快照
            _ => {}
        }
    }
}

/// 组合投递目标：多个 EventSink 的扇出（§4.9-5 观察者组合，bus 零改动）。
#[derive(Default)]
pub struct CompositeSink {
    sinks: tokio::sync::RwLock<Vec<Arc<dyn EventSink>>>,
}

impl CompositeSink {
    /// 构造时注入观察者列表（同步，避免 async 构造在非 async 上下文不可用）。
    #[must_use]
    pub fn new(sinks: Vec<Arc<dyn EventSink>>) -> Self {
        Self {
            sinks: tokio::sync::RwLock::new(sinks),
        }
    }
}

#[async_trait::async_trait]
impl EventSink for CompositeSink {
    async fn deliver(&self, user_id: i32, event: &RoomEvent) {
        let sinks = self.sinks.read().await.clone();
        for sink in sinks {
            sink.deliver(user_id, event).await;
        }
    }
}
