//! 房间契约：内部契约层（RoomCommand / RoomEvent / CmdCtx / 薄缝 trait）。
//!
//! 依据：ARCHITECTURE.md §4.4（薄缝完整形态）、§4.9（并发模型）、§6.5（规则清单）。
//!
//! 本文件是**改写产物**（协议中不存在的 Event 概念、系统命令、targets），不是协议直接投影
//! （§2.3 原则 1）——按设计对待：纳入评审、可演进、有版本（§5.6）。
//!
//! 红线程（§4.3-1 / §4.8）：零 tokio、零运行时，只依赖 std + thiserror + half + async-trait。

use std::sync::Arc;

use half::f16;

/// 单调毫秒时钟（§4.9-6）。
///
/// impl 唯一时钟源是 `RoomCommand::Tick`；测试可任意构造伪造（§6.5-25），
/// 10s 重连窗口可精确推进而无需真实等待。
pub type TimeMs = u64;

/// 长度受限字符串（协议 §6.2）。
///
/// 长度以**字节**计：token ≤32、聊天 ≤200、RoomId ≤20。
/// 构造时校验超限即拒绝。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Varchar<const N: usize>(pub(crate) String);

impl<const N: usize> Varchar<N> {
    /// 构造并校验长度（字节数 ≤ N）。
    pub fn new(value: String) -> Result<Self, String> {
        if value.len() > N {
            return Err(format!("string too long: {} > {N}", value.len()));
        }
        Ok(Self(value))
    }

    /// 借用内部字符串。
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 解包为内部字符串。
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl<const N: usize> std::fmt::Display for Varchar<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// 房间 id（协议 §6.2）：`Varchar<20>` + 合法字符约束 `[A-Za-z0-9_-]` 且非空。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoomId(pub(crate) Varchar<20>);

impl RoomId {
    /// 构造并校验：非空 + 全部字符属于 `[A-Za-z0-9_-]`。
    pub fn new(id: String) -> Result<Self, String> {
        let v = Varchar::new(id)?;
        if v.as_str().is_empty()
            || !v
                .as_str()
                .chars()
                .all(|it| it == '-' || it == '_' || it.is_ascii_alphanumeric())
        {
            return Err("invalid room id".to_owned());
        }
        Ok(Self(v))
    }

    /// 借用内部字符串。
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Display for RoomId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// 半精度坐标（协议 §6.2 / §4.8-1）：f16 × 2，不是 f32，写错即不兼容。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactPos {
    pub(crate) x: f16,
    pub(crate) y: f16,
}

impl CompactPos {
    /// 从 f32 构造（内部转 f16 半精度）。
    pub fn new(x: f32, y: f32) -> Self {
        Self {
            x: f16::from_f32(x),
            y: f16::from_f32(y),
        }
    }

    /// x 坐标（f16 转回 f32）。
    pub fn x(&self) -> f32 {
        self.x.to_f32()
    }

    /// y 坐标（f16 转回 f32）。
    pub fn y(&self) -> f32 {
        self.y.to_f32()
    }
}

/// 单帧触摸数据（协议 §6.3）：时间 + 触点列表（finger id, 坐标）。
#[derive(Debug, Clone, PartialEq)]
pub struct TouchFrame {
    /// 帧内时间（秒，f32）。
    pub time: f32,
    /// 触点：(手指 id, 半精度坐标)。
    pub points: Vec<(i8, CompactPos)>,
}

/// 判定类型（协议 §6.3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Judgement {
    /// Perfect。
    Perfect,
    /// Good。
    Good,
    /// Bad。
    Bad,
    /// Miss。
    Miss,
    /// HoldPerfect。
    HoldPerfect,
    /// HoldGood。
    HoldGood,
}

/// 单条判定事件（协议 §6.3）。
#[derive(Debug, Clone, PartialEq)]
pub struct JudgeEvent {
    /// 事件时间（秒，f32）。
    pub time: f32,
    /// 判定线 id。
    pub line_id: u32,
    /// 音符 id。
    pub note_id: u32,
    /// 判定类型。
    pub judgement: Judgement,
}

/// 房间状态（协议 §6.3，客户端视角）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomState {
    /// 选图阶段，携带已选谱面 id（None = 未选）。
    SelectChart(Option<i32>),
    /// 等待全员准备。
    WaitingForReady,
    /// 游玩中。
    Playing,
}

/// 用户信息（协议 §6.3）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserInfo {
    /// 用户 id（token 解析所得，§6.5-19）。
    pub id: i32,
    /// 昵称。
    pub name: String,
    /// 是否为观战者（monitor）。
    pub monitor: bool,
}

/// 客户端房间状态快照（协议 §6.3，重连恢复用，§6.5-23）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientRoomState {
    /// 房间 id。
    pub id: RoomId,
    /// 客户端视角状态。
    pub state: RoomState,
    /// 是否 live（有 monitor 观战）。
    pub live: bool,
    /// 是否锁房。
    pub locked: bool,
    /// 是否循环房。
    pub cycle: bool,
    /// 当前用户是否房主。
    pub is_host: bool,
    /// 当前用户是否已 ready。
    pub is_ready: bool,
    /// 房内用户表（玩家 + monitor）。
    pub users: std::collections::HashMap<i32, UserInfo>,
}

/// 加入房间的响应载荷（协议 §6.3）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinRoomResponse {
    /// 房间当前状态。
    pub state: RoomState,
    /// 房内用户列表（玩家 + monitor）。
    pub users: Vec<UserInfo>,
    /// 是否 live。
    pub live: bool,
}

/// 谱面元数据（回源 `GET {API}/chart/{id}`，§6.5-15）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chart {
    /// 谱面 id。
    pub id: i32,
    /// 谱面名。
    pub name: String,
}

/// 成绩记录（回源 `GET {API}/record/{id}`，§6.5-10/15）。
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// 记录 id。
    pub id: i32,
    /// 玩家 id（须与上报者一致，§6.5-10）。
    pub player: i32,
    /// 分数。
    pub score: i32,
    /// Perfect 数。
    pub perfect: i32,
    /// Good 数。
    pub good: i32,
    /// Bad 数。
    pub bad: i32,
    /// Miss 数。
    pub miss: i32,
    /// 最大连击。
    pub max_combo: i32,
    /// 准确率。
    pub accuracy: f32,
    /// 是否全连。
    pub full_combo: bool,
    /// 标准差。
    pub std: f32,
    /// 标准差分数。
    pub std_score: f32,
}

/// 房间配置（§4.4 `UpdateConfig` / §6.5-4 monitor 白名单）。
///
/// 配置是热重载的，不是构造期快照（§4.9-8）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoomConfig {
    /// monitor（观战者）白名单：用户 id 在此名单才可加入 monitor（§6.5-4）。
    pub monitors: Vec<i32>,
}

/// 命令来源（§4.4）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// 来自客户端连接，携带用户 id。
    Client {
        /// 用户 id。
        user_id: i32,
    },
    /// 来自 core（生命周期任务 / 定时器 / 配置热更）。
    System,
}

/// 命令上下文（§4.4）。
///
/// `room_id` 是路由目标，由 core 盖章（§4.9-4）：`CreateRoom`/`JoinRoom` 靠载荷里的 id 路由
/// （用户还不在目标房间，路由表查不到）；其余客户端命令靠路由表；系统命令直接按 room_id。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CmdCtx {
    /// 命令来源。
    pub origin: Origin,
    /// 路由目标房间。
    pub room_id: RoomId,
}

/// 业务拒绝码（`RoomError::Business` 的判别，§4.4）。
///
/// 业务拒绝（房满/越权）是预期行为，错误率统计只算 `Internal`（§3.2）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RoomErrorCode {
    /// 已在房间中（重复 CreateRoom/JoinRoom，§6.5-27）。
    AlreadyInRoom,
    /// 房间 id 已被占用（CreateRoom）。
    RoomIdOccupied,
    /// 房间不存在（JoinRoom）。
    RoomNotFound,
    /// 房间已锁（JoinRoom，§6.5-3）。
    RoomLocked,
    /// 游戏进行中不可加入（仅 SelectChart 可加入，§6.5-3）。
    GameOngoing,
    /// 无 monitor 权限（§6.5-4）。
    CannotMonitor,
    /// 房间已满（8 人上限，§6.5-1）。
    RoomFull,
    /// 仅房主可执行（锁房/循环/选图/请求开始，§6.5-2）。
    OnlyHost,
    /// 不在房间中（命令路由表 miss，§4.9-4）。
    NotInRoom,
    /// 房间状态不允许该命令（状态机违规，§6.4）。
    InvalidState,
    /// 未选谱面即请求开始（§6.5-7）。
    NoChartSelected,
    /// 已 ready 重复上报（§6.5）。
    AlreadyReady,
    /// 未 ready 却取消（§6.5）。
    NotReady,
    /// 成绩记录无效（player 不匹配 / 回源失败语义，§6.5-10）。
    InvalidRecord,
    /// 重复上报成绩（§6.5-10）。
    AlreadyUploaded,
    /// 已 abort 重复上报（§6.5）。
    AlreadyAborted,
    /// 命令频率超限（每连接限速，ISSUE-0006：滥用控制"快端"防线）。
    TooManyRequests,
}

/// 内部错误：Business（业务拒绝）与 Internal（内部故障）分开（§4.4）。
///
/// 错误率/灰度只统计 `Internal`——业务拒绝混入会扭曲对比（§3.2，评审 §8）。
/// 协议层 `Err(String)` 由 core 生成：Business 透传文案，Internal 返回通用文案 + 日志。
#[derive(Debug, Clone, thiserror::Error)]
pub enum RoomError {
    /// 业务拒绝：预期行为，客户端可见。
    #[error("{msg}")]
    Business {
        /// 业务拒绝码。
        code: RoomErrorCode,
        /// 客户端可见文案。
        msg: String,
    },
    /// 内部故障：不暴露细节，记日志。
    #[error("internal error: {msg}")]
    Internal {
        /// 内部错误描述（仅日志）。
        msg: String,
    },
}

/// 命令响应（§4.4）。
///
/// 每命令一次 handle、一次响应；channel FIFO + 分发配对保证按序对应（评审 §8 一）——
/// `Failure` 无需携带命令判别。
#[derive(Debug, Clone)]
pub enum RoomResponse {
    /// 成功（协议 Result 的 Ok 变体由 core 按命令映射）。
    Ok,
    /// 失败（Business 透传 / Internal 通用文案）。
    Failure(RoomError),
    /// JoinRoom 的响应载荷。
    JoinRoom(JoinRoomResponse),
    /// GetClientState 的响应（重连恢复用，§6.5-23）。
    ClientState(Option<ClientRoomState>),
}

/// 事件投递目标（§4.4 / §4.9-5）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Targets {
    /// 房内全部（成员 + 观察者；由 core 路由表反解）。
    All,
    /// 指定用户（impl 计算，如 monitor 列表）。
    Specific(Vec<i32>),
}

/// 房间事件（§4.4，评审 §8 二/四 修订后形态）。
///
/// 分类学：
/// - **领域事件**（与协议 Message 一一对应）：投递目标恒为房内 All，不再携带 targets
/// - **转发指令**（RelayTouches/RelayJudges）：仅 monitor——不进观察者通道，携带 targets
/// - **core 信号**（RoomClosed）：core 拆房间 + **通知观察者**（RoomListSink 等依赖它清理快照；
///   投递经 bus 步骤 4，`user_id=0` 系统约定，不发给任何用户会话）
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum RoomEvent {
    /// 聊天消息（Message::Chat）。
    Chat {
        /// 房间 id。
        room_id: RoomId,
        /// 发言用户 id。
        user: i32,
        /// 内容。
        content: String,
    },
    /// 房间创建（Message::CreateRoom + 路由增量 host→room）。
    RoomCreated {
        /// 房间 id。
        room_id: RoomId,
        /// 房主用户 id。
        host: i32,
    },
    /// 用户加入（Message::JoinRoom + 路由增量）。
    UserJoined {
        /// 房间 id。
        room_id: RoomId,
        /// 加入者信息。
        user: UserInfo,
    },
    /// 用户离开（Message::LeaveRoom + 路由增量；含驱逐：无独立协议对应物）。
    UserLeft {
        /// 房间 id。
        room_id: RoomId,
        /// 离开者用户 id。
        user: i32,
        /// 离开者昵称（转换层生成 `Message::LeaveRoom` 需要，§6.6 表 2；impl 持有）。
        name: String,
    },
    /// 房主变更（Message::NewHost + ChangeHost 双向，表 2）。
    NewHost {
        /// 房间 id。
        room_id: RoomId,
        /// 新房主 id。
        new_host: i32,
        /// 旧房主 id。
        old_host: i32,
    },
    /// 选图（Message::SelectChart）。
    SelectChart {
        /// 房间 id。
        room_id: RoomId,
        /// 选图用户 id。
        user: i32,
        /// 谱面名。
        name: String,
        /// 谱面 id。
        id: i32,
    },
    /// 请求开始（Message::GameStart）。
    GameStart {
        /// 房间 id。
        room_id: RoomId,
        /// 请求者（房主）id。
        user: i32,
    },
    /// 准备（Message::Ready）。
    Ready {
        /// 房间 id。
        room_id: RoomId,
        /// 准备用户 id。
        user: i32,
    },
    /// 取消准备（非房主，Message::CancelReady）。
    CancelReady {
        /// 房间 id。
        room_id: RoomId,
        /// 取消用户 id。
        user: i32,
    },
    /// 房主取消开局（Message::CancelGame）。
    CancelGame {
        /// 房间 id。
        room_id: RoomId,
        /// 房主 id。
        user: i32,
        /// 已选谱面（保留，转换层生成 `ChangeState(SelectChart)`，§6.6 表 2）。
        chart: Option<i32>,
    },
    /// 开局（Message::StartPlaying，全员 ready）。
    StartPlaying {
        /// 房间 id。
        room_id: RoomId,
    },
    /// 上报成绩（Message::Played）。
    Played {
        /// 房间 id。
        room_id: RoomId,
        /// 玩家 id。
        user: i32,
        /// 分数。
        score: i32,
        /// 准确率。
        accuracy: f32,
        /// 是否全连。
        full_combo: bool,
    },
    /// 结算（Message::GameEnd，全员完成/abort）。
    GameEnd {
        /// 房间 id。
        room_id: RoomId,
        /// 已选谱面（保留，转换层生成 `ChangeState(SelectChart)`，§6.6 表 2）。
        chart: Option<i32>,
    },
    /// 中止（Message::Abort）。
    Abort {
        /// 房间 id。
        room_id: RoomId,
        /// 中止用户 id。
        user: i32,
    },
    /// 锁房（Message::LockRoom）。
    LockRoom {
        /// 房间 id。
        room_id: RoomId,
        /// 是否锁定。
        lock: bool,
    },
    /// 循环房（Message::CycleRoom）。
    CycleRoom {
        /// 房间 id。
        room_id: RoomId,
        /// 是否循环。
        cycle: bool,
    },
    /// 触摸转发指令（结构化；core 编码一次、共享 Bytes，§6.5-17；仅此类携带 targets）。
    RelayTouches {
        /// 房间 id。
        room_id: RoomId,
        /// 投递目标（Specific(monitor_ids)）。
        targets: Targets,
        /// 玩家 id。
        player: i32,
        /// 触摸帧（Arc 共享，零拷贝）。
        frames: Arc<Vec<TouchFrame>>,
    },
    /// 判定转发指令（同 RelayTouches）。
    RelayJudges {
        /// 房间 id。
        room_id: RoomId,
        /// 投递目标。
        targets: Targets,
        /// 玩家 id。
        player: i32,
        /// 判定事件（Arc 共享）。
        judges: Arc<Vec<JudgeEvent>>,
    },
    /// 空房自毁信号（§4.9-9）：core 排空 channel、drop sender、拆任务。
    RoomClosed {
        /// 房间 id。
        room_id: RoomId,
    },
}

/// 房间命令（§4.4）。
///
/// 客户端命令与 §6.3 全量对齐（room_id 在 CmdCtx）；系统命令由柜台驱动（§4.6/§4.9）。
/// `#[non_exhaustive]`：追加变体不破坏下游（§5.6）。
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum RoomCommand {
    /// 建房（自带 room id；路由目标是新建房间，§4.9-4）。
    CreateRoom {
        /// 房间 id。
        id: RoomId,
        /// 房主昵称（core 从身份注册表填；impl 构造 `UserInfo` 需要，§6.6 表 2）。
        name: String,
    },
    /// 入房（自带 room id；路由目标是目标房间）。
    JoinRoom {
        /// 房间 id。
        id: RoomId,
        /// 是否以 monitor 身份加入。
        monitor: bool,
        /// 加入者昵称（core 从身份注册表填；impl 构造 `UserInfo` 需要，§6.6 表 2）。
        name: String,
    },
    /// 离开房间。
    LeaveRoom,
    /// 聊天。
    Chat {
        /// 消息（≤200 字节）。
        message: Varchar<200>,
    },
    /// 选图。
    SelectChart {
        /// 谱面 id。
        id: i32,
    },
    /// 请求开始（房主）。
    RequestStart,
    /// 准备。
    Ready,
    /// 取消准备。
    CancelReady,
    /// 中止（游玩中）。
    Abort,
    /// 上报成绩。
    Played {
        /// 成绩记录 id（回源校验）。
        id: i32,
    },
    /// 锁房。
    LockRoom {
        /// 是否锁定。
        lock: bool,
    },
    /// 循环房。
    CycleRoom {
        /// 是否循环。
        cycle: bool,
    },
    /// 触摸流（热路径入口，§6.5-17）。
    Touches {
        /// 触摸帧（Arc 共享）。
        frames: Arc<Vec<TouchFrame>>,
    },
    /// 判定流（热路径入口）。
    Judges {
        /// 判定事件（Arc 共享）。
        judges: Arc<Vec<JudgeEvent>>,
    },
    /// 心跳节拍（§4.9-9）：定时器按固定节拍派发，`now` 自带，可丢。
    Tick {
        /// 单调毫秒时间。
        now: TimeMs,
    },
    /// 断线事实（core 生命周期任务，单一生产者，§4.9-3）。
    UserDisconnected {
        /// 用户 id。
        user_id: i32,
        /// 会话纪元（替换会话时 epoch+1，§4.9-3）。
        epoch: u64,
    },
    /// 重连事实（窗口内重连保留座位，§6.5-21）。
    UserReconnected {
        /// 用户 id。
        user_id: i32,
        /// 会话纪元。
        epoch: u64,
    },
    /// 重连窗口到期（先查权威会话状态再派发，§4.9-3 窗口边界；不携带 epoch）。
    UserDangleExpired {
        /// 用户 id。
        user_id: i32,
    },
    /// 查询客户端房间状态（重连恢复用，§6.5-23）。
    GetClientState {
        /// 用户 id。
        user_id: i32,
    },
    /// 配置热重载（§4.9-8）。
    UpdateConfig {
        /// 新配置（Arc 共享）。
        config: Arc<RoomConfig>,
    },
}

/// 随机源（§4.9-6）：房主随机选择（§6.5-5），测试可注入 fake。
pub trait RandomSource: Send + Sync {
    /// 在 `[0, len)` 中均匀随机取一个下标；`len == 0` 返回 None。
    fn pick_index(&self, len: usize) -> Option<usize>;
}

/// 回源 HTTP 契约（§4.4，评审 §8 三）。
///
/// **每次请求必须自带超时（如 5-10s）**——无超时的挂起 = 房间永久冻结 +
/// 生命周期事实在 bus 侧无限等待 + 该房玩家被"丢弃断连"。
/// 超时/网络错归 `ApiError::Internal`。
#[async_trait::async_trait]
pub trait ApiClient: Send + Sync {
    /// 获取谱面元数据（超时 ≤10s）。
    async fn fetch_chart(&self, id: i32) -> Result<Chart, ApiError>;
    /// 获取成绩记录（超时 ≤10s）。
    async fn fetch_record(&self, id: i32) -> Result<Record, ApiError>;
}

/// 回源错误（归 `RoomError::Internal` / `AuthError::Internal`）。
#[derive(Debug, Clone, thiserror::Error)]
#[error("api error: {msg}")]
pub enum ApiError {
    /// 内部故障（网络/超时/HTTP 错误）。
    Internal {
        /// 错误描述（仅日志）。
        msg: String,
    },
}

/// 外部依赖（§4.9-6）：全部经构造器注入，契约测试可 mock。
pub struct RoomDeps {
    /// 回源 HTTP：chart/record（§6.5-15）。
    pub api: Arc<dyn ApiClient>,
    /// 随机源：房主随机选择（§6.5-5）。
    pub rng: Arc<dyn RandomSource>,
}

/// 房间工厂（§4.9）：组合根注入一次并持有 deps，`create` 不再收第二份（评审 §8）。
pub trait RoomFactory: Send + Sync {
    /// 创建新房间 actor（每房间一个实例，§4.9）。
    fn create(&self, room_id: RoomId) -> Box<dyn RoomActor>;
}

/// 房间 actor（§4.4 / §4.7）：每房间一个实例，命令串行进入，`&mut self` 独占状态无锁。
///
/// 对象安全（§4.7 规则）：api 中所有 async trait 一律以对象安全形式声明，
/// core 必然以 `Box<dyn RoomActor>` 持有。
#[async_trait::async_trait]
pub trait RoomActor: Send {
    /// 处理一条命令，返回（响应, 事件集）。
    ///
    /// 回话：多数系统命令无回话；`GetClientState` 例外（§4.4，评审 §8 二-5）。
    async fn handle(
        &mut self,
        ctx: CmdCtx,
        cmd: RoomCommand,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>);
}
