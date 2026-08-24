//! 会话层（§5.5-3 桩）：连接生命周期、心跳、协议帧。
//!
//! 协议层（阶段 1：编解码、帧、心跳，§14 阶段 1）落地前，本模块只提供类型骨架。
//! 用户生命周期任务（§4.9-3：单一生产者、窗口边界、会话纪元）的时序逻辑
//! 在会话层就绪后实现——测试位置 = phira-core 集成测试 + 脚本化假 actor（§4.9-3）。

/// 会话句柄（阶段 1 填：stream 写通道、心跳计时；阶段 2 注册为 bus 投递目标，§6.6 表 2）。
#[derive(Debug)]
pub struct SessionHandle {
    /// 用户 id（鉴权后确定，§6.5-19）。
    pub user_id: i32,
}

impl SessionHandle {
    /// 会话建立（鉴权通过后调用）。
    pub fn new(user_id: i32) -> Self {
        Self { user_id }
    }
}

/// 连接事实（§4.9-3）：用户生命周期任务的输入。
///
/// 生产者为会话层（断线检测 / 鉴权重连）；派发
/// `UserDisconnected`/`UserReconnected`/`UserDangleExpired` 前必须：
/// - **窗口边界**：先查权威会话状态（重连通知的入队序 ≠ 墙钟序，§4.9-3）
/// - **会话纪元**：替换会话时 epoch+1 且关闭旧 TCP、取消旧会话任务（§4.9-3）
///
/// TODO(阶段 1): 与 session 任务接线后实现单一生产者消费循环。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleFact {
    /// 连接建立（鉴权通过）。
    Connected {
        /// 用户 id。
        user_id: i32,
        /// 会话纪元。
        epoch: u64,
    },
    /// 断线（心跳 10s 无包，§6.1）。
    Disconnected {
        /// 用户 id。
        user_id: i32,
        /// 会话纪元。
        epoch: u64,
    },
    /// 重连（同 id 再次鉴权，替换会话，§6.5-19）。
    Reconnected {
        /// 用户 id。
        user_id: i32,
        /// 会话纪元。
        epoch: u64,
    },
}
