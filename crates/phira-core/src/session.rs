//! 会话层：连接生命周期事实的类型定义。
//!
//! 会话层**时序逻辑**已落地于 `lifecycle`（§4.9-3：单一生产者任务 + 注册表 +
//! 窗口边界），本模块保留**事实类型**的权威定义（`LifecycleEvent` 的语义原型）：
//! - `SessionHandle`：会话句柄（bus 投递目标注册用，§6.6 表 2）
//! - `LifecycleFact`：连接事实（历史类型；`lifecycle::LifecycleEvent` 为现行实现，
//!   两者变体一一对应——本类型保留文档价值与向后引用，勿增变体）

/// 会话句柄（bus 投递目标注册，§6.6 表 2）。
#[derive(Debug)]
pub struct SessionHandle {
    /// 用户 id（鉴权后确定，§6.5-19）。
    pub user_id: i32,
}

impl SessionHandle {
    /// 会话建立（鉴权通过后调用）。
    #[must_use]
    pub fn new(user_id: i32) -> Self {
        Self { user_id }
    }
}

/// 连接事实（§4.9-3）：用户生命周期任务的输入。
///
/// **现行实现**：`crate::lifecycle::LifecycleEvent`（单一生产者消费循环、epoch 校验、
/// 窗口边界全部落地，2026-08）。本类型为初始桩的历史形态，语义与其一致：
/// - **窗口边界**：先查权威会话状态（重连通知的入队序 ≠ 墙钟序，§4.9-3）
/// - **会话纪元**：替换会话时 epoch+1 且关闭旧 TCP、取消旧会话任务（§4.9-3）
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
