//! 鉴权契约（§4.4）：token → 身份解析。
//!
//! 它是重连编排的枢纽（§6.5-19/23）：core 编排 = token → AuthHandler →
//! 用户注册表（core）→ 旧会话替换（epoch+1）→ GetClientState 恢复房间。

/// 鉴权业务拒绝码（`AuthError::Business` 的判别）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthErrorCode {
    /// token 无效（客户端可见）。
    InvalidToken,
}

/// 鉴权错误（§4.4）。
#[derive(Debug, Clone, thiserror::Error)]
pub enum AuthError {
    /// 业务拒绝：token 无效（客户端可见）。
    #[error("{msg}")]
    Business {
        /// 业务拒绝码。
        code: AuthErrorCode,
        /// 客户端可见文案。
        msg: String,
    },
    /// 内部故障：官方 API 不可达 → 降级策略（§12）。
    #[error("internal error: {msg}")]
    Internal {
        /// 错误描述（仅日志）。
        msg: String,
    },
}

/// 身份（§4.4）：token 解析出的用户身份。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserIdentity {
    /// 用户 id。
    pub user_id: i32,
    /// 昵称。
    pub name: String,
    /// 语言（原版 `language` 字段）。
    pub lang: String,
}

/// 鉴权处理器（§4.4 / §4.7 对象安全规则）。
#[async_trait::async_trait]
pub trait AuthHandler: Send + Sync {
    /// 鉴权：token → 身份。
    ///
    /// **每次调用必须自带超时（如 5s，评审 §8 三）**：鉴权挂起会卡死会话建立。
    async fn authenticate(&self, token: &str) -> Result<UserIdentity, AuthError>;
}
