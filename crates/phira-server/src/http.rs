//! 生产实现：HTTP 回源客户端 + 随机源 + 鉴权（§4.5 组合根内，原则 5）。
//!
//! v1 生产实现直接放组合根，第二实现出现再抽独立 crate（原则 5 对自己生效，§4.5）。

use phira_api::{
    ApiClient, ApiError, AuthError, AuthErrorCode, AuthHandler, Chart, RandomSource, Record,
    UserIdentity,
};
use rand::Rng;

/// 回源 HTTP 客户端（§4.9-6 / §6.5-15）。
///
/// TODO(阶段 2): 手写 HTTP/1.1 GET + rustls 单栈（§10.1 / 附录 D P1）——
/// 回源只是带 Bearer 的 GET，约两百行，最贴合内存目标；当前为占位（返回 Internal）。
pub struct HttpApiClient {
    base: String,
}

impl HttpApiClient {
    /// 构造。`base` = 官方 API 基地址（Config::api_base）。
    pub fn new(base: String) -> Self {
        Self { base }
    }
}

#[async_trait::async_trait]
impl ApiClient for HttpApiClient {
    async fn fetch_chart(&self, _id: i32) -> Result<Chart, ApiError> {
        Err(ApiError::Internal {
            msg: format!("http client not implemented (phase 2), base={}", self.base),
        })
    }

    async fn fetch_record(&self, _id: i32) -> Result<Record, ApiError> {
        Err(ApiError::Internal {
            msg: format!("http client not implemented (phase 2), base={}", self.base),
        })
    }
}

/// 鉴权处理器（§4.4 / §6.5-14）。
///
/// TODO(阶段 2): 回源 `GET {base}/me`（Bearer token）→ 身份解析。
#[allow(dead_code)] // 阶段 2 鉴权编排接入后启用
pub struct HttpAuth {
    base: String,
}

impl HttpAuth {
    /// 构造。`base` = 官方 API 基地址。
    #[allow(dead_code)] // 阶段 2 鉴权编排接入后启用
    pub fn new(base: String) -> Self {
        Self { base }
    }
}

#[async_trait::async_trait]
impl AuthHandler for HttpAuth {
    async fn authenticate(&self, _token: &str) -> Result<UserIdentity, AuthError> {
        Err(AuthError::Business {
            code: AuthErrorCode::InvalidToken,
            msg: format!("auth not implemented (phase 2), base={}", self.base),
        })
    }
}

/// 生产随机源（§4.9-6）：房主随机选择。
#[derive(Default)]
pub struct ThreadRngSource;

impl RandomSource for ThreadRngSource {
    fn pick_index(&self, len: usize) -> Option<usize> {
        if len == 0 {
            None
        } else {
            Some(rand::rng().random_range(0..len))
        }
    }
}
