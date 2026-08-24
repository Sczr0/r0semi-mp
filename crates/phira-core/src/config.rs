//! 配置加载与热重载（§4.5 / §4.9-8）。
//!
//! 配置是热重载的，不是构造期快照——`Bus::update_config` 广播 `UpdateConfig`（§4.9-8）。

use std::net::{Ipv6Addr, SocketAddr};

use phira_api::RoomConfig;

/// 服务器配置（§4.5 / §4.9-8）。
#[derive(Debug, Clone)]
pub struct Config {
    /// 监听地址（默认 0.0.0.0:12346——原版默认端口，§3.5）。
    pub listen: SocketAddr,
    /// 官方 API 基地址（阶段 2 回源用；§6.5-14/15）。
    pub api_base: String,
    /// 房间配置（monitors 白名单等，§6.5-4）。
    pub rooms: RoomConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 12346),
            api_base: "https://phira.5wyxi.com".to_owned(),
            // 原版默认白名单（server.rs：monitors: vec![2]）
            rooms: RoomConfig { monitors: vec![2] },
        }
    }
}

/// 配置错误。
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// 环境变量解析失败。
    #[error("invalid env var {name}: {value}")]
    InvalidEnv {
        /// 变量名。
        name: &'static str,
        /// 非法值。
        value: String,
    },
}

impl Config {
    /// 加载配置：默认值 + 环境变量覆盖。
    ///
    /// # Errors
    ///
    /// 环境变量值非法（如端口非数字）时返回 `ConfigError::InvalidEnv`。
    ///
    /// TODO(阶段 5): 支持 server_config.yml（原版语义）+ 文件轮询热重载
    /// （`Bus::watch_config`，§4.9-8；机制 = `Bus::update_config`）。
    pub fn load() -> Result<Self, ConfigError> {
        Self::load_from(|name| std::env::var(name))
    }

    /// 从可注入的环境变量来源加载（测试可注入 fake 环境，§4.9-6 精神：可测性）。
    ///
    /// # Errors
    ///
    /// 环境变量值非法时返回 `ConfigError::InvalidEnv`。
    pub fn load_from<E>(env: E) -> Result<Self, ConfigError>
    where
        E: Fn(&str) -> Result<String, std::env::VarError>,
    {
        let mut config = Self::default();
        if let Ok(port) = env("R0SEMI_MP_PORT") {
            config
                .listen
                .set_port(port.parse().map_err(|_| ConfigError::InvalidEnv {
                    name: "R0SEMI_MP_PORT",
                    value: port,
                })?);
        }
        if let Ok(base) = env("R0SEMI_MP_API_BASE") {
            config.api_base = base;
        }
        Ok(config)
    }
}
