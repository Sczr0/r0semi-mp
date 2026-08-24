//! 配置加载与热重载（§4.5 / §4.9-8）。
//!
//! 配置是热重载的，不是构造期快照——`Bus::update_config` 广播 `UpdateConfig`（§4.9-8）。
//! 加载优先级：默认值 → `server_config.yml` → 环境变量（部署环境覆盖文件，§4.5）。

use std::net::{Ipv6Addr, SocketAddr};

use phira_api::RoomConfig;
use serde::Deserialize;

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
    /// 配置文件读取失败。
    #[error("config file {path}: {source}")]
    ReadFile {
        /// 文件路径。
        path: String,
        /// 底层 IO 错误。
        source: std::io::Error,
    },
    /// 配置文件解析失败。
    #[error("config file {path}: {msg}")]
    InvalidYaml {
        /// 文件路径。
        path: String,
        /// 解析错误描述。
        msg: String,
    },
}

impl Config {
    /// 加载配置：默认值 + `server_config.yml`（`R0SEMI_MP_CONFIG` 可改路径）+ 环境变量覆盖。
    ///
    /// 配置文件不存在 = 跳过（全部走默认 + 环境变量）；存在但非法 = 启动失败（配置损坏不让起）。
    ///
    /// # Errors
    ///
    /// 环境变量非法 / 配置文件存在但读取或解析失败时返回对应 `ConfigError`。
    pub fn load() -> Result<Self, ConfigError> {
        let path =
            std::env::var("R0SEMI_MP_CONFIG").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_owned());
        let yaml = match std::fs::read_to_string(&path) {
            Ok(text) => Some(text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                return Err(ConfigError::ReadFile {
                    path: path.clone(),
                    source: e,
                });
            }
        };
        Self::load_from_yaml(|name| std::env::var(name), yaml.as_deref(), Some(&path))
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
        Self::load_from_yaml(env, None, None)
    }

    /// 加载：默认值 → yml 文本（如有）→ 环境变量覆盖（优先级最高）。
    ///
    /// # Errors
    ///
    /// 环境变量非法 / yml 解析失败时返回对应 `ConfigError`。
    pub fn load_from_yaml<E>(
        env: E,
        yaml: Option<&str>,
        yaml_path: Option<&str>,
    ) -> Result<Self, ConfigError>
    where
        E: Fn(&str) -> Result<String, std::env::VarError>,
    {
        let mut config = Self::default();
        if let Some(text) = yaml {
            config.apply_yaml(text, yaml_path)?;
        }
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

    /// 合并 yml 文本（覆盖默认值；缺失字段保持默认）。
    ///
    /// # Errors
    ///
    /// yml 语法 / 类型非法时返回 `ConfigError::InvalidYaml`。
    pub fn apply_yaml(&mut self, text: &str, path: Option<&str>) -> Result<(), ConfigError> {
        let yaml: YamlConfig =
            serde_yaml_ng::from_str(text).map_err(|e| ConfigError::InvalidYaml {
                path: path.unwrap_or(DEFAULT_CONFIG_PATH).to_owned(),
                msg: e.to_string(),
            })?;
        if let Some(listen) = yaml.listen {
            self.listen = listen.parse().map_err(|_| ConfigError::InvalidYaml {
                path: path.unwrap_or(DEFAULT_CONFIG_PATH).to_owned(),
                msg: format!("invalid listen address: {listen}"),
            })?;
        }
        if let Some(api_base) = yaml.api_base {
            self.api_base = api_base;
        }
        if let Some(monitors) = yaml.monitors {
            self.rooms.monitors = monitors;
        }
        Ok(())
    }
}

/// 默认配置文件路径（工作目录；`R0SEMI_MP_CONFIG` 可覆盖）。
pub const DEFAULT_CONFIG_PATH: &str = "server_config.yml";

/// `server_config.yml` 的 DTO（§4.6-4：monitor 白名单在此；缺失字段保持默认值）。
///
/// phira-api 零 serde 红线：DTO 副本在 phira-core，转换回 `RoomConfig` 由 `apply_yaml` 完成。
#[derive(Debug, Default, Deserialize)]
struct YamlConfig {
    /// 监听地址（`"0.0.0.0:12346"`）。
    listen: Option<String>,
    /// 官方 API 基地址。
    api_base: Option<String>,
    /// 观战者白名单（§6.5-4 / §4.6-4）。
    monitors: Option<Vec<i32>>,
}
