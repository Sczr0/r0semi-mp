//! 配置加载与热重载（§4.5 / §4.9-8）。
//!
//! 配置是热重载的，不是构造期快照——`Bus::update_config` 广播 `UpdateConfig`（§4.9-8）。
//! 加载优先级：默认值 → `server_config.yml` → 环境变量（部署环境覆盖文件，§4.5）。

use std::net::{Ipv4Addr, SocketAddr};

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
    /// 断线重连窗口（§6.5-21，默认 10s）。
    pub reconnect_window: std::time::Duration,
    /// 对局中断线重连窗口（C-03/ADR-0012：默认 60s，> reconnect_window）。
    ///
    /// 窗口决策需要房间状态视野（core 不认识房间态）：Disconnected 时命令化查询
    /// `GetClientState`，`Playing` 用本窗口、其余用 [`Config::reconnect_window`]。
    pub playing_reconnect_window: std::time::Duration,
    /// 鉴权阶段超时（C-01：版本字节确认后→鉴权完成之间；默认 10s，> http_timeout）。
    ///
    /// 未鉴权连接若无限挂起会占住准入额度（§10.4），本超时兜底断开；
    /// 与 PROXY 头 5s / 握手 5s 同为 per-phase deadline（对照 gooophira）。
    pub auth_timeout: std::time::Duration,
    /// 回源 HTTP 请求超时（§4.4，默认 5s）。
    pub http_timeout: std::time::Duration,
    /// 停机维护宽限窗口（§11，默认 10s）。
    pub maintenance_grace: std::time::Duration,
    /// 配置文件轮询间隔（§4.9-8，默认 2s）。
    pub config_poll_interval: std::time::Duration,
    /// 停机维护通知文案（§11 系统 Chat，默认中文提示）。
    pub maintenance_notice: String,
    /// 进服欢迎语（§运营：鉴权成功后广播给本人，user=0 系统消息；None = 不发）。
    pub welcome_message: Option<String>,
    /// 私密房间 id 前缀（§运营：房间列表 `/rooms` 不展示这些房间，如 `["solo"]`）。
    pub hidden_room_prefixes: Vec<String>,
    /// 管理 HTTP 端口（§运营：`/rooms` 房间列表；None = 不开启）。
    pub http_port: Option<u16>,
    /// 管理 API Bearer token（阶段 2，docs/admin-api.md §2）：`/admin/*` 全部端点
    /// 需要 `Authorization: Bearer <token>`；None = 管理面（含读）一律 401 禁用。
    pub admin_token: Option<String>,
    /// 管理面持久化目录（组合根 storage：bans.json / audit.jsonl / config.current|last.json；
    /// 默认 `./data`，None = 仅内存）。
    pub persist_dir: String,
    /// PROXY protocol（§前置层：反代后真实 IP，每 IP 限额才有效）。
    ///
    /// true = 所有连接必须先发 PROXY 头（HAProxy `send-proxy` / nginx `proxy_protocol on`），
    /// 头缺失/非法 → 断开；false = 直连（默认）。
    pub proxy_protocol: bool,
    /// 安全锁 A：全局在途字节上限（§10.4 承诺兑现；默认 64MiB = 现值，见 server.rs）。
    ///
    /// 0/缺省 = 使用 server.rs 的 `MEMORY_GUARD_LIMIT` 常量默认（64MiB）。
    pub memory_guard_bytes: usize,
    /// 安全锁 A：每连接 send 队列字节上限（超限 → 该连接被踢；默认 8MiB = 现值）。
    ///
    /// 0/缺省 = 使用 server.rs 的 `PER_CONN_MEM_LIMIT` 常量默认（8MiB）。
    pub per_conn_mem_bytes: usize,
    /// 安全锁 B：已鉴权连接总数上限（§11 兑现；默认 1000 = 现值）。
    ///
    /// 0/缺省 = 使用 server.rs 的 `MAX_AUTHED_CONNECTIONS` 常量默认（1000）。
    pub max_authed_connections: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // ISSUE-0008 修复：默认 0.0.0.0（IPv4）——Windows 上 [::] 是 V6ONLY（只收 IPv6），
            // 玩家（IPv4 客户端）连不上；双栈需 socket2 V6ONLY=false，v1 用 IPv4 足够
            listen: SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 12346),
            api_base: "https://phira.5wyxi.com".to_owned(),
            // 原版默认白名单（server.rs：monitors: vec![2]）
            rooms: RoomConfig { monitors: vec![2] },
            reconnect_window: std::time::Duration::from_secs(10),
            playing_reconnect_window: std::time::Duration::from_secs(60),
            auth_timeout: std::time::Duration::from_secs(10),
            http_timeout: std::time::Duration::from_secs(5),
            maintenance_grace: std::time::Duration::from_secs(10),
            config_poll_interval: std::time::Duration::from_secs(2),
            maintenance_notice: "服务器维护中，房间即将关闭，请稍后再来".to_owned(),
            welcome_message: None,
            hidden_room_prefixes: Vec::new(),
            http_port: None,
            proxy_protocol: false,
            admin_token: None,
            persist_dir: "./data".to_owned(),
            // 安全锁阈值（P1 技术债：可参数化；0 = 使用 server.rs 的 const 默认 = 现值）。
            memory_guard_bytes: 0,
            per_conn_mem_bytes: 0,
            max_authed_connections: 0,
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
        if let Ok(token) = env("R0SEMI_MP_ADMIN_TOKEN") {
            config.admin_token = Some(token);
        }
        if let Ok(dir) = env("R0SEMI_MP_PERSIST_DIR") {
            config.persist_dir = dir;
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
        if let Some(secs) = yaml.reconnect_window {
            self.reconnect_window = std::time::Duration::from_secs(secs);
        }
        if let Some(secs) = yaml.playing_reconnect_window {
            self.playing_reconnect_window = std::time::Duration::from_secs(secs);
        }
        if let Some(secs) = yaml.auth_timeout {
            self.auth_timeout = std::time::Duration::from_secs(secs);
        }
        if let Some(secs) = yaml.http_timeout {
            self.http_timeout = std::time::Duration::from_secs(secs);
        }
        if let Some(secs) = yaml.maintenance_grace {
            self.maintenance_grace = std::time::Duration::from_secs(secs);
        }
        if let Some(secs) = yaml.config_poll_interval {
            self.config_poll_interval = std::time::Duration::from_secs(secs);
        }
        if let Some(notice) = yaml.maintenance_notice {
            self.maintenance_notice = notice;
        }
        if let Some(msg) = yaml.welcome_message {
            self.welcome_message = Some(msg);
        }
        if let Some(prefixes) = yaml.hidden_room_prefixes {
            self.hidden_room_prefixes = prefixes;
        }
        if let Some(port) = yaml.http_port {
            self.http_port = Some(port);
        }
        if let Some(proxy) = yaml.proxy_protocol {
            self.proxy_protocol = proxy;
        }
        if let Some(token) = yaml.admin_token {
            self.admin_token = Some(token);
        }
        if let Some(dir) = yaml.persist_dir {
            self.persist_dir = dir;
        }
        if let Some(v) = yaml.memory_guard_mb {
            self.memory_guard_bytes = v * 1024 * 1024;
        }
        if let Some(v) = yaml.per_conn_mem_mb {
            self.per_conn_mem_bytes = v * 1024 * 1024;
        }
        if let Some(v) = yaml.max_authed_connections {
            self.max_authed_connections = v;
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
    /// 断线重连窗口（秒）。
    reconnect_window: Option<u64>,
    /// 对局中断线重连窗口（秒；缺省 = reconnect_window，区分需要显式配）。
    playing_reconnect_window: Option<u64>,
    /// 鉴权阶段超时（秒；版本字节确认后→鉴权完成之间）。
    auth_timeout: Option<u64>,
    /// 回源 HTTP 请求超时（秒）。
    http_timeout: Option<u64>,
    /// 停机维护宽限窗口（秒）。
    maintenance_grace: Option<u64>,
    /// 配置文件轮询间隔（秒）。
    config_poll_interval: Option<u64>,
    /// 停机维护通知文案。
    maintenance_notice: Option<String>,
    /// 进服欢迎语（None/缺省 = 不发）。
    welcome_message: Option<String>,
    /// 私密房间 id 前缀（房间列表不展示）。
    hidden_room_prefixes: Option<Vec<String>>,
    /// 管理 HTTP 端口（None = 不开启）。
    http_port: Option<u16>,
    /// PROXY protocol（反代真实 IP）。
    proxy_protocol: Option<bool>,
    /// 管理 API Bearer token（None = 管理面禁用）。
    admin_token: Option<String>,
    /// 管理面持久化目录（None = 仅内存）。
    persist_dir: Option<String>,
    /// 安全锁 A：全局在途字节上限（MiB；缺省 0 = 用 server.rs const 默认 64MiB）。
    memory_guard_mb: Option<usize>,
    /// 安全锁 A：每连接 send 队列字节上限（MiB；缺省 0 = 用 server.rs const 默认 8MiB）。
    per_conn_mem_mb: Option<usize>,
    /// 安全锁 B：已鉴权连接总数上限（缺省 0 = 用 server.rs const 默认 1000）。
    max_authed_connections: Option<usize>,
}
