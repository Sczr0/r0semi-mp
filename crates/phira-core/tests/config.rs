//! phira-core config 单元测试（§4.5 / §4.9-8）。
//!
//! 环境变量通过 `Config::load_from` 注入 fake 来源（不写真实 env——
//! workspace 红线 `forbid(unsafe_code)` 禁止 unsafe，§5.1）。

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use phira_core::{Config, ConfigError};

/// fake 环境变量来源（§4.9-6 精神：可测性）。
fn fake_env(overrides: &[(&str, &str)]) -> impl Fn(&str) -> Result<String, std::env::VarError> {
    let map: HashMap<&str, String> = overrides.iter().map(|(k, v)| (*k, v.to_string())).collect();
    move |name| map.get(name).cloned().ok_or(std::env::VarError::NotPresent)
}

#[test]
fn default_config_matches_original() {
    let cfg = Config::load_from(fake_env(&[])).unwrap();
    // 原版默认端口 12346（§3.5 / 原版 main.rs）
    assert_eq!(cfg.listen.port(), 12346);
    // 原版默认 API 基地址（原版 session.rs HOST）
    assert_eq!(cfg.api_base, "https://phira.5wyxi.com");
    // 原版默认 monitor 白名单（原版 server.rs ServerConfig::default）
    assert_eq!(cfg.rooms.monitors, vec![2]);
}

#[test]
fn env_port_overrides() {
    let cfg = Config::load_from(fake_env(&[("R0SEMI_MP_PORT", "3939")])).unwrap();
    assert_eq!(cfg.listen.port(), 3939);
}

#[test]
fn env_api_base_overrides() {
    let cfg =
        Config::load_from(fake_env(&[("R0SEMI_MP_API_BASE", "https://example.test")])).unwrap();
    assert_eq!(cfg.api_base, "https://example.test");
}

#[test]
fn invalid_port_env_errors() {
    let err = Config::load_from(fake_env(&[("R0SEMI_MP_PORT", "not-a-port")])).unwrap_err();
    assert!(matches!(
        err,
        ConfigError::InvalidEnv {
            name: "R0SEMI_MP_PORT",
            ..
        }
    ));
}

#[test]
fn listen_addr_is_unspecified_ipv6() {
    let cfg = Config::load_from(fake_env(&[])).unwrap();
    // 原版 main.rs：Ipv6Addr::UNSPECIFIED（双栈）
    assert!(cfg.listen.ip().is_unspecified());
}

#[test]
fn listen_port_keep_default_when_env_absent() {
    // 环境变量缺失时保留默认值，不 panic、不覆盖
    let cfg = Config::load_from(fake_env(&[("R0SEMI_MP_API_BASE", "x")])).unwrap();
    assert_eq!(cfg.listen.port(), 12346);
}

// —— 阶段 5：server_config.yml（§4.6-4 / §4.9-8）——

#[test]
fn yaml_full_fields() {
    // 全字段 yml：listen + api_base + monitors
    let cfg = Config::load_from_yaml(
        fake_env(&[]),
        Some("listen: \"127.0.0.1:19999\"\napi_base: \"http://mock.test\"\nmonitors: [7, 8, 9]\n"),
        None,
    )
    .unwrap();
    assert_eq!(cfg.listen.port(), 19999);
    assert_eq!(cfg.listen.ip().to_string(), "127.0.0.1");
    assert_eq!(cfg.api_base, "http://mock.test");
    assert_eq!(cfg.rooms.monitors, vec![7, 8, 9]);
}

#[test]
fn yaml_partial_fields_keep_defaults() {
    // 只给 monitors：其余保持默认（缺失字段不覆盖）
    let cfg = Config::load_from_yaml(fake_env(&[]), Some("monitors: [42]\n"), None).unwrap();
    assert_eq!(cfg.rooms.monitors, vec![42]);
    assert_eq!(cfg.listen.port(), 12346, "缺失字段保持默认");
    assert_eq!(cfg.api_base, "https://phira.5wyxi.com", "缺失字段保持默认");
}

#[test]
fn yaml_with_comments_and_empty_lines() {
    let cfg = Config::load_from_yaml(
        fake_env(&[]),
        Some("# r0semi-mp 配置\n\n# 观战者白名单\nmonitors:\n  - 2\n  - 5\n"),
        None,
    )
    .unwrap();
    assert_eq!(cfg.rooms.monitors, vec![2, 5]);
}

#[test]
fn yaml_env_overrides_file() {
    // 优先级：yml < 环境变量（部署环境覆盖文件，§4.5）
    let cfg = Config::load_from_yaml(
        fake_env(&[("R0SEMI_MP_API_BASE", "https://env-override.test")]),
        Some("api_base: \"https://file.test\"\n"),
        None,
    )
    .unwrap();
    assert_eq!(cfg.api_base, "https://env-override.test");
}

#[test]
fn yaml_invalid_errors() {
    let err =
        Config::load_from_yaml(fake_env(&[]), Some("monitors: [not-a-number\n"), None).unwrap_err();
    assert!(matches!(err, ConfigError::InvalidYaml { .. }), "{err:?}");
}

#[test]
fn yaml_invalid_listen_errors() {
    let err = Config::load_from_yaml(fake_env(&[]), Some("listen: \"nope\"\n"), None).unwrap_err();
    assert!(matches!(err, ConfigError::InvalidYaml { .. }), "{err:?}");
}

#[test]
fn yaml_unknown_fields_ignored() {
    // 未知字段容忍（向前兼容：老版本服务器读新配置不炸）
    let cfg = Config::load_from_yaml(
        fake_env(&[]),
        Some("monitors: [1]\nfuture_field: \"whatever\"\n"),
        None,
    )
    .unwrap();
    assert_eq!(cfg.rooms.monitors, vec![1]);
}

// —— 运维参数配置化（2026-08：重连窗口/HTTP 超时/宽限窗口/轮询间隔/维护文案）——

#[test]
fn yaml_ops_params_full() {
    let cfg = Config::load_from_yaml(
        fake_env(&[]),
        Some(
            "reconnect_window: 30\nhttp_timeout: 8\nmaintenance_grace: 15\nconfig_poll_interval: 5\nmaintenance_notice: \"维护中，稍后回来\"\n",
        ),
        None,
    )
    .unwrap();
    assert_eq!(cfg.reconnect_window, std::time::Duration::from_secs(30));
    assert_eq!(cfg.http_timeout, std::time::Duration::from_secs(8));
    assert_eq!(cfg.maintenance_grace, std::time::Duration::from_secs(15));
    assert_eq!(cfg.config_poll_interval, std::time::Duration::from_secs(5));
    assert_eq!(cfg.maintenance_notice, "维护中，稍后回来");
}

#[test]
fn yaml_ops_params_absent_keep_defaults() {
    // 缺失 → 默认（10s 重连 / 5s 超时 / 10s 宽限 / 2s 轮询 / 默认文案）
    let cfg = Config::load_from_yaml(fake_env(&[]), Some("monitors: [1]\n"), None).unwrap();
    assert_eq!(cfg.reconnect_window, std::time::Duration::from_secs(10));
    assert_eq!(cfg.http_timeout, std::time::Duration::from_secs(5));
    assert_eq!(cfg.maintenance_grace, std::time::Duration::from_secs(10));
    assert_eq!(cfg.config_poll_interval, std::time::Duration::from_secs(2));
    assert!(cfg.maintenance_notice.contains("维护"));
}

#[test]
fn yaml_ops_zero_values_accepted() {
    // 0 秒合法（不报错；语义由调用方解释——如 0 宽限 = 立即退出）
    let cfg = Config::load_from_yaml(
        fake_env(&[]),
        Some("maintenance_grace: 0\nconfig_poll_interval: 0\n"),
        None,
    )
    .unwrap();
    assert_eq!(cfg.maintenance_grace, std::time::Duration::ZERO);
    assert_eq!(cfg.config_poll_interval, std::time::Duration::ZERO);
}

// —— §运营：欢迎语 / 私密房间前缀 / HTTP 管理端口 ——

#[test]
fn yaml_welcome_and_hidden_prefixes() {
    let cfg = Config::load_from_yaml(
        fake_env(&[]),
        Some(
            "welcome_message: \"欢迎来到 r0semi\"\nhidden_room_prefixes: [solo]\nhttp_port: 8080\n",
        ),
        None,
    )
    .unwrap();
    assert_eq!(cfg.welcome_message.as_deref(), Some("欢迎来到 r0semi"));
    assert_eq!(cfg.hidden_room_prefixes, vec!["solo".to_owned()]);
    assert_eq!(cfg.http_port, Some(8080));
}

#[test]
fn yaml_welcome_absent_means_none() {
    // 缺省 welcome_message → None（不发欢迎语）
    let cfg = Config::load_from_yaml(fake_env(&[]), Some("monitors: [2]\n"), None).unwrap();
    assert_eq!(cfg.welcome_message, None);
    assert!(cfg.hidden_room_prefixes.is_empty());
    assert_eq!(cfg.http_port, None);
}
