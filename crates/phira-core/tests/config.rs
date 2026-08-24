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
