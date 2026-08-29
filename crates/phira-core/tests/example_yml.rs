//! D-02 回归测试：`server_config.example.yml`（仓库根）能被 `Config::load_from_yaml`
//! 解析，且显式字段与代码默认一致——钉住"仅凭 example yml 可启动服务"。

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[test]
fn example_yaml_parses() {
    let text = std::fs::read_to_string("../../server_config.example.yml").unwrap();
    let config = phira_core::Config::load_from_yaml(
        |_| Err(std::env::VarError::NotPresent),
        Some(&text),
        Some("server_config.example.yml"),
    )
    .expect("example yml 应可解析");
    // 显式字段应生效
    assert_eq!(config.listen.to_string(), "0.0.0.0:12346");
    assert_eq!(config.api_base, "https://phira.5wyxi.com");
    assert_eq!(config.rooms.monitors, vec![2]);
    assert_eq!(config.reconnect_window, std::time::Duration::from_secs(10));
    assert_eq!(
        config.playing_reconnect_window,
        std::time::Duration::from_secs(60)
    );
    assert_eq!(config.auth_timeout, std::time::Duration::from_secs(10));
    assert_eq!(config.http_timeout, std::time::Duration::from_secs(5));
    assert_eq!(
        config.maintenance_notice,
        "服务器维护中，房间即将关闭，请稍后再来"
    );
    assert_eq!(config.persist_dir, "./data");
}
