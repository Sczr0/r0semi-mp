//! HTTP 回源客户端测试（§10.1 手写 HTTP/1.1 + 本地 mock API，§9 Oracle 环境）。

use std::net::SocketAddr;

use phira_api::{ApiClient, AuthHandler};
use phira_server::http::{HttpApiClient, HttpAuth};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// 简易 mock API：每次连接读请求头（验证 path/Bearer），按预设响应写回。
async fn mock_server(addr: SocketAddr, responses: Vec<(String, String, String)>) {
    let listener = TcpListener::bind(addr).await.unwrap();
    for (path, body, bearer) in responses {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = sock.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            head.extend_from_slice(&buf[..n]);
            if head.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let head_text = String::from_utf8_lossy(&head);
        assert!(
            head_text.starts_with(&format!("GET {path} ")),
            "mock 应收到 GET {path}, got: {head_text}"
        );
        if !bearer.is_empty() {
            assert!(
                head_text.contains(&format!("Authorization: Bearer {bearer}")),
                "mock 应收到 Bearer {bearer}: {head_text}"
            );
        }
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        sock.write_all(resp.as_bytes()).await.unwrap();
    }
}

#[tokio::test]
async fn fetch_chart_parses_json() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let mock = tokio::spawn(mock_server(
        addr,
        vec![(
            "/chart/7".into(),
            r#"{"id": 7, "name": "Test Chart"}"#.into(),
            String::new(),
        )],
    ));

    let client = HttpApiClient::new(format!("http://{addr}"));
    let chart = client.fetch_chart(7).await.unwrap();
    assert_eq!(chart.id, 7);
    assert_eq!(chart.name, "Test Chart");
    mock.await.unwrap();
}

#[tokio::test]
async fn fetch_record_parses_all_fields() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let body = r#"{"id": 1, "player": 42, "score": 980000, "perfect": 100,
        "good": 2, "bad": 1, "miss": 0, "max_combo": 150,
        "accuracy": 0.995, "full_combo": true, "std": 0.1, "std_score": 90.5}"#;
    let mock = tokio::spawn(mock_server(
        addr,
        vec![("/record/1".into(), body.to_owned(), String::new())],
    ));

    let client = HttpApiClient::new(format!("http://{addr}"));
    let rec = client.fetch_record(1).await.unwrap();
    assert_eq!(rec.player, 42);
    assert_eq!(rec.score, 980_000);
    assert_eq!(rec.max_combo, 150);
    assert!(rec.full_combo);
    mock.await.unwrap();
}

#[tokio::test]
async fn authenticate_sends_bearer_and_parses_me() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let mock = tokio::spawn(mock_server(
        addr,
        vec![(
            "/me".into(),
            r#"{"id": 99, "name": "p99", "language": "en"}"#.into(),
            "tok123".into(),
        )],
    ));

    let auth = HttpAuth::new(format!("http://{addr}"));
    let identity = auth.authenticate("tok123").await.unwrap();
    assert_eq!(identity.user_id, 99);
    assert_eq!(identity.name, "p99");
    assert_eq!(identity.lang, "en");
    mock.await.unwrap();
}

#[tokio::test]
async fn http_404_is_internal_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // 响应 404（覆盖 mock_server 的 200 假设——单独实现）
    let mock = tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        sock.write_all(resp.as_bytes()).await.unwrap();
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("404")),
        "404 应报 Internal 含状态码: {err:?}"
    );
    mock.await.unwrap();
}

#[allow(dead_code)]
fn _unused(_: TcpStream) {}

/// Never Trust the Client：token 含 CR/LF → 拒绝（防 HTTP 请求头注入，2026-08）。
#[tokio::test]
async fn auth_token_with_crlf_rejected() {
    let auth = HttpAuth::new("http://127.0.0.1:1".into()); // 端口 1 不监听——不应走到 connect
    let err = auth
        .authenticate("evil\r\nX-Evil: injected")
        .await
        .unwrap_err();
    assert!(
        matches!(&err, phira_api::AuthError::Internal { msg } if msg.contains("CR/LF")),
        "CR/LF token 应被拒绝: {err:?}"
    );
}

/// Never Trust the Client：token 含换行（\n 单独）同样拒绝。
#[tokio::test]
async fn auth_token_with_lf_rejected() {
    let auth = HttpAuth::new("http://127.0.0.1:1".into());
    let err = auth.authenticate("a\nb").await.unwrap_err();
    assert!(
        matches!(&err, phira_api::AuthError::Internal { msg } if msg.contains("CR/LF")),
        "LF token 应被拒绝: {err:?}"
    );
}

/// Never Trust the Client（回源响应同样不可信）：Content-Length 巨大（10GB）→
/// 渐进读不预分配、5s 超时截断 → Err（无内存放大）。
#[tokio::test]
async fn huge_content_length_times_out_without_oom() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // 服务器：响应头声明 10GB Content-Length，但只发 1KB 后挂住
    let mock = tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 10737418240\r\nConnection: close\r\n\r\n";
        sock.write_all(resp.as_bytes()).await.unwrap();
        sock.write_all(&[0u8; 1024]).await.unwrap(); // 1KB 后静默
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let start = std::time::Instant::now();
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg }
            if msg.contains("timeout") || msg.contains("truncated")),
        "超大 Content-Length 应超时或截断报错（不 OOM）: {err:?}"
    );
    assert!(
        start.elapsed() < std::time::Duration::from_secs(8),
        "不应无限等待"
    );
    mock.await.unwrap();
}
