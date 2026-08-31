//! HTTP 回源客户端测试（§10.1 手写 HTTP/1.1 + 本地 mock API，§9 Oracle 环境）。

use std::time::Duration;

use phira_api::{ApiClient, AuthHandler};
use phira_server::http::{HttpApiClient, HttpAuth};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// 简易 mock API：每次连接读请求头（验证 path/Bearer），按预设响应写回。
async fn mock_server(listener: TcpListener, responses: Vec<(String, String, String)>) {
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

    let mock = tokio::spawn(mock_server(
        listener,
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

    let body = r#"{"id": 1, "player": 42, "score": 980000, "perfect": 100,
        "good": 2, "bad": 1, "miss": 0, "max_combo": 150,
        "accuracy": 0.995, "full_combo": true, "std": 0.1, "std_score": 90.5}"#;
    let mock = tokio::spawn(mock_server(
        listener,
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

    let mock = tokio::spawn(mock_server(
        listener,
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

    // 响应 404（覆盖 mock_server 的 200 假设——单独实现）
    let mock = tokio::spawn(async move {
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
/// 加固后（C3，2026-08）声明值超 16MiB 上限 → **不读不缓冲直接拒绝**（比旧的
/// "渐进读 + 5s 超时"更快更省；原断言 timeout/truncated 随加固升级为 too large）。
#[tokio::test]
async fn huge_content_length_times_out_without_oom() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // 服务器：响应头声明 10GB Content-Length，但只发 1KB 后挂住
    let mock = tokio::spawn(async move {
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
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("too large")),
        "超大 Content-Length 应按上限拒绝（不读不 OOM）: {err:?}"
    );
    assert!(
        start.elapsed() < std::time::Duration::from_secs(8),
        "不应无限等待"
    );
    mock.await.unwrap();
}

/// 配置化接线：`http_timeout`（yml）穿透——自定义 1s 超时 + 慢回源 → 1s 内快速失败。
#[tokio::test]
async fn custom_http_timeout_applies() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // mock：收到请求后 sleep 3s 再响应（客户端 1s 超时应先触发）
    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let _ = sock
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .await;
    });

    let client = HttpApiClient::new_with_timeout(
        format!("http://{addr}"),
        std::time::Duration::from_secs(1),
    );
    let start = std::time::Instant::now();
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("timeout")),
        "1s 超时应触发: {err:?}"
    );
    assert!(
        start.elapsed() < std::time::Duration::from_secs(2),
        "应在 ~1s 超时而非等 3s: {:?}",
        start.elapsed()
    );
    mock.await.unwrap();
}

/// 配置化接线：HttpAuth 的 timeout 同样穿透（/me 慢响应 → 快速失败）。
#[tokio::test]
async fn custom_auth_timeout_applies() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let _ = sock
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .await;
    });

    let auth =
        HttpAuth::new_with_timeout(format!("http://{addr}"), std::time::Duration::from_secs(1));
    let err = auth.authenticate("tok").await.unwrap_err();
    assert!(
        matches!(&err, phira_api::AuthError::Internal { msg } if msg.contains("timeout")),
        "HttpAuth 1s 超时应触发: {err:?}"
    );
    mock.await.unwrap();
}

/// 反向：短超时 + 即时响应 → 正常成功（超时参数不误伤快路径）。
#[tokio::test]
async fn short_timeout_still_succeeds_on_fast_path() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock = tokio::spawn(mock_server(
        listener,
        vec![(
            "/chart/3".into(),
            r#"{"id": 3, "name": "Fast Chart"}"#.into(),
            String::new(),
        )],
    ));

    let client = HttpApiClient::new_with_timeout(
        format!("http://{addr}"),
        std::time::Duration::from_millis(100),
    );
    let chart = client.fetch_chart(3).await.unwrap();
    assert_eq!(chart.id, 3);
    assert_eq!(chart.name, "Fast Chart");
    mock.await.unwrap();
}

// ===== C3 加固（2026-08）：响应体上限 + 重定向显式拒绝 =====

/// Content-Length 声明超上限（>16MiB）→ 不读 body 直接拒绝。
#[tokio::test]
async fn oversized_content_length_rejected_without_reading() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        // 声明 64MiB body，不实际发送
        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 67108864\r\nConnection: close\r\n\r\n";
        sock.write_all(resp.as_bytes()).await.unwrap();
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("too large")),
        "超限 Content-Length 应被拒绝: {err:?}"
    );
    mock.await.unwrap();
}

/// 302 至跨域 host → 拒绝（token 绝不离开信任域；可诊断错误）。
#[tokio::test]
async fn redirect_302_cross_host_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        let resp = "HTTP/1.1 302 Found\r\nLocation: http://evil.example:1234/chart\r\nContent-Length: 0\r\n\r\n";
        sock.write_all(resp.as_bytes()).await.unwrap();
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(7).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("untrusted host")),
        "跨域 302 应拒绝而非跟随: {err:?}"
    );
    mock.await.unwrap();
}

/// 按序应答 mock：第 i 次 accept 校验请求以「GET {path_i} 」开头，回写 {resp_i}。
/// （跟随测试专用：多次连接、原始响应可控；mock_server 固定回 200 不适配。）
async fn mock_sequence(listener: TcpListener, steps: Vec<(String, String)>) {
    let mut buf = [0u8; 4096];
    for (path, resp) in steps {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        loop {
            let n = sock.read(&mut buf).await.unwrap();
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
        sock.write_all(resp.as_bytes()).await.unwrap();
    }
}

/// 302 跟随：绝对 URL（同 host）→ 相对 Location → 终态 200，谱面取回。
#[tokio::test]
async fn redirect_302_followed_same_host() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let abs = format!("http://{addr}/chart/7");
    let body = br#"{"id": 7, "name": "T"}"#;
    let ok = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        String::from_utf8_lossy(body)
    );

    let mock = tokio::spawn(async move {
        mock_sequence(
            listener,
            vec![
                (
                    "/chart/7".to_owned(),
                    format!("HTTP/1.1 302 Found\r\nLocation: {abs}\r\nContent-Length: 0\r\n\r\n"),
                ),
                (
                    "/chart/7".to_owned(),
                    "HTTP/1.1 302 Found\r\nLocation: /chart/7\r\nContent-Length: 0\r\n\r\n"
                        .to_owned(),
                ),
                ("/chart/7".to_owned(), ok),
            ],
        )
        .await;
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let chart = client.fetch_chart(7).await.expect("两跳后应取到谱面");
    assert_eq!(chart.id, 7);
    assert_eq!(chart.name, "T");
    mock.await.unwrap();
}

/// 302 自环 → 跳数耗尽报错（不无限跟随）。
#[tokio::test]
async fn redirect_302_loop_exhausted() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock = tokio::spawn(async move {
        let step = (
            "/chart/7".to_owned(),
            "HTTP/1.1 302 Found\r\nLocation: /chart/7\r\nContent-Length: 0\r\n\r\n".to_owned(),
        );
        mock_sequence(listener, vec![step; 4]).await;
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(7).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("too many redirects")),
        "自环 302 应在跳数上限后报错: {err:?}"
    );
    mock.await.unwrap();
}

/// 302 无 Location → 显式报错（不可跟随），不静默。
#[tokio::test]
async fn redirect_302_without_location_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        let resp = "HTTP/1.1 302 Found\r\nContent-Length: 0\r\n\r\n";
        sock.write_all(resp.as_bytes()).await.unwrap();
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(7).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("without Location")),
        "302 无 Location 应显式报错: {err:?}"
    );
    mock.await.unwrap();
}

/// 声明长度之外的尾随数据 → 按 Content-Length 截取、余量丢弃
/// （钉住"内存上界 = 声明值 + 一个 chunk"的读取语义；C3 后声明值本身受 16MiB 上限约束）。
#[tokio::test]
async fn trailing_bytes_beyond_content_length_discarded() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        // 声明 = 合法 JSON 实际长度，随后继续发垃圾——客户端只取前 len 字节
        let body = br#"{"id": 7, "name": "T"}"#;
        sock.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        sock.write_all(body).await.unwrap();
        let chunk = [b'x'; 4096];
        for _ in 0..1000 {
            if sock.write_all(&chunk).await.is_err() {
                break;
            }
        }
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let chart = client.fetch_chart(7).await.unwrap();
    assert_eq!(chart.id, 7);
    mock.await.unwrap();
}

// —— 错误分支补齐（覆盖率第 6 项）——

/// base URL 协议不支持 → 内部错误（不发起连接）。
#[tokio::test]
async fn base_url_bad_scheme_rejected() {
    let client = HttpApiClient::new("ftp://upstream".into());
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("unsupported base url scheme")),
        "非法 scheme 应拒绝: {err:?}"
    );
}

/// base URL 端口越界 → 内部错误。
#[tokio::test]
async fn base_url_bad_port_rejected() {
    let client = HttpApiClient::new("http://upstream:99999".into());
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("invalid port")),
        "越界端口应拒绝: {err:?}"
    );
}

/// 302 跳转到同主机但**非信任端口** → 拒绝（防端口走私）。
#[tokio::test]
async fn redirect_cross_port_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let target = format!("http://127.0.0.1:{}/chart/1", addr.port() + 1);
    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        let resp = format!(
            "HTTP/1.1 302 Found\r\nLocation: {target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        sock.write_all(resp.as_bytes()).await.unwrap();
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("redirect to untrusted port")),
        "跨端口跳转应拒绝: {err:?}"
    );
    mock.await.unwrap();
}

/// 响应头超 64KiB → 拒绝（防上游头洪水）。
#[tokio::test]
async fn oversized_response_headers_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        let mut resp = String::from("HTTP/1.1 200 OK\r\n");
        resp.push_str(&"X-Pad: yyyyyyyyyy\r\n".repeat(7000)); // ~91KB 头
        resp.push_str("\r\n");
        sock.write_all(resp.as_bytes()).await.unwrap();
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("response headers too large")),
        "超大响应头应拒绝: {err:?}"
    );
    mock.await.unwrap();
}

/// 声明 16MiB（恰好合法）但实发超限 → 读中拒绝（防御"声明小实发多"）。
#[tokio::test]
async fn body_exceeds_limit_while_streaming_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            16 * 1024 * 1024
        );
        sock.write_all(resp.as_bytes()).await.unwrap();
        // 实发 16MiB + 余量（读中超过上限触发防御分支）
        // 用不整除 16MiB 的块长（10000B）——末次读必然越过上限触发防御分支
        let chunk = [0x41u8; 10_000];
        for _ in 0..(16 * 1024 * 1024 / 10_000 + 16) {
            if sock.write_all(&chunk).await.is_err() {
                break; // 客户端已断——测试目标达成
            }
        }
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("exceeded limit while reading")),
        "流式超限应拒绝: {err:?}"
    );
    mock.await.unwrap();
}

/// 畸形状态行（无状态码）→ 内部错误。
#[tokio::test]
async fn malformed_status_line_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        sock.write_all(b"BANANA\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("malformed status line")),
        "畸形状态行应拒绝: {err:?}"
    );
    mock.await.unwrap();
}

/// 状态码非 UTF-8 字节 → 内部错误。
#[tokio::test]
async fn malformed_status_code_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        sock.write_all(b"HTTP/1.1 2\xFF0 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("malformed status code")),
        "非法字节状态码应拒绝: {err:?}"
    );
    mock.await.unwrap();
}

/// 状态码非数字 → 内部错误。
#[tokio::test]
async fn invalid_status_code_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        sock.write_all(b"HTTP/1.1 ABC Nope\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("invalid status code")),
        "非数字状态码应拒绝: {err:?}"
    );
    mock.await.unwrap();
}

/// Content-Length 非法 → 内部错误。
#[tokio::test]
async fn invalid_content_length_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: oops\r\n\r\n")
            .await
            .unwrap();
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("invalid content-length")),
        "非法 Content-Length 应拒绝: {err:?}"
    );
    mock.await.unwrap();
}

/// 无 Content-Length + 无体 → 空体 → get_json 解析失败（invalid JSON 分支 + CL 缺失 = 0）。
#[tokio::test]
async fn missing_content_length_empty_body_invalid_json() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        sock.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("invalid JSON")),
        "空体应报 invalid JSON: {err:?}"
    );
    mock.await.unwrap();
}

/// 200 + 非 JSON 体 → get_json 的 invalid JSON 分支（L69-70）。
#[tokio::test]
async fn invalid_json_body_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = "<html>not json</html>";
    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        sock.write_all(resp.as_bytes()).await.unwrap();
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("invalid JSON")),
        "非 JSON 体应报 invalid JSON: {err:?}"
    );
    mock.await.unwrap();
}

/// /me 返回非 JSON → HttpAuth 的 invalid JSON 分支（L168-169）。
#[tokio::test]
async fn auth_invalid_json_body_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nnope!!")
            .await
            .unwrap();
    });

    let auth = HttpAuth::new(format!("http://{addr}"));
    let err = auth.authenticate("tok").await.unwrap_err();
    assert!(
        matches!(&err, phira_api::AuthError::Internal { msg } if msg.contains("invalid JSON")),
        "/me 非 JSON 应报 invalid JSON: {err:?}"
    );
    mock.await.unwrap();
}

/// 声明有体但连接中断 → body truncated（L491-493）。
#[tokio::test]
async fn truncated_body_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await.unwrap();
        sock.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n0123456789",
        )
        .await
        .unwrap();
        // 关闭（不再发剩余 90 字节）
    });

    let client = HttpApiClient::new(format!("http://{addr}"));
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg } if msg.contains("body truncated")),
        "截断体应报 truncated: {err:?}"
    );
    mock.await.unwrap();
}

/// https base + 端口不可达 → 连接错误。
/// 注：rustls 0.23 起 IP 字面量可作为 SNI（IpAddress 变体），"invalid hostname"
/// 分支仅剩防御性（非法输入被 URL 解析提前拦下）。
/// 失败发生在哪个阶段依网络环境而定：直连 → TCP connect 失败（文案含 connect）；
/// 代理/防火墙 accept 后丢弃 → TLS 握手 EOF（文案含 handshake）。两者都必须映射 Internal。
#[tokio::test]
async fn tls_connect_failure_reported() {
    let client =
        HttpApiClient::new_with_timeout("https://192.0.2.1:1".into(), Duration::from_secs(2));
    let err = client.fetch_chart(1).await.unwrap_err();
    assert!(
        matches!(&err, phira_api::ApiError::Internal { msg }
            if msg.contains("connect") || msg.contains("handshake")),
        "连接失败应报 Internal: {err:?}"
    );
}
