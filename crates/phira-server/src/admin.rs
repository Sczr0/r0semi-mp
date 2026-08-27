//! 管理 HTTP 面（§运营，独立端口 `http_port`；docs/admin-api.md 设计定稿）。
//!
//! **C1 拆分第 1 步（2026-08）**：`http_serve`/`http_accept_loop` 从 server.rs（1594 行
//! 上帝文件）抽出到本模块。角色 = 组合根旁的无状态翻译层：只读查询全部走既有快照
//! （RoomListSink / SessionSink / Metrics），零写风险；写面（系统命令族）留阶段 2。
//!
//! 端点（阶段 1）：`/` 端点清单 · `/rooms` 公开列表（隐私过滤）· `/healthz` 测活 +
//! Metrics（B3）· `/admin/rooms[?state=]` · `/admin/rooms/{id}` · `/admin/users` ·
//! `/admin/metrics`。
//!
//! 健壮性继承（防御哲学与回源侧一致，§10.4/C3）：头 ≤4KiB、体经 serde_json 自限、
//! 端口隔离（管理面挂掉不影响 MP 入口）、`Connection: close` 无长连接面。

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use crate::server::ConnContext;

/// 进程启动时刻（/healthz uptime 数据源，§11.1；OnceLock 惰性初始化）。
static PROCESS_START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

/// 进程已运行秒数（§11.1 /healthz）。
fn uptime_s() -> u64 {
    let start = PROCESS_START.get_or_init(std::time::Instant::now);
    start.elapsed().as_secs()
}

/// 管理端口 accept 循环（口 `http_port` 配置时由组合根 spawn）。
pub async fn http_accept_loop(listener: Option<TcpListener>, ctx: Arc<ConnContext>) {
    let Some(listener) = listener else {
        return;
    };
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let ctx = Arc::clone(&ctx);
                tokio::spawn(async move {
                    if let Err(err) = http_serve(stream, addr, ctx).await {
                        warn!("http handler error from {addr}: {err:?}");
                    }
                });
            }
            Err(err) => warn!("http accept failed: {err:?}"),
        }
    }
}

/// bus 的 Metrics 快照转 JSON 对象（`/healthz` 与 `/admin/metrics` 共用）。
fn metrics_json(ctx: &ConnContext) -> serde_json::Value {
    let snap = ctx.bus.metrics().snapshot();
    let mut map = serde_json::Map::new();
    for (name, s) in snap {
        map.insert(
            name.to_owned(),
            serde_json::json!({
                "calls": s.calls,
                "ok": s.ok,
                "business": s.business,
                "internal": s.internal,
                "avg_latency_ms": s.avg_latency_ms,
            }),
        );
    }
    serde_json::Value::Object(map)
}

/// 单请求处理：读头（≤4KiB）→ 路由 → 手写响应（`Connection: close`）。
/// 路由表按阶段分层（docs/admin-api.md §4）：公共面 + 阶段 1 只读管理面。
///
/// # Errors
///
/// 读写对端失败时返回 IO 错误。
#[allow(clippy::too_many_lines)] // 路由表完整呈现优于拆碎（端点逐条可审计）
pub async fn http_serve(
    mut stream: TcpStream,
    addr: std::net::SocketAddr,
    ctx: Arc<ConnContext>,
) -> std::io::Result<()> {
    // 读请求头（到空行，<=4KiB）
    let mut head = Vec::new();
    let mut buf = [0u8; 512];
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        head.extend_from_slice(&buf[..n]);
        if head.windows(4).any(|w| w == b"\r\n\r\n") || head.len() > 4096 {
            break;
        }
    }
    let text = String::from_utf8_lossy(&head);
    let target = text
        .lines()
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .unwrap_or("/");
    // 分离路径与查询串（`/admin/rooms?state=playing`）
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p, q),
        None => (target, ""),
    };
    let query = query.to_ascii_lowercase();

    let (status, body, ctype) = match path {
        "/" => (
            "200 OK",
            serde_json::json!({
                "service": "r0semi-mp",
                "endpoints": ["/rooms", "/healthz", "/admin/rooms", "/admin/rooms/{id}", "/admin/users", "/admin/metrics"],
            })
            .to_string(),
            "application/json; charset=utf-8",
        ),
        "/rooms" => {
            let rooms = ctx.room_list.snapshot().await;
            let body = serde_json::to_string(&rooms).unwrap_or_else(|_| "[]".to_owned());
            ("200 OK", body, "application/json; charset=utf-8")
        }
        "/healthz" => {
            // §11.1 方案 B：测活 + 测健康一步到位；不依赖官方 API（官方挂掉不影响测活）
            // B3（技术债）：把 bus 收集的 Metrics 也暴露出来
            let body = serde_json::json!({
                "status": "ok",
                "version": env!("CARGO_PKG_VERSION"),
                "uptime_s": uptime_s(),
                "connections": ctx.sink.conn_count().await,
                "rooms": ctx.room_list.snapshot().await.len(),
                "internal_errors": ctx.bus.metrics().internal_errors(),
                "metrics": metrics_json(&ctx),
            })
            .to_string();
            ("200 OK", body, "application/json; charset=utf-8")
        }
        // —— 阶段 1：只读管理面（docs/admin-api.md §4） ——
        "/admin/metrics" => {
            let body = serde_json::json!({
                "internal_errors": ctx.bus.metrics().internal_errors(),
                "metrics": metrics_json(&ctx),
            })
            .to_string();
            ("200 OK", body, "application/json; charset=utf-8")
        }
        "/admin/rooms" => {
            // 状态过滤：`?state=` 子串匹配（大小写不敏感；RoomListSink 状态字符串
            // 如 "Playing"/"WaitingForReady"/"SelectChart(1)"）；不传 = 全部。
            let rooms = ctx.room_list.snapshot().await;
            let body = if let Some(st) = query.strip_prefix("state=") {
                let rooms: Vec<_> = rooms
                    .into_iter()
                    .filter(|r| r.state.to_ascii_lowercase().contains(st))
                    .collect();
                serde_json::to_string(&rooms).unwrap_or_else(|_| "[]".to_owned())
            } else {
                serde_json::to_string(&rooms).unwrap_or_else(|_| "[]".to_owned())
            };
            ("200 OK", body, "application/json; charset=utf-8")
        }
        target if target.starts_with("/admin/rooms/") => {
            let id = &target["/admin/rooms/".len()..];
            let rooms = ctx.room_list.snapshot().await;
            match rooms.into_iter().find(|r| r.id == id) {
                Some(room) => {
                    let body = serde_json::to_string(&room).unwrap_or_else(|_| "{}".to_owned());
                    ("200 OK", body, "application/json; charset=utf-8")
                }
                None => ("404 Not Found", "room not found".to_owned(), "text/plain"),
            }
        }
        "/admin/users" => {
            // 在线用户：SessionSink 会话表 + 注册表名字 + 路由表房间归属（组合根视角拼装）
            let mut users = Vec::new();
            for user_id in ctx.sink.online().await {
                let name = ctx.registry.name_of(user_id);
                let room_id = ctx.bus.room_of(user_id).await;
                users.push(serde_json::json!({
                    "user_id": user_id,
                    "name": name,
                    "room_id": room_id.map(|r| r.as_str().to_owned()),
                }));
            }
            let body = serde_json::to_string(&users).unwrap_or_else(|_| "[]".to_owned());
            ("200 OK", body, "application/json; charset=utf-8")
        }
        _ => ("404 Not Found", "not found".to_owned(), "text/plain"),
    };

    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(resp.as_bytes()).await?;
    info!("http {path} from {addr} -> {status}");
    Ok(())
}

// —— 本模块仅做读查询；写面（POST /admin/...）留阶段 2，届时走系统命令族 ——
