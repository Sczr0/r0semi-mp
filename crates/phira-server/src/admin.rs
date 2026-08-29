//! 管理 HTTP 面（§运营，独立端口 `http_port`；docs/admin-api.md 设计定稿）。
//!
//! **C1 拆分第 1 步（2026-08）**：`http_serve`/`http_accept_loop` 从 server.rs 上帝文件
//! 抽出。角色 = 组合根旁的无状态翻译层：读查询走既有快照，写动作翻译成**系统命令族**
//! （`AdminKick`/`AdminBroadcast`，§4.4 薄缝）——管理 API 不认识 impl、不持有状态。
//!
//! 阶段 1（只读）：`/` `/rooms` `/healthz` + `/admin/rooms[?state=]` `/admin/rooms/{id}`
//! `/admin/users` `/admin/metrics`。`/rooms` 与管理读面的房间 JSON 统一经
//! `room_json` 渲染为外部标准格式（2026-08 定稿：roomid/lock/host/state/chart/players）。
//! 阶段 2（写面 + 审计 + 认证，docs/admin-api.md §3 四件套）：`POST /admin/rooms/{id}/kick`、
//! `POST /admin/rooms/{id}/broadcast`、`POST /admin/users/{id}/ban`、
//! `POST /admin/users/{id}/disconnect`、`GET /admin/audit`；**全部 `/admin/*` 需要
//! `Authorization: Bearer <admin_token>`**（token 未配置 = 管理面整体 401 禁用）。
//!
//! 健壮性继承：头 ≤4KiB、POST 体 ≤1KiB、端口隔离（管理面挂掉不影响 MP 入口）、
//! `Connection: close`。

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use phira_api::{RoomCommand, RoomConfig, RoomError, RoomErrorCode, RoomResponse};
use phira_core::lifecycle::SessionRegistry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use crate::server::{ConnContext, RoomInfo};

/// 进程启动时刻（/healthz uptime 数据源，§11.1；OnceLock 惰性初始化）。
static PROCESS_START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

/// 房间快照项 → 标准公开房间 JSON（2026-08 对齐外部格式定稿）：
/// `roomid`/`lock`/`host{name,id}`/`state`(select_chart|playing|wait_for_ready)/
/// `chart{name,id}`/`players[{name,id}]`；**id 统一 int**，`players` 不含 monitor
/// （RoomListSink 已过滤）。名字经 `SessionRegistry` 渲染时解析（未注册 → null；
/// 存活期成立：`evict_name` 在 `UserLeft` 之后，lifecycle.rs）。
fn room_json(info: &RoomInfo, registry: &SessionRegistry) -> serde_json::Value {
    serde_json::json!({
        "roomid": info.id,
        "cycle": info.cycle,
        "lock": info.locked,
        "host": { "name": registry.name_of(info.host), "id": info.host },
        "state": info.state,
        "chart": info
            .chart
            .as_ref()
            .map(|(name, id)| serde_json::json!({ "name": name, "id": id })),
        "players": info
            .players
            .iter()
            .map(|id| serde_json::json!({ "name": registry.name_of(*id), "id": id }))
            .collect::<Vec<_>>(),
    })
}

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

// —— 审计（docs/admin-api.md §3 四件套之魂） ——

/// 管理写操作审计条目。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct AuditEntry {
    /// 操作时间（unix 秒）。
    pub at: u64,
    /// 动作（如 `admin.kick` / `admin.ban`）。
    pub action: String,
    /// 目标（人类可读，如 `room:r1 user:42`）。
    pub target: String,
    /// 结果摘要（`ok` / 错误信息）。
    pub result: String,
}

/// runtime-config 回滚状态（阶段 3，docs/admin-api.md §3-3）：保留"上一份"全量
/// 快照（gooophira rollback 概念，v1 做"上一份"不做版本栈）。组合根注入。
#[derive(Default)]
pub struct AdminConfigState {
    /// 上次 `POST /admin/config` 成功前的生效配置；rollback 消费后清空。
    last: Mutex<Option<Arc<RoomConfig>>>,
}

impl AdminConfigState {
    /// 新建（空）。返回 Arc 便于组合根注入。
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// 保存"本次更新前的配置"（覆盖）。
    pub fn stash(&self, cfg: Arc<RoomConfig>) {
        *self
            .last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cfg);
    }

    /// 取出上一份（rollback 用；取走即清空——只能回切一次）。
    pub fn take_last(&self) -> Option<Arc<RoomConfig>> {
        self.last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

/// 审计环（有界 256 条，内存可控；组合根注入 ConnContext.admin_audit）。
///
/// 持久化（组合根 `storage` 模块契约）：`file` = 归档 JSONL——`record` 同步 append
/// （fail soft：写失败仅日志，环照记）；启动时 `new_with_file` 回填尾部至多 256 行，
/// 重启不丢历史。环 = 内存检索面（`GET /admin/audit`），文件 = 归档面。
#[derive(Default)]
pub struct AuditLog {
    inner: Mutex<VecDeque<AuditEntry>>,
    /// 归档文件（None = 仅内存；测试默认）。
    file: Option<std::path::PathBuf>,
}

impl AuditLog {
    /// 新建（仅内存）。返回 Arc 便于组合根注入。
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// 新建并启用归档（`persist_dir` 下 `audit.jsonl`；启动回填至多 256 行）。
    #[must_use]
    pub fn new_with_file(dir: &std::path::Path) -> Arc<Self> {
        crate::storage::ensure_dir(dir);
        let path = dir.join("audit.jsonl");
        let inner = crate::storage::audit_read_tail(&path, crate::storage::AUDIT_BACKFILL_MAX)
            .into_iter()
            .filter_map(|line| serde_json::from_str::<AuditEntry>(&line).ok())
            .collect::<VecDeque<_>>();
        Arc::new(Self {
            inner: Mutex::new(inner),
            file: Some(path),
        })
    }

    /// 记录一条（超限丢最旧；同步追加到归档文件，失败仅日志）。
    pub fn record(&self, action: &str, target: &str, result: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.len() >= 256 {
            inner.pop_front();
        }
        let entry = AuditEntry {
            at: now,
            action: action.to_owned(),
            target: target.to_owned(),
            result: result.to_owned(),
        };
        inner.push_back(entry.clone());
        // 归档：仅在找到文件后追加（首次失败不重试刷日志，下条操作自然重试）
        if let (Some(path), Ok(line)) = (&self.file, serde_json::to_string(&entry))
            && let Err(e) = crate::storage::audit_append(path, &line)
        {
            tracing::error!("audit archive append {path:?}: {e}");
        }
    }

    /// 快照（时间倒序——最新在前，面板按此渲染）。
    pub fn snapshot(&self) -> Vec<AuditEntry> {
        let mut list: Vec<_> = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect();
        list.sort_by(|a, b| b.at.cmp(&a.at).then_with(|| a.action.cmp(&b.action)));
        list
    }
}

// —— 响应与认证 ——

/// 解析请求头里的字段（小写键，去空白）。
fn header_of(head: &str, key: &str) -> Option<String> {
    head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        if k.trim().eq_ignore_ascii_case(key) {
            Some(v.trim().to_owned())
        } else {
            None
        }
    })
}

/// 管理面认证：`Authorization: Bearer <token>` 且 `admin_token` 已配置且相等。
/// token 未配置 = 管理面禁用（一律拒绝）。回话：`Ok(())` 或拒绝原因。
fn authorize(ctx: &ConnContext, head: &str) -> Result<(), (&'static str, &'static str)> {
    let Some(expect) = &ctx.admin_token else {
        return Err((
            "403 Forbidden",
            "admin api disabled (no admin_token configured)",
        ));
    };
    match header_of(head, "authorization") {
        Some(v) if v == format!("Bearer {expect}") => Ok(()),
        Some(_) => Err(("401 Unauthorized", "invalid token")),
        None => Err(("401 Unauthorized", "missing bearer token")),
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

/// 单请求处理：读头（≤4KiB）+ 读体（≤1KiB，POST）→ 认证 → 路由 → 手写响应。
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
    let request_line = text.lines().next().unwrap_or("");
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or("GET");
    let target = parts.next().unwrap_or("/");
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p, q),
        None => (target, ""),
    };
    let query = query.to_ascii_lowercase();

    // POST 体：提取头内已读部分 + 按 Content-Length 补读（管理 JSON ≤1KiB，超限拒读
    // ——防御哲学同 §10.4）。头体同包时 body 已在 head 尾部，不能丢。
    let body = if method == "POST" {
        let clen: usize = header_of(&text, "content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        if clen > 1024 {
            let resp = http_resp("413 Payload Too Large", "body too large", "text/plain");
            stream.write_all(resp.as_bytes()).await?;
            return Ok(());
        }
        let body_start = head
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map_or(0, |i| i + 4);
        let mut body = head[body_start..].to_vec();
        if body.len() < clen {
            let mut rest = vec![0u8; clen - body.len()];
            stream.read_exact(&mut rest).await?;
            body.extend_from_slice(&rest);
        }
        body
    } else {
        Vec::new()
    };
    let body_json: Option<serde_json::Value> = if body.is_empty() {
        None
    } else {
        serde_json::from_slice(&body).ok()
    };

    // —— 路由 ——
    let (status, resp_body, ctype) =
        route(method, path, &query, &text, body_json.as_ref(), &ctx).await;

    let resp = http_resp(status, &resp_body, ctype);
    stream.write_all(resp.as_bytes()).await?;
    info!("http {method} {path} from {addr} -> {status}");
    Ok(())
}

fn http_resp(status: &str, body: &str, ctype: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

async fn route(
    method: &str,
    path: &str,
    query: &str,
    head: &str,
    body: Option<&serde_json::Value>,
    ctx: &ConnContext,
) -> (&'static str, String, &'static str) {
    if !path.starts_with("/admin/") {
        // 公共面（无需认证）：服务盘点/公开房间列表/测活
        return route_public(method, path, ctx).await;
    }
    // 管理面：先认证（token 未配置 = 整体禁用 403）
    if let Err((status, msg)) = authorize(ctx, head) {
        return (status, msg.to_owned(), "text/plain");
    }
    if method == "GET" {
        route_admin_read(path, query, ctx).await
    } else {
        route_admin_write(method, path, body, ctx).await
    }
}

/// 公共面（`/` `/rooms` `/healthz`）。
async fn route_public(
    method: &str,
    path: &str,
    ctx: &ConnContext,
) -> (&'static str, String, &'static str) {
    match (method, path) {
        ("GET", "/") => (
            "200 OK",
            serde_json::json!({
                "service": "r0semi-mp",
                "endpoints": ["/rooms", "/healthz", "/admin/rooms", "/admin/rooms/{id}", "/admin/users", "/admin/metrics", "/admin/audit", "POST /admin/rooms/{id}/kick", "POST /admin/rooms/{id}/broadcast", "POST /admin/users/{id}/ban", "POST /admin/users/{id}/disconnect"],
            })
            .to_string(),
            "application/json; charset=utf-8",
        ),
        ("GET", "/rooms") => {
            // 标准格式定稿（2026-08）：{rooms:[…], total:服务器在线玩家总数（含未进房）}
            let rooms = ctx.room_list.snapshot().await;
            let rooms: Vec<_> = rooms.iter().map(|r| room_json(r, &ctx.registry)).collect();
            let total = ctx.sink.online().await.len();
            (
                "200 OK",
                serde_json::json!({ "rooms": rooms, "total": total }).to_string(),
                "application/json; charset=utf-8",
            )
        }
        ("GET", "/healthz") => (
            "200 OK",
            serde_json::json!({
                "status": "ok",
                "version": env!("CARGO_PKG_VERSION"),
                "uptime_s": uptime_s(),
                "connections": ctx.sink.conn_count().await,
                "rooms": ctx.room_list.snapshot().await.len(),
                "internal_errors": ctx.bus.metrics().internal_errors(),
                "metrics": metrics_json(ctx),
            })
            .to_string(),
            "application/json; charset=utf-8",
        ),
        _ => ("404 Not Found", "not found".to_owned(), "text/plain"),
    }
}

/// 管理读面（认证后）：metrics / rooms(过滤) / rooms/{id} / users / audit。
async fn route_admin_read(
    path: &str,
    query: &str,
    ctx: &ConnContext,
) -> (&'static str, String, &'static str) {
    match path {
        "/admin/metrics" => (
            "200 OK",
            serde_json::json!({
                "internal_errors": ctx.bus.metrics().internal_errors(),
                "metrics": metrics_json(ctx),
            })
            .to_string(),
            "application/json; charset=utf-8",
        ),
        "/admin/rooms" => {
            let rooms = ctx.room_list.snapshot().await;
            let rooms = if let Some(st) = query.strip_prefix("state=") {
                // 查询值本地归一（docs 承诺"GET 值不区分大小写"）：http_serve 已对整个
                // query 小写（:288），此处再归一是兜底自洽——过滤点不依赖上游顺序
                let want = st.to_ascii_lowercase();
                rooms
                    .into_iter()
                    .filter(|r| r.state.contains(want.as_str()))
                    .collect::<Vec<_>>()
            } else {
                rooms
            };
            let rooms: Vec<_> = rooms.iter().map(|r| room_json(r, &ctx.registry)).collect();
            (
                "200 OK",
                serde_json::to_string(&rooms).unwrap_or_else(|_| "[]".to_owned()),
                "application/json; charset=utf-8",
            )
        }
        "/admin/users" => {
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
            (
                "200 OK",
                serde_json::to_string(&users).unwrap_or_else(|_| "[]".to_owned()),
                "application/json; charset=utf-8",
            )
        }
        "/admin/audit" => (
            "200 OK",
            serde_json::to_string(&ctx.admin_audit.snapshot()).unwrap_or_else(|_| "[]".to_owned()),
            "application/json; charset=utf-8",
        ),
        "/admin/anticheat" => (
            "200 OK",
            serde_json::json!({
                "fingerprints": ctx.admin_anticheat.fingerprint_len(),
                "rejects": ctx.admin_anticheat.rejects_snapshot(),
                "flags": ctx.admin_anticheat.flags_snapshot(),
            })
            .to_string(),
            "application/json; charset=utf-8",
        ),
        path if path.starts_with("/admin/rooms/") => {
            let id = &path["/admin/rooms/".len()..];
            let rooms = ctx.room_list.snapshot().await;
            match rooms.iter().find(|r| r.id == id) {
                Some(room) => (
                    "200 OK",
                    room_json(room, &ctx.registry).to_string(),
                    "application/json; charset=utf-8",
                ),
                None => ("404 Not Found", "room not found".to_owned(), "text/plain"),
            }
        }
        _ => ("404 Not Found", "not found".to_owned(), "text/plain"),
    }
}

/// 管理写面（认证后，阶段 2）：系统命令族 + 审计。写操作无论成败都落审计。
#[allow(clippy::too_many_lines)] // 端点逐条可审计，拆分反而破坏一目了然
async fn route_admin_write(
    method: &str,
    path: &str,
    body: Option<&serde_json::Value>,
    ctx: &ConnContext,
) -> (&'static str, String, &'static str) {
    match (method, path) {
        (m, path)
            if path.starts_with("/admin/rooms/") && path.ends_with("/kick") && m == "POST" =>
        {
            let room_id = path["/admin/rooms/".len()..path.len() - "/kick".len()].to_owned();
            let Some(user_id) = body
                .and_then(|b| b["user_id"].as_i64())
                .and_then(|v| i32::try_from(v).ok())
            else {
                return (
                    "400 Bad Request",
                    "missing user_id".to_owned(),
                    "text/plain",
                );
            };
            let result = admin_kick(ctx, &room_id, user_id).await;
            ctx.admin_audit.record(
                "admin.kick",
                &format!("room:{room_id} user:{user_id}"),
                &result.1,
            );
            (result.0, result.1, "application/json; charset=utf-8")
        }
        (m, path)
            if path.starts_with("/admin/rooms/") && path.ends_with("/broadcast") && m == "POST" =>
        {
            let room_id = path["/admin/rooms/".len()..path.len() - "/broadcast".len()].to_owned();
            let Some(content) = body.and_then(|b| b["content"].as_str()) else {
                return (
                    "400 Bad Request",
                    "missing content".to_owned(),
                    "text/plain",
                );
            };
            // 截断至协议 Chat 上限类似量级（防超长公告）；user=0 系统约定
            let content = content.chars().take(200).collect::<String>();
            let result = admin_broadcast(ctx, &room_id, &content).await;
            ctx.admin_audit
                .record("admin.broadcast", &format!("room:{room_id}"), &result.1);
            (result.0, result.1, "application/json; charset=utf-8")
        }
        (m, path) if path.starts_with("/admin/users/") && path.ends_with("/ban") && m == "POST" => {
            let user_id = &path["/admin/users/".len()..path.len() - "/ban".len()];
            let Ok(user_id) = user_id.parse::<i32>() else {
                return ("400 Bad Request", "bad user_id".to_owned(), "text/plain");
            };
            let result = admin_ban(ctx, user_id).await;
            ctx.admin_audit
                .record("admin.ban", &format!("user:{user_id}"), &result.1);
            (result.0, result.1, "application/json; charset=utf-8")
        }
        (m, path)
            if path.starts_with("/admin/users/")
                && path.ends_with("/disconnect")
                && m == "POST" =>
        {
            let user_id = &path["/admin/users/".len()..path.len() - "/disconnect".len()];
            let Ok(user_id) = user_id.parse::<i32>() else {
                return ("400 Bad Request", "bad user_id".to_owned(), "text/plain");
            };
            let result = admin_disconnect(ctx, user_id).await;
            ctx.admin_audit
                .record("admin.disconnect", &format!("user:{user_id}"), &result.1);
            (result.0, result.1, "application/json; charset=utf-8")
        }
        (m, path) if path == "/admin/config" && m == "POST" => {
            let Some(monitors) = body
                .and_then(|b| b["rooms"].as_object().and_then(|r| r.get("monitors")))
                .and_then(|m| m.as_array())
            else {
                return (
                    "400 Bad Request",
                    "expected {\"rooms\":{\"monitors\":[ids]}}".to_owned(),
                    "text/plain",
                );
            };
            let Ok(monitors) = monitors
                .iter()
                .map(|v| v.as_i64().and_then(|v| i32::try_from(v).ok()).ok_or(()))
                .collect::<Result<Vec<_>, _>>()
            else {
                return ("400 Bad Request", "bad monitors".to_owned(), "text/plain");
            };
            let result = admin_set_config(ctx, RoomConfig { monitors }).await;
            ctx.admin_audit
                .record("admin.config", "rooms.monitors", &result.1);
            if result.0 == "200 OK" {
                // 持久化（组合根 storage）：旧 current → last，新原文 → current
                if let Some(b) = body {
                    ctx.config_store.record_success(b);
                }
            }
            (result.0, result.1, "application/json; charset=utf-8")
        }
        (m, path) if path == "/admin/config/rollback" && m == "POST" => {
            let result = admin_rollback_config(ctx).await;
            ctx.admin_audit
                .record("admin.config.rollback", "", &result.1);
            if result.0 == "200 OK" {
                // 持久化：last → current；清 last（与内存 take_last 取走即清空一致）
                ctx.config_store.record_rollback();
            }
            (result.0, result.1, "application/json; charset=utf-8")
        }
        (m, path) if path == "/admin/observers" && m == "POST" => {
            let Some(kind) = body.and_then(|b| b["kind"].as_str()) else {
                return ("400 Bad Request", "missing kind".to_owned(), "text/plain");
            };
            let Some(op) = body.and_then(|b| b["op"].as_str()) else {
                return (
                    "400 Bad Request",
                    "missing op (add|remove)".to_owned(),
                    "text/plain",
                );
            };
            let result = admin_toggle_observer(ctx, kind, op);
            ctx.admin_audit.record(
                &format!("admin.observer.{op}"),
                &format!("kind:{kind}"),
                &result.1,
            );
            (result.0, result.1, "application/json; charset=utf-8")
        }
        _ => (
            "405 Method Not Allowed",
            "unsupported admin endpoint".to_owned(),
            "text/plain",
        ),
    }
}

/// 踢人：翻译成 `RoomCommand::AdminKick` 系统命令（房间 actor 串行通道执行，无需锁）。
async fn admin_kick(ctx: &ConnContext, room_id: &str, user_id: i32) -> (&'static str, String) {
    let Ok(room_id) = phira_api::RoomId::new(room_id.to_owned()) else {
        return ("400 Bad Request", "bad room id".to_owned());
    };
    match ctx
        .bus
        .dispatch_system(room_id, RoomCommand::AdminKick { user_id })
        .await
    {
        Ok(RoomResponse::Ok) => ("200 OK", serde_json::json!({"ok": true}).to_string()),
        Ok(RoomResponse::Failure(RoomError::Business {
            code: RoomErrorCode::NotInRoom,
            ..
        })) => (
            "409 Conflict",
            serde_json::json!({"ok": false, "error": "not_in_room"}).to_string(),
        ),
        Ok(_) => (
            "500 Internal Server Error",
            serde_json::json!({"ok": false, "error": "unexpected"}).to_string(),
        ),
        Err(e) => (
            "404 Not Found",
            serde_json::json!({"ok": false, "error": format!("{e}")}).to_string(),
        ),
    }
}

/// 公告：`RoomCommand::AdminBroadcast` 系统命令（房内系统 Chat，user=0）。
async fn admin_broadcast(
    ctx: &ConnContext,
    room_id: &str,
    content: &str,
) -> (&'static str, String) {
    let Ok(room_id) = phira_api::RoomId::new(room_id.to_owned()) else {
        return ("400 Bad Request", "bad room id".to_owned());
    };
    match ctx
        .bus
        .dispatch_system(
            room_id,
            RoomCommand::AdminBroadcast {
                content: content.to_owned(),
            },
        )
        .await
    {
        Ok(RoomResponse::Ok) => ("200 OK", serde_json::json!({"ok": true}).to_string()),
        Err(e) => (
            "404 Not Found",
            serde_json::json!({"ok": false, "error": format!("{e}")}).to_string(),
        ),
        _ => (
            "500 Internal Server Error",
            serde_json::json!({"ok": false, "error": "unexpected"}).to_string(),
        ),
    }
}

/// 封禁（阶段 2 语义）：踢出当前房间（若有）+ 断 TCP（kicker force_close）+ 审计。
/// 注：重连后可再进——真正的名单拦截依赖 P2 Moderator + 阶段 4 鉴权拦截（文档 §1/§6）。
async fn admin_ban(ctx: &ConnContext, user_id: i32) -> (&'static str, String) {
    let mut parts = Vec::new();
    // 真 ban 语义第 1 步：进入封禁名单（其后的入房类命令被 intercept 拒绝）——
    // 名单本体由组合根单例持有（热插拔挂载与否不影响自名单生效？不：名单拦截依赖
    // BanObserver 被挂进 bus；组合根默认挂载，管理面可热卸载——文档注明）。
    ctx.admin_ban_observer.ban(user_id);
    parts.push("banned".to_owned());
    if let Some(room_id) = ctx.bus.room_of(user_id).await {
        match ctx
            .bus
            .dispatch_system(room_id.clone(), RoomCommand::AdminKick { user_id })
            .await
        {
            Ok(RoomResponse::Ok) => parts.push("kicked".to_owned()),
            other => parts.push(format!("kick:{other:?}")),
        }
    } else {
        parts.push("not_in_room".to_owned());
    }
    if ctx.sink.force_disconnect(user_id).await {
        parts.push("disconnected".to_owned());
    } else {
        parts.push("offline".to_owned());
    }
    (
        "200 OK",
        serde_json::json!({ "ok": true, "actions": parts }).to_string(),
    )
}

/// 断连：仅 TCP 断开（连接收尾流程发生命周期事实），不出房。
async fn admin_disconnect(ctx: &ConnContext, user_id: i32) -> (&'static str, String) {
    if ctx.sink.force_disconnect(user_id).await {
        ("200 OK", serde_json::json!({"ok": true}).to_string())
    } else {
        (
            "404 Not Found",
            serde_json::json!({"ok": false, "error": "user offline"}).to_string(),
        )
    }
}

/// runtime-config 热更（阶段 3）：先存"上一份"（bus 当前生效配置）→ 广播替换。
async fn admin_set_config(ctx: &ConnContext, config: RoomConfig) -> (&'static str, String) {
    let current = ctx.bus.current_config().await;
    ctx.admin_config.stash(current);
    ctx.bus.update_config(Arc::new(config)).await;
    ("200 OK", serde_json::json!({"ok": true}).to_string())
}

/// runtime-config 一步回切（阶段 3）：恢复"上一份"，取走即清空（只可回切一次）。
async fn admin_rollback_config(ctx: &ConnContext) -> (&'static str, String) {
    match ctx.admin_config.take_last() {
        Some(last) => {
            ctx.bus.update_config(last).await;
            ("200 OK", serde_json::json!({"ok": true}).to_string())
        }
        None => (
            "409 Conflict",
            serde_json::json!({"ok": false, "error": "nothing to rollback"}).to_string(),
        ),
    }
}

/// observer 热插拔（阶段 3，§7.3 预留兑现）：`kind` 目前支持 `ban`（封禁名单观察者，
/// 组合根单例挂载/卸载——卸载后现有名单不生效，重挂恢复）。其它 kind → 400。
fn admin_toggle_observer(ctx: &ConnContext, kind: &str, op: &str) -> (&'static str, String) {
    let moderator: Arc<dyn phira_api::Moderator> = match kind {
        "ban" => Arc::clone(&ctx.admin_ban_observer) as Arc<dyn phira_api::Moderator>,
        "anticheat" => Arc::clone(&ctx.admin_anticheat) as Arc<dyn phira_api::Moderator>,
        _ => {
            return (
                "400 Bad Request",
                serde_json::json!({"ok": false, "error": "unsupported kind"}).to_string(),
            );
        }
    };
    match op {
        "add" => {
            ctx.bus.add_moderator(moderator);
            (
                "200 OK",
                serde_json::json!({"ok": true, "banned": ctx.admin_ban_observer.banned_users()})
                    .to_string(),
            )
        }
        "remove" => {
            let removed = ctx.bus.remove_moderator(kind);
            (
                "200 OK",
                serde_json::json!({"ok": removed, "banned": ctx.admin_ban_observer.banned_users()})
                    .to_string(),
            )
        }
        _ => (
            "400 Bad Request",
            serde_json::json!({"ok": false, "error": "unsupported op"}).to_string(),
        ),
    }
}
