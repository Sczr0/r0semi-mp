//! 生产实现：HTTP 回源客户端 + 随机源 + 鉴权（§4.5 组合根内，原则 5）。
//!
//! v1 生产实现直接放组合根，第二实现出现再抽独立 crate（原则 5 对自己生效，§4.5）。
//!
//! 传输层：手写 HTTP/1.1 GET 跑在两种传输上（§10.1 / §10.3 红线）——
//! - `http://`：明文 TCP（本地 mock API，§9 Oracle 环境）
//! - `https://`：**rustls 单栈**（ring provider + webpki-roots 根，§4.9-7 / §10.3），
//!   生产基址 `https://phira.5wyxi.com`（原版 HOST 硬编码，阶段 4 解锁）。

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use phira_api::{
    ApiClient, ApiError, AuthError, AuthHandler, Chart, RandomSource, Record, UserIdentity,
};
use rand::Rng;
use rustls::pki_types::ServerName;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

/// TLS 客户端配置（webpki-roots 根 + ring provider，进程内单例，§10.3）。
fn tls_config() -> Arc<rustls::ClientConfig> {
    static CONFIG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    Arc::clone(CONFIG.get_or_init(|| {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Arc::new(config)
    }))
}

/// 回源 HTTP 请求超时（§4.4：每次请求自带超时，评审 §8 三）。
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

/// 响应体上限（C3 技术债清偿，2026-08）：官方 API 的 /me /chart /record 响应均为
/// 小 JSON（KB 级）；16MiB 上限 = 恶意/异常上游不能把回源连接变成内存放大器
/// （对照协议侧 PRE_AUTH→2MiB 同一防御哲学，§10.4）。
const MAX_BODY_LEN: usize = 16 * 1024 * 1024;

/// 回源 HTTP 客户端（§4.9-6 / §6.5-15）。
///
/// 手写 HTTP/1.1 GET（§10.1，约两百行，最贴合内存目标）。
pub struct HttpApiClient {
    base: String,
    /// 请求超时（yml `http_timeout`，默认 5s）。
    timeout: Duration,
}

impl HttpApiClient {
    /// 构造（默认 5s 超时）。`base` = 官方 API 基地址（Config::api_base）。
    #[must_use]
    pub const fn new(base: String) -> Self {
        Self::new_with_timeout(base, HTTP_TIMEOUT)
    }

    /// 构造并指定请求超时（yml `http_timeout` 接线点）。
    #[must_use]
    pub const fn new_with_timeout(base: String, timeout: Duration) -> Self {
        Self { base, timeout }
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, ApiError> {
        let bytes = http_get_with_timeout(&self.base, path, None, self.timeout).await?;
        serde_json::from_slice(&bytes).map_err(|e| ApiError::Internal {
            msg: format!("invalid JSON from {path}: {e}"),
        })
    }
}

/// `/chart/{id}` 响应 DTO（字段名 = 官方 API snake_case，原版 serde 默认）。
#[derive(Deserialize)]
struct ChartDto {
    id: i32,
    name: String,
}

/// `/record/{id}` 响应 DTO。
#[derive(Deserialize)]
struct RecordDto {
    id: i32,
    player: i32,
    /// P1 反作弊数据口：上游缺省时 `None`（serde 白名单不解 req 缺失字段）。
    chart: Option<i32>,
    score: i32,
    perfect: i32,
    good: i32,
    bad: i32,
    miss: i32,
    max_combo: i32,
    accuracy: f32,
    full_combo: bool,
    std: f32,
    std_score: f32,
}

/// `/me` 响应 DTO（原版字段 `language`）。
#[derive(Deserialize)]
struct MeDto {
    id: i32,
    name: String,
    language: String,
}

#[async_trait::async_trait]
impl ApiClient for HttpApiClient {
    async fn fetch_chart(&self, id: i32) -> Result<Chart, ApiError> {
        let dto: ChartDto = self.get_json(&format!("/chart/{id}")).await?;
        Ok(Chart {
            id: dto.id,
            name: dto.name,
        })
    }

    async fn fetch_record(&self, id: i32) -> Result<Record, ApiError> {
        let dto: RecordDto = self.get_json(&format!("/record/{id}")).await?;
        Ok(Record {
            id: dto.id,
            player: dto.player,
            chart: dto.chart,
            score: dto.score,
            perfect: dto.perfect,
            good: dto.good,
            bad: dto.bad,
            miss: dto.miss,
            max_combo: dto.max_combo,
            accuracy: dto.accuracy,
            full_combo: dto.full_combo,
            std: dto.std,
            std_score: dto.std_score,
        })
    }
}

/// 鉴权处理器（§4.4 / §6.5-14）。
pub struct HttpAuth {
    base: String,
    /// 请求超时（yml `http_timeout`，默认 5s）。
    timeout: Duration,
}

impl HttpAuth {
    /// 构造（默认 5s 超时）。`base` = 官方 API 基地址。
    #[must_use]
    pub const fn new(base: String) -> Self {
        Self::new_with_timeout(base, HTTP_TIMEOUT)
    }

    /// 构造并指定请求超时（yml `http_timeout` 接线点）。
    #[must_use]
    pub const fn new_with_timeout(base: String, timeout: Duration) -> Self {
        Self { base, timeout }
    }
}

#[async_trait::async_trait]
impl AuthHandler for HttpAuth {
    async fn authenticate(&self, token: &str) -> Result<UserIdentity, AuthError> {
        let bytes = http_get_with_timeout(&self.base, "/me", Some(token), self.timeout)
            .await
            .map_err(|e| match e {
                ApiError::Internal { msg } => AuthError::Internal { msg },
            })?;
        let me: MeDto = serde_json::from_slice(&bytes).map_err(|e| AuthError::Internal {
            msg: format!("invalid JSON from /me: {e}"),
        })?;
        Ok(UserIdentity {
            user_id: me.id,
            name: me.name,
            lang: me.language,
        })
    }
}

/// 生产随机源（§4.9-6）：房主随机选择。
#[derive(Default)]
pub struct ThreadRngSource;

impl RandomSource for ThreadRngSource {
    fn pick_index(&self, len: usize) -> Option<usize> {
        if len == 0 {
            None
        } else {
            Some(rand::rng().random_range(0..len))
        }
    }
}

/// 传输层（§10.3）：明文 TCP 或 rustls TLS。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    /// 明文（本地 mock API，§9 Oracle 环境）。
    Plain,
    /// TLS（生产基址 `https://phira.5wyxi.com`，阶段 4 解锁）。
    Tls,
}

/// 解析基址：`scheme://host[:port]`。
fn parse_base(base: &str) -> Result<(Transport, String, u16), ApiError> {
    let (transport, rest) = if let Some(rest) = base.strip_prefix("https://") {
        (Transport::Tls, rest)
    } else if let Some(rest) = base.strip_prefix("http://") {
        (Transport::Plain, rest)
    } else {
        return Err(ApiError::Internal {
            msg: format!("unsupported base url scheme (need http:// or https://): {base}"),
        });
    };
    let default_port = match transport {
        Transport::Tls => 443,
        Transport::Plain => 80,
    };
    let (host, port) = match rest.split_once(':') {
        Some((h, p)) => (
            h.to_owned(),
            p.parse::<u16>().map_err(|_| ApiError::Internal {
                msg: format!("invalid port in base url: {base}"),
            })?,
        ),
        None => (rest.to_owned(), default_port),
    };
    Ok((transport, host, port))
}

/// 带自定义超时的请求（yml `http_timeout` 接线点）。
async fn http_get_with_timeout(
    base: &str,
    path: &str,
    bearer: Option<&str>,
    timeout: Duration,
) -> Result<Vec<u8>, ApiError> {
    http_get_with_tls_timeout(base, path, bearer, None, timeout).await
}
/// 请求入口（测试可注入自定义 TLS 配置，`None` = 生产 webpki-roots 验证）。
///
/// `pub` 仅对集成测试暴露（组合根内部细节，doc(hidden)）。
#[doc(hidden)]
pub async fn http_get_with_tls(
    base: &str,
    path: &str,
    bearer: Option<&str>,
    tls: Option<Arc<rustls::ClientConfig>>,
) -> Result<Vec<u8>, ApiError> {
    http_get_with_tls_timeout(base, path, bearer, tls, HTTP_TIMEOUT).await
}

/// 同 host 重定向跟随上限（含首跳共 3 跳）：防重定向环/链式注入把请求打向任意上游。
const MAX_REDIRECTS: usize = 3;

/// 内部实现（timeout 可注入）。
///
/// 2026-08（C3 清偿）：30x 从"显式拒绝"升级为**有限跟随**——同 host（含端口）
/// 重定向最多 3 跳，跨域拒绝。安全边界：
/// - 白名单 = base 的 host，跳转目标必须与其一致（含端口），否则 `ApiError`；
/// - bearer token 只随同 host 请求重发，**绝不离开信任域**（跨域 302 落"untrusted host"）；
/// - Location 缺失 / 跳数耗尽 / 非 200 终态均显式报错（可诊断，不静默）。
async fn http_get_with_tls_timeout(
    base: &str,
    path: &str,
    bearer: Option<&str>,
    tls: Option<Arc<rustls::ClientConfig>>,
    timeout: Duration,
) -> Result<Vec<u8>, ApiError> {
    let (_bt, base_host, base_port) = parse_base(base)?;
    let mut path = path.to_owned();
    for hop in 0..=MAX_REDIRECTS {
        let payload = http_get_one(base, &path, bearer, tls.clone(), timeout).await?;
        match payload.status {
            200 => return Ok(payload.body),
            300..=399 => {
                let location = payload.location.ok_or_else(|| ApiError::Internal {
                    msg: format!("HTTP {} from {path} without Location", payload.status),
                })?;
                let next = resolve_same_host(&location, &base_host, base_port)?;
                debug!(
                    "redirect hop {hop}: {path} -> {next} (HTTP {})",
                    payload.status
                );
                path = next;
            }
            other => {
                return Err(ApiError::Internal {
                    msg: format!("HTTP {other} from {path}"),
                });
            }
        }
    }
    Err(ApiError::Internal {
        msg: format!("too many redirects (>={} hops): {path}", MAX_REDIRECTS + 1),
    })
}

/// 重定向目标解析：相对路径直接接受；绝对 URL 校验 host+端口 == base（白名单，
/// 跨域拒绝）；返回下一跳相对 path（重发仍走 base host，token 不外泄）。
fn resolve_same_host(location: &str, base_host: &str, base_port: u16) -> Result<String, ApiError> {
    let location = location.trim();
    let Some(rest) = location
        .strip_prefix("http://")
        .or_else(|| location.strip_prefix("https://"))
    else {
        // 相对 Location：同一信任域，直接接受
        let p = if location.starts_with('/') {
            location.to_owned()
        } else {
            format!("/{location}")
        };
        return Ok(p);
    };
    // 绝对 URL：`host[:port][/path...]`
    let (host_part, path) = match rest.split_once('/') {
        Some((h, p)) => (h, format!("/{p}")),
        None => (rest, "/".to_owned()),
    };
    let (host, port) = match host_part.split_once(':') {
        Some((h, p)) => (h, p.parse::<u16>().ok()),
        None => (host_part, None),
    };
    if host != base_host {
        return Err(ApiError::Internal {
            msg: format!("redirect to untrusted host {host:?} (base {base_host:?})"),
        });
    }
    if let Some(port) = port
        && port != base_port
    {
        return Err(ApiError::Internal {
            msg: format!("redirect to untrusted port {port} (base {base_port})"),
        });
    }
    Ok(path)
}

/// 单次 GET 交换（不跟随）：返回状态 + 可选 Location + body。
async fn http_get_one(
    base: &str,
    path: &str,
    bearer: Option<&str>,
    tls: Option<Arc<rustls::ClientConfig>>,
    timeout: Duration,
) -> Result<Exchange, ApiError> {
    let (transport, host, port) = parse_base(base)?;
    // Never Trust the Client（2026-08）：token 是客户端可控数据（协议 Varchar 允许 CR/LF），
    // 直接拼进请求头可注入任意头——拒绝而非转义（fail closed）。
    if bearer.is_some_and(|t| t.contains(['\r', '\n'])) {
        return Err(ApiError::Internal {
            msg: "invalid bearer token (CR/LF)".to_owned(),
        });
    }
    let addr = format!("{host}:{port}");
    let socket = TcpStream::connect(&addr)
        .await
        .map_err(|e| ApiError::Internal {
            msg: format!("connect {addr}: {e}"),
        })?;
    let _ = socket.set_nodelay(true);

    match transport {
        Transport::Plain => http_exchange(socket, &host, path, bearer, timeout).await,
        Transport::Tls => {
            // rustls 握手（SNI = host，证书链验证 = webpki-roots / 测试注入配置）
            let server_name =
                ServerName::try_from(host.clone()).map_err(|e| ApiError::Internal {
                    msg: format!("invalid hostname for TLS SNI: {host}: {e}"),
                })?;
            let connector = tokio_rustls::TlsConnector::from(tls.unwrap_or_else(tls_config));
            let tls_stream =
                connector
                    .connect(server_name, socket)
                    .await
                    .map_err(|e| ApiError::Internal {
                        msg: format!("TLS handshake with {host}: {e}"),
                    })?;
            http_exchange(tls_stream, &host, path, bearer, timeout).await
        }
    }
}

/// 一次 HTTP 交换的结果（状态 + 可选 Location + body；30x 跟随决策在上层）。
struct Exchange {
    status: u16,
    location: Option<String>,
    body: Vec<u8>,
}

/// 传输无关的 HTTP/1.1 GET 交换（`S` = 明文 TcpStream 或 TlsStream<TcpStream>）。
async fn http_exchange<S>(
    stream: S,
    host: &str,
    path: &str,
    bearer: Option<&str>,
    timeout: Duration,
) -> Result<Exchange, ApiError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut stream = stream;
    let (mut read, mut write) = tokio::io::split(&mut stream);

    // 请求行 + 头（Connection: close——回源低频，每次新建连接简单可靠）
    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nAccept: application/json\r\nConnection: close\r\n"
    );
    if let Some(token) = bearer {
        use std::fmt::Write as _;
        let _ = write!(req, "Authorization: Bearer {token}\r\n");
    }
    req.push_str("\r\n");
    debug!("http GET {path}");

    let result = tokio::time::timeout(timeout, async {
        write
            .write_all(req.as_bytes())
            .await
            .map_err(|e| ApiError::Internal {
                msg: format!("write request: {e}"),
            })?;

        // 读响应头（≤64KiB 防护）
        let mut head = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let n = read.read(&mut buf).await.map_err(|e| ApiError::Internal {
                msg: format!("read headers: {e}"),
            })?;
            if n == 0 {
                return Err(ApiError::Internal {
                    msg: "connection closed before headers".to_owned(),
                });
            }
            head.extend_from_slice(&buf[..n]);
            if head.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
            if head.len() > 64 * 1024 {
                return Err(ApiError::Internal {
                    msg: "response headers too large".to_owned(),
                });
            }
        }

        // 状态行：`HTTP/1.1 200 OK`；30x 的 Location 供上层决定跟随与否
        let status = parse_status(&head)?;
        let location = if (300..400).contains(&status) {
            location_of(&head)
        } else {
            None
        };

        // Content-Length（缺失则按"无 body"处理；声明超上限直接拒绝——不读不缓冲）
        let len = content_length(&head)?;
        if len > MAX_BODY_LEN {
            return Err(ApiError::Internal {
                msg: format!("response body too large ({len} bytes) from {path}"),
            });
        }

        // body：头内已读部分 + 余量
        let body_start = head
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map_or(0, |i| i + 4);
        let mut body = head[body_start..].to_vec();
        // C3：无 Content-Length 头时的兜底——持续发数据的恶意上游不能让缓冲无界累积。
        // 原实现"缺失=0 后循环读到断开"，上游可用无限流撑爆内存；现按上限报错。
        if body.len() > MAX_BODY_LEN {
            return Err(ApiError::Internal {
                msg: format!("response body too large (>{MAX_BODY_LEN} bytes) from {path}"),
            });
        }
        while body.len() < len {
            let mut buf = [0u8; 2048];
            let n = read.read(&mut buf).await.map_err(|e| ApiError::Internal {
                msg: format!("read body: {e}"),
            })?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&buf[..n]);
            // C3：len 声明值可信（已验 ≤ 上限），但防御上游声明小、实发多——
            // 超上限立刻断，不等 read 自然结束。
            if body.len() > MAX_BODY_LEN {
                return Err(ApiError::Internal {
                    msg: format!("response body exceeded limit while reading from {path}"),
                });
            }
        }
        if body.len() < len {
            return Err(ApiError::Internal {
                msg: "body truncated".to_owned(),
            });
        }
        body.truncate(len);
        Ok(Exchange {
            status,
            location,
            body,
        })
    })
    .await;

    match result {
        Ok(r) => r,
        Err(_) => Err(ApiError::Internal {
            msg: format!("http timeout after {timeout:?}: {path}"),
        }),
    }
}

/// 解析 Location 头（仅 30x 需要；缺失 = None）。
fn location_of(head: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(head);
    text.split("\r\n").find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("location")
            .then(|| value.trim().to_owned())
    })
}

/// 解析状态行取状态码。
fn parse_status(head: &[u8]) -> Result<u16, ApiError> {
    let line = head
        .split(|b| *b == b'\r')
        .next()
        .ok_or_else(|| ApiError::Internal {
            msg: "empty status line".to_owned(),
        })?;
    // `HTTP/1.1 200 OK`
    let mut parts = line.split(|b| *b == b' ');
    let _version = parts.next();
    let code = parts.next().ok_or_else(|| ApiError::Internal {
        msg: "malformed status line".to_owned(),
    })?;
    let text = std::str::from_utf8(code).map_err(|_| ApiError::Internal {
        msg: "malformed status code".to_owned(),
    })?;
    text.parse::<u16>().map_err(|_| ApiError::Internal {
        msg: format!("invalid status code: {text}"),
    })
}

/// 解析 Content-Length 头（缺失 = 0）。
fn content_length(head: &[u8]) -> Result<usize, ApiError> {
    let text = String::from_utf8_lossy(head);
    for line in text.split("\r\n") {
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            return value
                .trim()
                .parse::<usize>()
                .map_err(|_| ApiError::Internal {
                    msg: "invalid content-length".to_owned(),
                });
        }
    }
    Ok(0)
}
