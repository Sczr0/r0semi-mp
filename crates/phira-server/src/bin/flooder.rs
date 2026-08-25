#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! flooder —— Phira 房间服务器压测工具（本地回环优先，可连任意目标）。
//!
//! 攻击形态（`--mode`）：
//! - `random`：纯随机字节流——打解码器外层（第一帧 tag 乱码即拒）
//! - `proto`：**协议形状垃圾**——半帧 / 超长 ULEB / 越界 tag / 截断 / 数组炸弹，
//!   用 `phira-api` 编码器生成"看起来像协议但畸形"的流，打深层解析路径
//! - `reconnect`：连接建立/断开风暴（握手后立即断开，不打数据）
//! - `mixed`：攻击连接 + **合法玩家**并发——验证隔离性（攻击时玩家延迟/可用性）
//!
//! 用法：
//! ```bash
//! # 纯随机垃圾 10 秒（50 连接）
//! cargo run -p phira-server --bin flooder -- --mode random --duration 10 --connections 50
//! # 协议形状垃圾
//! cargo run -p phira-server --bin flooder -- --mode proto --duration 15 --connections 30
//! # 连接风暴
//! cargo run -p phira-server --bin flooder -- --mode reconnect --duration 10 --connections 200
//! # 混合：攻击 + 2 个合法玩家（需服务端可鉴权，如 mock API）
//! cargo run -p phira-server --bin flooder -- --mode mixed --players 2 --duration 15
//! ```
//!
//! 说明：flooder 是客户端工具，直接检测不到服务端 panic——通过"连接全部失败/玩家延迟
//! 爆表"间接推断；服务端进程存活/日志需另行确认。

use std::sync::Arc;
use std::time::{Duration, Instant};

use phira_api::{ClientCommand, Varchar, encode_packet};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

const PROTOCOL_VERSION: u8 = 1;

// —— 确定性伪随机（xorshift，可复现）——

struct Lcg(u64);

impl Lcg {
    const fn new(seed: u64) -> Self {
        Self(if seed == 0 { 1 } else { seed })
    }
    const fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let v = self.next().to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&v[..n]);
        }
    }
    const fn byte(&mut self) -> u8 {
        self.next() as u8
    }
}

// —— 参数解析（手写，零依赖风格）——

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Random,
    Proto,
    Reconnect,
    Mixed,
}

struct Args {
    target: String,
    mode: Mode,
    connections: usize,
    duration: u64,
    seed: u64,
    players: usize,
    json: bool,
}

fn usage() -> ! {
    eprintln!(
        "flooder —— Phira 服务器压测工具\n\
\n\
用法: flooder [选项]\n\
  --target <host:port>    目标地址（默认 127.0.0.1:3939）\n\
  --mode <m>              攻击形态: random | proto | reconnect | mixed（默认 random）\n\
  --connections <n>       攻击并发连接数（默认 50）\n\
  --duration <secs>       持续秒数（默认 10）\n\
  --seed <n>              随机种子（默认 42）\n\
  --players <n>           mixed 模式合法玩家数（默认 2）\n\
  --json                  结构化 JSON 输出\n"
    );
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut a = Args {
        target: "127.0.0.1:3939".to_owned(),
        mode: Mode::Random,
        connections: 50,
        duration: 10,
        seed: 42,
        players: 2,
        json: false,
    };
    let mut args = std::env::args().skip(1);
    while let Some(k) = args.next() {
        match k.as_str() {
            "--target" => a.target = args.next().unwrap_or_else(|| usage()),
            "--mode" => {
                a.mode = match args.next().unwrap_or_else(|| usage()).as_str() {
                    "random" => Mode::Random,
                    "proto" => Mode::Proto,
                    "reconnect" => Mode::Reconnect,
                    "mixed" => Mode::Mixed,
                    other => {
                        eprintln!("未知 mode: {other}");
                        usage();
                    }
                };
            }
            "--connections" => {
                a.connections = args
                    .next()
                    .unwrap_or_else(|| usage())
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--duration" => {
                a.duration = args
                    .next()
                    .unwrap_or_else(|| usage())
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--seed" => {
                a.seed = args
                    .next()
                    .unwrap_or_else(|| usage())
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--players" => {
                a.players = args
                    .next()
                    .unwrap_or_else(|| usage())
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--json" => a.json = true,
            "--help" | "-h" => usage(),
            other => {
                eprintln!("未知参数: {other}");
                usage();
            }
        }
    }
    a
}

// —— 帧编码（ULEB128 长度前缀 + 载荷，§6.1）——

fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 5);
    let mut x = u32::try_from(payload.len()).expect("frame fits u32");
    loop {
        let mut b = (x & 0x7f) as u8;
        x >>= 7;
        if x != 0 {
            b |= 0x80;
        }
        out.push(b);
        if x == 0 {
            break;
        }
    }
    out.extend_from_slice(payload);
    out
}

/// 不终止的 ULEB128（0x80 连发——服务端应超 32 bit 拒绝）。
fn uleb_nonterminating() -> Vec<u8> {
    let mut v = vec![0x80u8; 12];
    v.push(0x00);
    v
}

/// 声明巨大长度的 ULEB 前缀（半帧——服务端 read_exact 挂起等剩余）。
fn frame_half_declared(len: u64) -> Vec<u8> {
    // ULEB128 编码大长度（长度值本身合法编码，但只发 1 字节载荷）
    let mut out = Vec::new();
    let mut x = len;
    loop {
        let mut b = (x & 0x7f) as u8;
        x >>= 7;
        if x != 0 {
            b |= 0x80;
        }
        out.push(b);
        if x == 0 {
            break;
        }
    }
    out.push(0x00); // 1 字节载荷（声明远大于此）
    out
}

/// 协议形状垃圾帧（用 phira-api 编码器生成"半合法"载荷）。
fn proto_frame(rng: &mut Lcg) -> Vec<u8> {
    match rng.next() % 8 {
        // 合法 Ping + 随机尾巴（打到"包内剩余数据"处理）
        0 => {
            let mut p = Vec::new();
            encode_packet(&ClientCommand::Ping, &mut p);
            let tail = 1 + (rng.next() % 64) as usize;
            let mut bytes = vec![0u8; tail];
            rng.fill(&mut bytes);
            p.extend_from_slice(&bytes);
            frame(&p)
        }
        // 合法命令 + 截断（在合法编码的随机偏移截断——半包）
        1 => {
            let cmds = [
                ClientCommand::Ping,
                ClientCommand::Chat {
                    message: Varchar::new("x".repeat(200)).unwrap(),
                },
                ClientCommand::Authenticate {
                    token: Varchar::new("t".repeat(32)).unwrap(),
                },
            ];
            let cmd = &cmds[(rng.next() % 3) as usize];
            let mut p = Vec::new();
            encode_packet(cmd, &mut p);
            if p.is_empty() {
                return frame(&p);
            }
            let cut = (rng.next() as usize) % p.len();
            p.truncate(cut);
            frame(&p)
        }
        // 超长 tag（0xFF——越界应拒绝）
        2 => frame(&[0xFF, rng.byte(), rng.byte(), rng.byte()]),
        // 嵌套数组长度炸弹（数组元素数声明巨大）
        3 => {
            let mut p = Vec::new();
            let mut x = 1u64 << 30;
            loop {
                let mut b = (x & 0x7f) as u8;
                x >>= 7;
                if x != 0 {
                    b |= 0x80;
                }
                p.push(b);
                if x == 0 {
                    break;
                }
            }
            p.extend_from_slice(&[0x00, 0x01, 0x02]);
            frame(&p)
        }
        // 超大字符串长度（Varchar 超限应拒绝）
        4 => {
            let mut p = Vec::new();
            let mut x = 100_000u64;
            loop {
                let mut b = (x & 0x7f) as u8;
                x >>= 7;
                if x != 0 {
                    b |= 0x80;
                }
                p.push(b);
                if x == 0 {
                    break;
                }
            }
            p.extend_from_slice(b"hello");
            frame(&p)
        }
        // 纯随机载荷（正常包帧）
        5 => {
            let n = 1 + (rng.next() % 256) as usize;
            let mut p = vec![0u8; n];
            rng.fill(&mut p);
            frame(&p)
        }
        // 半帧：声明 1MB 只发 1 字节（read_exact 挂起等剩余）
        6 => frame_half_declared(1 << 20),
        // 不终止 ULEB（0x80 连发——超 32 bit 应拒绝）
        _ => uleb_nonterminating(),
    }
}

// —— 统计 ——

#[derive(Default, Clone)]
struct Stats {
    sent_bytes: u64,
    attempts: u64,
}

// —— 攻击 worker ——

async fn flood_worker(
    target: &str,
    mode: Mode,
    seed: u64,
    deadline: Instant,
    stats: Arc<Mutex<Stats>>,
) {
    let mut rng = Lcg::new(seed);
    let mut buf = vec![0u8; 256 * 1024];
    while Instant::now() < deadline {
        let mut st = stats.lock().await;
        st.attempts += 1;
        drop(st);
        if let Ok(mut client) = TcpStream::connect(target).await {
            if client.write_all(&[PROTOCOL_VERSION]).await.is_err() {
                continue;
            }
            match mode {
                Mode::Random => {
                    // 猛灌大块随机字节
                    while Instant::now() < deadline {
                        rng.fill(&mut buf);
                        match client.write_all(&buf).await {
                            Ok(()) => {
                                stats.lock().await.sent_bytes += buf.len() as u64;
                            }
                            Err(_) => break, // 服务端断开 → 重连
                        }
                    }
                }
                Mode::Proto => {
                    // 协议形状垃圾帧（小而狠，打深层解析）
                    let mut count = 0u64;
                    while Instant::now() < deadline && count < 10_000 {
                        let f = proto_frame(&mut rng);
                        let sent = f.len() as u64;
                        if client.write_all(&f).await.is_err() {
                            break;
                        }
                        stats.lock().await.sent_bytes += sent;
                        count += 1;
                    }
                }
                Mode::Reconnect => {
                    // 握手即断（连接风暴），不打数据
                    drop(client);
                }
                Mode::Mixed => {
                    // mixed 模式的攻击侧用 proto 垃圾
                    let mut count = 0u64;
                    while Instant::now() < deadline && count < 10_000 {
                        let f = proto_frame(&mut rng);
                        let sent = f.len() as u64;
                        if client.write_all(&f).await.is_err() {
                            break;
                        }
                        stats.lock().await.sent_bytes += sent;
                        count += 1;
                    }
                }
            }
            // 给服务端处理机会后自然断开
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// —— 合法玩家 worker（mixed 模式）——

async fn player_worker(target: &str, duration: Duration, latencies: Arc<Mutex<Vec<Duration>>>) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        match TcpStream::connect(target).await {
            Ok(mut client) => {
                if client.write_all(&[PROTOCOL_VERSION]).await.is_err() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                // 握手 + 鉴权（token 任意——服务端 mock 接受）
                let auth = frame(&{
                    let mut p = Vec::new();
                    encode_packet(
                        &ClientCommand::Authenticate {
                            token: Varchar::new("flooder-token".to_owned()).unwrap(),
                        },
                        &mut p,
                    );
                    p
                });
                if client.write_all(&auth).await.is_err() {
                    continue;
                }
                let mut rbuf = vec![0u8; 4096];
                let _ = tokio::time::timeout(Duration::from_secs(2), client.read(&mut rbuf)).await;

                // 建房（若服务端可鉴权）
                let create = frame(&{
                    let mut p = Vec::new();
                    encode_packet(
                        &ClientCommand::CreateRoom {
                            id: phira_api::RoomId::new("flood".to_owned()).unwrap(),
                        },
                        &mut p,
                    );
                    p
                });
                let _ = client.write_all(&create).await;

                // 循环 Ping/Pong 测延迟（玩家存活 + 服务端响应）
                while Instant::now() < deadline {
                    let ping = frame(&{
                        let mut p = Vec::new();
                        encode_packet(&ClientCommand::Ping, &mut p);
                        p
                    });
                    let t0 = Instant::now();
                    if client.write_all(&ping).await.is_err() {
                        break;
                    }
                    match tokio::time::timeout(Duration::from_secs(3), client.read(&mut rbuf)).await
                    {
                        Ok(Ok(n)) if n > 0 => {
                            latencies.lock().await.push(t0.elapsed());
                        }
                        _ => break, // 超时/断开
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

// —— 主流程 ——

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let args = parse_args();
    let duration = Duration::from_secs(args.duration);
    let deadline = Instant::now() + duration;
    let stats = Arc::new(Mutex::new(Stats::default()));

    // 攻击 worker
    let mut workers = Vec::new();
    for i in 0..args.connections {
        let target = args.target.clone();
        let mode = args.mode;
        let stats = Arc::clone(&stats);
        workers.push(tokio::spawn(async move {
            flood_worker(
                &target,
                mode,
                args.seed.wrapping_add(i as u64),
                deadline,
                stats,
            )
            .await;
        }));
    }
    // 玩家 worker（mixed）
    let latencies = Arc::new(Mutex::new(Vec::<Duration>::new()));
    let mut players = Vec::new();
    if args.mode == Mode::Mixed {
        for _ in 0..args.players {
            let target = args.target.clone();
            let lat = Arc::clone(&latencies);
            players.push(tokio::spawn(async move {
                player_worker(&target, duration, lat).await;
            }));
        }
    }

    for w in workers {
        let _ = w.await;
    }
    for p in players {
        let _ = p.await;
    }

    let st = stats.lock().await.clone();
    let lats = latencies.lock().await;
    let mbps = st.sent_bytes as f64 / 1_000_000.0 / duration.as_secs_f64() * 8.0;

    let lat_stats = if lats.is_empty() {
        (Duration::ZERO, Duration::ZERO, 0usize)
    } else {
        let mut v: Vec<Duration> = lats.clone();
        v.sort_unstable();
        let p50 = v[v.len() / 2];
        let p95 = v[(v.len() * 95) / 100];
        (p50, p95, v.len())
    };

    if args.json {
        let out = format!(
            r#"{{"mode":"{:?}","duration_s":{},"connections":{},"attempts":{},"sent_mb":{:.2},"mbps":{:.0},"player_p50_ms":{:.1},"player_p95_ms":{:.1},"player_samples":{}}}"#,
            args.mode,
            args.duration,
            args.connections,
            st.attempts,
            st.sent_bytes as f64 / 1_000_000.0,
            mbps,
            lat_stats.0.as_secs_f64() * 1000.0,
            lat_stats.1.as_secs_f64() * 1000.0,
            lat_stats.2,
        );
        println!("{out}");
    } else {
        println!("== flooder 结果 ==");
        println!(
            "mode: {:?} | 时长: {}s | 并发攻击连接: {}",
            args.mode, args.duration, args.connections
        );
        println!(
            "总发送: {:.2} MB ({mbps:.0} Mbps 等效)",
            st.sent_bytes as f64 / 1_000_000.0
        );
        println!("连接尝试: {}", st.attempts);
        if args.mode == Mode::Mixed {
            println!(
                "玩家延迟 (n={}): p50={:.1}ms p95={:.1}ms",
                lat_stats.2,
                lat_stats.0.as_secs_f64() * 1000.0,
                lat_stats.1.as_secs_f64() * 1000.0
            );
        }
    }
}
