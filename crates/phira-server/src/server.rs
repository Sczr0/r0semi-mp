//! 服务器（§4.5）：监听 + accept 循环 + 优雅停机（§11）。
//!
//! TODO(阶段 2): 鉴权编排 + dispatch 到 bus + 事件编码投递（§6.6 表 2）。

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use phira_api::{ClientCommand, ServerCommand};
use phira_core::Bus;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::stream::{PROTOCOL_VERSION, Stream};

/// 服务器：持有监听器 + 柜台（组合根唯一接线点之外，本结构不认识具体货物）。
pub struct Server {
    listener: TcpListener,
    #[allow(dead_code)] // 阶段 2 dispatch 接入后读取
    bus: Bus,
}

impl Server {
    /// 绑定端口（默认 12346，§3.5）。
    ///
    /// # Errors
    ///
    /// 端口绑定失败（占用 / 权限）→ `std::io::Error`。
    pub async fn new(addr: SocketAddr, bus: Bus) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener, bus })
    }

    /// 运行主循环：accept → 会话（阶段 1 填协议帧）。
    ///
    /// # Errors
    ///
    /// 获取本地地址失败 / 停机信号 handler 安装失败。
    pub async fn run(self) -> Result<()> {
        let local = self.listener.local_addr()?;
        info!("r0semi-mp-server listening on {local}");

        // 优雅停机（§11）：SIGTERM/SIGINT → 停止 accept
        let shutdown = shutdown_signal();

        tokio::select! {
            () = shutdown => {
                info!("shutdown signal received");
                // TODO(阶段 2): 向所有房间广播"服务器维护中" + 宽限窗口（§11）
            }
            () = self.accept_loop() => {}
        }
        Ok(())
    }

    async fn accept_loop(self) {
        let accept = self.listener;
        loop {
            match accept.accept().await {
                Ok((stream, addr)) => {
                    info!("connection from {addr}");
                    // 协议层（阶段 1）：版本握手 + ULEB128 帧；阶段 2 接鉴权与 dispatch
                    tokio::spawn(async move {
                        if let Err(err) = handle_connection(stream, addr).await {
                            warn!("connection handler error from {addr}: {err:?}");
                        }
                    });
                }
                Err(err) => warn!("failed to accept: {err:?}"),
            }
        }
    }
}

/// 单连接处理（阶段 1）：握手 → 心跳应答 → 等待关闭。
///
/// 阶段 2 在此接入：鉴权编排 → `Bus::dispatch` → 事件编码投递（§6.6 表 2）。
/// 独立成函数以便集成测试直接驱动（§4.9-6）。
///
/// # Errors
///
/// 握手失败（版本读取失败）时返回；业务错误走 `warn` 日志（不中断 accept）。
pub async fn handle_connection(stream: TcpStream, addr: SocketAddr) -> Result<()> {
    let handler = Box::new(
        move |tx: Arc<mpsc::Sender<ServerCommand>>, cmd: ClientCommand| async move {
            match cmd {
                // 心跳应答（§6.1：服务端不发 Ping，只回 Pong）
                ClientCommand::Ping => {
                    if let Err(e) = tx.send(ServerCommand::Pong).await {
                        warn!("failed to send Pong: {e:?}");
                    }
                }
                // 阶段 2 前：非 Ping 命令在会话建立后才会出现，先忽略并记录
                other => warn!("non-Ping command before auth wiring (stage 2): {other:?}"),
            }
        },
    );
    match Stream::<ServerCommand, ClientCommand>::new(None, stream, handler).await {
        Ok(stream) => {
            if stream.version() != PROTOCOL_VERSION {
                warn!(
                    "client protocol v{} != our v{PROTOCOL_VERSION} (accepting: backward compat)",
                    stream.version()
                );
            }
            info!("protocol v{} established from {addr}", stream.version());
            // 保持连接：等待对端关闭 / 解码失败；会话层（阶段 2）在此收尾
            stream.await_closed().await;
            info!("connection from {addr} closed");
            Ok(())
        }
        Err(err) => {
            warn!("handshake failed from {addr}: {err:?}");
            Ok(())
        }
    }
}

/// 优雅停机信号（§11）：SIGTERM（Unix）或 Ctrl+C（Windows）。
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl-c handler");
    }
}
