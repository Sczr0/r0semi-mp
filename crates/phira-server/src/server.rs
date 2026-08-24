//! 服务器（§4.5）：监听 + accept 循环 + 优雅停机（§11）。
//!
//! TODO(阶段 1): 协议帧——版本握手 + ULEB128 帧 + ClientCommand 解码（§6.1/§6.3，
//! 可复用原版 phira-mp-common，Apache-2.0）。
//! TODO(阶段 2): 鉴权编排 + dispatch 到 bus + 事件编码投递（§6.6 表 2）。

use std::net::SocketAddr;

use anyhow::Result;
use phira_core::Bus;
use tokio::net::TcpListener;
use tracing::{info, warn};

/// 服务器：持有监听器 + 柜台（组合根唯一接线点之外，本结构不认识具体货物）。
pub struct Server {
    listener: TcpListener,
    #[allow(dead_code)] // 阶段 2 dispatch 接入后读取
    bus: Bus,
}

impl Server {
    /// 绑定端口（默认 12346，§3.5）。
    pub async fn new(addr: SocketAddr, bus: Bus) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener, bus })
    }

    /// 运行主循环：accept → 会话（阶段 1 填协议帧）。
    pub async fn run(self) -> Result<()> {
        let local = self.listener.local_addr()?;
        info!("r0semi-mp-server listening on {local}");

        // 优雅停机（§11）：SIGTERM/SIGINT → 停止 accept
        let shutdown = shutdown_signal();

        tokio::select! {
            _ = shutdown => {
                info!("shutdown signal received");
                // TODO(阶段 2): 向所有房间广播"服务器维护中" + 宽限窗口（§11）
            }
            _ = self.accept_loop() => {}
        }
        Ok(())
    }

    async fn accept_loop(self) {
        let accept = self.listener;
        loop {
            match accept.accept().await {
                Ok((stream, addr)) => {
                    info!("connection from {addr}");
                    // 协议层未就绪：阶段 1 填 Stream::new + 解码；当前关闭
                    drop(stream);
                }
                Err(err) => warn!("failed to accept: {err:?}"),
            }
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
