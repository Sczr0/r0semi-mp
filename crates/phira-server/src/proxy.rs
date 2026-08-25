//! PROXY protocol v1/v2 解析（前置层：反代后真实 IP）。
//!
//! 依据 HAProxy PROXY protocol 规范：v1 文本行 / v2 二进制头。
//! 本项目只需要**源 IP**（每 IP 限额/审计用，§10.4）——端口与目标地址解析后丢弃。
//!
//! 启用条件：`server_config.yml` 的 `proxy_protocol: true`（反代/CDN 后部署）。
//! 未启用时 `handle_connection` 不调用本模块（直连零开销）。
//! 启用后客户端（反代）**必须**发 PROXY 头（HAProxy `send-proxy` / nginx `proxy_protocol on`），
//! 头非法/缺失 → 断开——协议错乱比误放行安全。
//!
//! 借鉴：gooophira（v1/v2 均支持）、phira-mp-nodejsver（PROXY v2）、jphira-mp（HAProxy）。

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use tokio::io::{AsyncRead, AsyncReadExt};

/// PROXY protocol v2 签名（`\r\n\r\n\0\r\nQUIT\n`，12 字节）。
const V2_SIGNATURE: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];
/// v1 文本行前缀。
const V1_PREFIX: &[u8] = b"PROXY ";
/// v1 行最大长度（`PROXY TCP6 <ipv6> <ipv6> 65535 65535` ~107 字节；留裕量防洪水）。
const V1_MAX_LINE: usize = 128;
/// v2 命令（低 4 位）。
const V2_CMD_LOCAL: u8 = 0;
const V2_CMD_PROXY: u8 = 1;
/// v2 地址族（高 4 位）。
const V2_FAM_INET: u8 = 1;
const V2_FAM_INET6: u8 = 2;
/// v2 版本（高 4 位）。
const V2_VERSION: u8 = 2;

/// PROXY 头解析结果：真实源 IP。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProxyHeader {
    /// 客户端真实源 IP（反代透传）。
    pub src_ip: IpAddr,
}

/// PROXY 头解析失败（头非法/截断/EOF——调用方应断开连接）。
#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    /// 前 12 字节既不是 v2 签名也不是 v1 前缀（未启用 PROXY 的直连客户端）。
    #[error("not a PROXY protocol header (first bytes {0:02X?})")]
    NotProxyHeader([u8; 12]),
    /// 头截断（EOF/超时）。
    #[error("proxy header truncated: {0}")]
    Truncated(#[from] io::Error),
    /// v1 行超长。
    #[error("proxy v1 line too long ({0} bytes)")]
    V1TooLong(usize),
    /// v1 字段非法（TCP4/TCP6 但源 IP 解析失败等）。
    #[error("proxy v1 invalid: {0}")]
    V1Invalid(String),
    /// v2 版本/族/长度非法。
    #[error("proxy v2 invalid: {0}")]
    V2Invalid(String),
}

impl From<ProxyError> for io::Error {
    fn from(e: ProxyError) -> Self {
        match e {
            ProxyError::Truncated(io) => io,
            other => io::Error::new(io::ErrorKind::InvalidData, other),
        }
    }
}

/// 读取并解析 PROXY protocol 头（v1/v2），返回真实源 IP。
///
/// - `Ok(Some(header))`：解析成功（v1 TCP4/TCP6、v2 PROXY+INET/INET6）
/// - `Ok(None)`：无地址信息（v2 LOCAL、v1 UNKNOWN、v2 UNIX 族）——调用方用 socket 地址
/// - `Err`：头非法 / 截断——调用方应断开连接
///
/// 本函数**不会多读**：读完头后，剩余字节（版本握手/协议帧）仍在流中。
/// 调用方应在外层包 `tokio::time::timeout`（半开连接防护，与握手超时同级）。
///
/// # Errors
///
/// 头非法 / 截断（EOF）时返回 [`ProxyError`]——调用方应断开连接。
pub async fn read_proxy_header<R>(reader: &mut R) -> Result<Option<ProxyHeader>, ProxyError>
where
    R: AsyncRead + Unpin,
{
    let mut head = [0u8; 12];
    reader.read_exact(&mut head).await?; // 截断 → Truncated
    if head == V2_SIGNATURE {
        read_v2(reader).await
    } else if head.starts_with(V1_PREFIX) {
        read_v1(reader, head).await
    } else {
        Err(ProxyError::NotProxyHeader(head))
    }
}

/// v2 二进制头（签名已验证）。
async fn read_v2<R>(reader: &mut R) -> Result<Option<ProxyHeader>, ProxyError>
where
    R: AsyncRead + Unpin,
{
    let mut fixed = [0u8; 4]; // ver_cmd, fam_proto, len(2, BE)
    reader.read_exact(&mut fixed).await?;
    let ver_cmd = fixed[0];
    let fam = fixed[1] >> 4;
    let len = u16::from_be_bytes([fixed[2], fixed[3]]) as usize;

    if ver_cmd >> 4 != V2_VERSION {
        return Err(ProxyError::V2Invalid(format!(
            "unsupported version {}",
            ver_cmd >> 4
        )));
    }
    let cmd = ver_cmd & 0x0F;

    match (cmd, fam) {
        // LOCAL / UNIX 族等：无 IP，丢弃地址块，用 socket 地址
        (V2_CMD_LOCAL, _) | (V2_CMD_PROXY, 3..) => {
            skip(reader, len).await?;
            Ok(None)
        }
        (V2_CMD_PROXY, V2_FAM_INET) => {
            if len < 12 {
                return Err(ProxyError::V2Invalid(format!("inet addr len {len} < 12")));
            }
            let mut addr = [0u8; 12];
            reader.read_exact(&mut addr).await?;
            skip(reader, len - 12).await?;
            Ok(Some(ProxyHeader {
                src_ip: IpAddr::V4(Ipv4Addr::new(addr[0], addr[1], addr[2], addr[3])),
            }))
        }
        (V2_CMD_PROXY, V2_FAM_INET6) => {
            if len < 36 {
                return Err(ProxyError::V2Invalid(format!("inet6 addr len {len} < 36")));
            }
            let mut addr = [0u8; 36];
            reader.read_exact(&mut addr).await?;
            skip(reader, len - 36).await?;
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&addr[..16]);
            Ok(Some(ProxyHeader {
                src_ip: IpAddr::V6(Ipv6Addr::from(octets)),
            }))
        }
        (V2_CMD_PROXY, _) => {
            skip(reader, len).await?;
            Ok(None)
        }
        // 未知命令（规范只有 LOCAL/PROXY）：拒绝
        _ => Err(ProxyError::V2Invalid(format!("unknown command {cmd}"))),
    }
}

/// v1 文本行（前缀已验证；已读 12 字节作为行缓冲起点）。
async fn read_v1<R>(reader: &mut R, buf: [u8; 12]) -> Result<Option<ProxyHeader>, ProxyError>
where
    R: AsyncRead + Unpin,
{
    let mut line = Vec::with_capacity(64);
    line.extend_from_slice(&buf);
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).await?;
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            break;
        }
        if line.len() > V1_MAX_LINE {
            return Err(ProxyError::V1TooLong(line.len()));
        }
    }
    // 去掉行尾 \r\n
    let text = String::from_utf8_lossy(&line[..line.len() - 2]);
    let fields: Vec<&str> = text.split_whitespace().collect();
    match fields.as_slice() {
        // UNKNOWN：无信息（规范允许带地址，忽略）
        ["PROXY", "UNKNOWN", ..] => Ok(None),
        ["PROXY", "TCP4", src, ..] => {
            let ip: Ipv4Addr = src
                .parse()
                .map_err(|_| ProxyError::V1Invalid(format!("bad TCP4 src {src}")))?;
            Ok(Some(ProxyHeader {
                src_ip: IpAddr::V4(ip),
            }))
        }
        ["PROXY", "TCP6", src, ..] => {
            let ip: Ipv6Addr = src
                .parse()
                .map_err(|_| ProxyError::V1Invalid(format!("bad TCP6 src {src}")))?;
            Ok(Some(ProxyHeader {
                src_ip: IpAddr::V6(ip),
            }))
        }
        ["PROXY", fam, ..] => Err(ProxyError::V1Invalid(format!("bad family {fam}"))),
        _ => Err(ProxyError::V1Invalid("malformed line".to_owned())),
    }
}

/// 丢弃 len 字节（地址块剩余 / LOCAL 块）。
async fn skip<R>(reader: &mut R, len: usize) -> Result<(), ProxyError>
where
    R: AsyncRead + Unpin,
{
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncWriteExt, duplex};

    /// 构造 v2 头（fam = 地址族，cmd = 0 LOCAL / 1 PROXY，addr = 地址块）。
    #[allow(clippy::cast_possible_truncation)] // 测试：地址块 ≤ u16::MAX
    fn v2(fam: u8, cmd: u8, addr: &[u8]) -> Vec<u8> {
        let mut buf = V2_SIGNATURE.to_vec();
        buf.push((V2_VERSION << 4) | cmd);
        buf.push((fam << 4) | 1); // STREAM 协议
        let len = addr.len() as u16;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(addr);
        buf
    }

    /// 把字节喂进 duplex 并解析（写端 drop → 截断场景 read_exact 返回 EOF，不挂起）。
    async fn parse(bytes: &[u8]) -> Result<Option<ProxyHeader>, ProxyError> {
        let (mut a, mut b) = duplex(1024);
        a.write_all(bytes).await.unwrap();
        drop(a);
        read_proxy_header(&mut b).await
    }

    #[tokio::test]
    async fn v2_inet_ipv4() {
        let mut addr = [0u8; 12];
        addr[..4].copy_from_slice(&[192, 168, 1, 5]);
        let hdr = v2(V2_FAM_INET, V2_CMD_PROXY, &addr);
        let parsed = parse(&hdr).await.unwrap().unwrap();
        assert_eq!(parsed.src_ip, IpAddr::from([192, 168, 1, 5]));
    }

    #[tokio::test]
    async fn v2_inet6_ipv6() {
        let mut addr = [0u8; 36];
        addr[..16].copy_from_slice(&[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        let hdr = v2(V2_FAM_INET6, V2_CMD_PROXY, &addr);
        let parsed = parse(&hdr).await.unwrap().unwrap();
        assert_eq!(
            parsed.src_ip,
            IpAddr::from([0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
        );
    }

    #[tokio::test]
    async fn v2_local_returns_none() {
        // LOCAL 命令 + 任意地址块：无真实地址
        let hdr = v2(V2_FAM_INET, V2_CMD_LOCAL, &[0u8; 12]);
        assert!(parse(&hdr).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn v2_extra_bytes_not_consumed() {
        // 头之后还有协议字节（如版本 0x01）——解析后不应吞掉
        let mut addr = [0u8; 12];
        addr[..4].copy_from_slice(&[10, 0, 0, 9]);
        let mut bytes = v2(V2_FAM_INET, V2_CMD_PROXY, &addr);
        bytes.push(0x01); // 版本握手字节
        let (mut a, mut b) = duplex(1024);
        a.write_all(&bytes).await.unwrap();
        let parsed = read_proxy_header(&mut b).await.unwrap().unwrap();
        assert_eq!(parsed.src_ip, IpAddr::from([10, 0, 0, 9]));
        let mut rest = [0u8; 1];
        b.read_exact(&mut rest).await.unwrap();
        assert_eq!(rest[0], 0x01);
    }

    #[tokio::test]
    async fn v1_tcp4() {
        let bytes = b"PROXY TCP4 203.0.113.7 123.0.0.1 12345 12346\r\n";
        let parsed = parse(bytes).await.unwrap().unwrap();
        assert_eq!(parsed.src_ip, IpAddr::from([203, 0, 113, 7]));
    }

    #[tokio::test]
    async fn v1_tcp6() {
        let bytes = b"PROXY TCP6 2001:db8::1 2001:db8::2 12345 12346\r\n";
        let parsed = parse(bytes).await.unwrap().unwrap();
        assert_eq!(
            parsed.src_ip,
            IpAddr::from([0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
        );
    }

    #[tokio::test]
    async fn v1_unknown_returns_none() {
        let bytes = b"PROXY UNKNOWN\r\n";
        assert!(parse(bytes).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn not_proxy_header_errors() {
        // 直连客户端（版本字节 0x01 开头）→ 明确报错而非误判
        let bytes = [
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let err = parse(&bytes).await.unwrap_err();
        assert!(matches!(err, ProxyError::NotProxyHeader(_)));
    }

    #[tokio::test]
    async fn truncated_header_errors() {
        // 只有部分签名 → 截断错误（EOF）
        let bytes = &V2_SIGNATURE[..6];
        assert!(matches!(parse(bytes).await, Err(ProxyError::Truncated(_))));
    }

    #[tokio::test]
    async fn v1_too_long_errors() {
        let mut bytes = b"PROXY TCP4 ".to_vec();
        bytes.extend_from_slice(&[b'1'; V1_MAX_LINE + 10]);
        let err = parse(&bytes).await.unwrap_err();
        assert!(matches!(err, ProxyError::V1TooLong(_)));
    }

    #[tokio::test]
    async fn v2_bad_family_len_errors() {
        // INET 但 len < 12 → 非法
        let hdr = v2(V2_FAM_INET, V2_CMD_PROXY, &[0u8; 4]);
        assert!(matches!(parse(&hdr).await, Err(ProxyError::V2Invalid(_))));
    }

    #[tokio::test]
    async fn v2_unix_family_returns_none() {
        // UNIX 族（fam=3）：无 IP，用 socket 地址
        let hdr = v2(3, V2_CMD_PROXY, &[0u8; 108]);
        assert!(parse(&hdr).await.unwrap().is_none());
    }
}
