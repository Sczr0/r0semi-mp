//! 阶段 4 TLS 集成测试（§10.3 红线：rustls 单栈）。
//!
//! 覆盖：手写 HTTP/1.1 客户端在 **rustls TLS 传输**上的握手与请求/响应正确性。
//! 客户端注入"接受一切"verifier（自签证书），验证的是**传输链路**；
//! 生产证书链验证（webpki-roots）由 rustls 标准行为保证，此处不重测。

use std::sync::Arc;

use phira_server::http_get_with_tls;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// 测试 verifier：接受任何服务器证书（只验证传输，不验证信任链）。
#[derive(Debug)]
struct AcceptAny;

impl ServerCertVerifier for AcceptAny {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

/// 本地 TLS 服务器：自签证书 + rustls 服务端，回 `HTTP/1.1 200` + JSON body。
async fn spawn_tls_server() -> (String, u16) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert_der = cert.cert.der().clone();
    let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut tls = acceptor.accept(tcp).await.unwrap();
        // 读请求头（到空行）
        let mut head = Vec::new();
        let mut buf = [0u8; 512];
        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = tls.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            head.extend_from_slice(&buf[..n]);
        }
        let body = br#"{"id":1,"name":"TLS Chart"}"#;
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        tls.write_all(resp.as_bytes()).await.unwrap();
        tls.write_all(body).await.unwrap();
        let _ = tls.shutdown().await;
    });

    (addr.to_string(), addr.port())
}

/// 测试用客户端 config：ring provider + AcceptAny verifier。
fn accept_any_config() -> Arc<rustls::ClientConfig> {
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAny))
        .with_no_client_auth();
    Arc::new(config)
}

#[tokio::test]
async fn https_request_over_rustls_tls() {
    let (addr, port) = spawn_tls_server().await;
    // 端口必须 = 基址显式端口（默认 443 在测试里连不上）
    let base = format!("https://localhost:{port}");

    let bytes = http_get_with_tls(&base, "/chart/1", None, Some(accept_any_config()))
        .await
        .expect("TLS 握手 + HTTP 请求应成功");

    let body = String::from_utf8(bytes).unwrap();
    assert_eq!(body, r#"{"id":1,"name":"TLS Chart"}"#);
    assert!(!addr.is_empty());
}

#[tokio::test]
async fn https_default_port_is_443() {
    // 默认端口 443 + TLS 传输：连接 127.0.0.1:443（无服务）应报 connect 错误
    let err = http_get_with_tls("https://127.0.0.1", "/me", None, Some(accept_any_config()))
        .await
        .expect_err("127.0.0.1:443 无服务应连接失败");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("connect"),
        "https 默认端口 443 应被解析: {msg}"
    );
}

#[tokio::test]
async fn http_default_port_is_80() {
    let err = http_get_with_tls("http://127.0.0.1", "/me", None, None)
        .await
        .expect_err("127.0.0.1:80 无服务应连接失败");
    let msg = format!("{err:?}");
    assert!(msg.contains("connect"), "http 默认端口 80 应被解析: {msg}");
}
