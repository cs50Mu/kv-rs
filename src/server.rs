use kv::{Service, ServiceInner, MemTable, ProstServerStream, TlsServerAcceptor};
use tokio::net::TcpListener;
use tracing::info;
use anyhow::Result;


#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:9527";

    // 以后从配置文件取
    let server_cert = include_str!("../fixtures/server.cert");
    let server_key = include_str!("../fixtures/server.key");

    let acceptor = TlsServerAcceptor::new(server_cert, server_key, None)?;
    let service: Service = ServiceInner::new(MemTable::new()).into();
    let listner = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);
    loop {
        let tls = acceptor.clone();
        let (stream, addr) = listner.accept().await?;
        info!("Client {:?} connected", addr);
        let stream = tls.accept(stream).await?;
        let stream = ProstServerStream::new(stream, service.clone());
        tokio::spawn(async move { stream.process().await });
    }
}
