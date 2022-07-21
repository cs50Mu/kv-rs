use anyhow::Result;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use kv::{CommandRequest, MemTable, Service, ServiceInner};
use prost::Message;
use tokio::net::TcpListener;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let service: Service = ServiceInner::new(MemTable::default()).into();
    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);
    loop {
        let (stream, addr) = listener.accept().await?;
        info!("Client {:?} connected", addr);
        // https://docs.rs/tokio-util/latest/tokio_util/codec/length_delimited/index.html
        // LengthDelimitedCodec 默认 4 字节长度
        let mut stream = Framed::new(stream, LengthDelimitedCodec::new());
        let svc = service.clone();
        tokio::spawn(async move {
            // stream 之所以有 next 和 send 方法，是因为：
            // `Framed` 实现了 `Stream` 和 `Sink` trait
            // 而只要一个类型实现了 `Stream` 和 `Sink` trait，会
            // 自动实现 `StreamExt` 和 `SinkExt` trait (Blanket Implementations)
            // `next` 是 `StreamExt` 的方法；`send` 是 `SinkExt` 的方法
            // ref: https://docs.rs/tokio-util/latest/tokio_util/codec/struct.Framed.html#impl-Stream
            while let Some(Ok(data)) = stream.next().await {
                let cmd = CommandRequest::decode(data).unwrap();
                info!("Got a new command: {:?}", cmd);
                let res = svc.execute(cmd);
                stream.send(Bytes::from(res.encode_to_vec())).await.unwrap();
            }
            info!("Client {:?} disconnected", addr);
        });
    }
}
