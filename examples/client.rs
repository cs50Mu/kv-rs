use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use kv::{CommandRequest, CommandResponse};
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";
    // 连接服务器
    let stream = TcpStream::connect(addr).await?;

    // 使用 AsyncProstStream 来处理 Tcp Frame
    let mut client = 
        AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();

    // 生成一个 HSET 命令
    let cmd = CommandRequest::new_hset("table1", "hello", "world".into());

    // 发送 HSET 命令
    client.send(cmd).await?;
    if let Some(Ok(data)) = client.next().await {
        info!("Got hset response {:?}", data);
    }

    // hget
    let cmd = CommandRequest::new_hget("table1", "hello");
    client.send(cmd).await?;
    if let Some(Ok(data)) = client.next().await {
        info!("Got hget response {:?}", data);
    }

    // // hmget
    // let cmd = CommandRequest::new_hmget("table1", vec!["hello".into(), "this".into()]);
    // client.send(cmd).await?;
    // if let Some(Ok(data)) = client.next().await {
    //     info!("Got hmget response {:?}", data);
    // }

    // // hexist
    // let cmd = CommandRequest::new_hexist("table1", "hello");
    // client.send(cmd).await?;
    // if let Some(Ok(data)) = client.next().await {
    //     info!("Got hexist response {:?}", data);
    // }

    // // hdel
    // let cmd = CommandRequest::new_hdel("table1", "hello");
    // client.send(cmd).await?;
    // if let Some(Ok(data)) = client.next().await {
    //     info!("Got hdel response {:?}", data);
    // }

    // // hexist should return false now
    // let cmd = CommandRequest::new_hexist("table1", "hello");
    // client.send(cmd).await?;
    // if let Some(Ok(data)) = client.next().await {
    //     info!("Got hexist response {:?} for key: `hello`", data);
    // }

    Ok(())
}
