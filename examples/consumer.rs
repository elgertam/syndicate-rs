use syndicate::{packets, value::Value};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures::SinkExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut frames = Framed::new(TcpStream::connect("127.0.0.1:8001").await?,
                                 packets::ClientCodec::standard());
    frames.send(packets::C2S::Connect(Value::from("producer-consumer-example").wrap())).await?;
    Ok(())
}
