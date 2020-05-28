use syndicate::value::Value;
use syndicate::packets::{ClientCodec, C2S, S2C, Action};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt, poll};
use std::task::Poll;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut frames = Framed::new(TcpStream::connect("127.0.0.1:8001").await?, ClientCodec::new());
    frames.send(C2S::Connect(Value::from("chat").wrap())).await?;

    let mut counter: u64 = 0;
    loop {
        counter = counter + 1;
        frames.send(C2S::Turn(vec![Action::Message(
            Value::simple_record("Says", vec![
                Value::from("producer").wrap(),
                Value::from(counter).wrap(),
            ]).wrap())])).await?;
        loop {
            match poll!(frames.next()) {
                Poll::Pending => break,
                Poll::Ready(None) => {
                    print!("Server closed connection");
                    return Ok(());
                }
                Poll::Ready(Some(res)) => {
                    let p = res?;
                    print!("{:?}\n", p);
                    if let S2C::Ping() = p { frames.send(C2S::Pong()).await? }
                }
            }
        }
    }
}
