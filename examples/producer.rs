use futures::{SinkExt, StreamExt, poll};
use std::task::Poll;
use structopt::StructOpt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use syndicate::packets::{ClientCodec, C2S, S2C, Action};
use syndicate::value::Value;

#[derive(Clone, Debug, StructOpt)]
pub struct Config {
    #[structopt(short = "a", default_value = "1")]
    action_count: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_args();

    let mut frames = Framed::new(TcpStream::connect("127.0.0.1:8001").await?, ClientCodec::new());
    frames.send(C2S::Connect(Value::from("chat").wrap())).await?;

    let mut counter: u64 = 0;
    loop {
        counter = counter + 1;

        let mut actions = vec![];
        for _ in 0..config.action_count {
            actions.push(Action::Message(
                Value::simple_record("Says", vec![
                    Value::from("producer").wrap(),
                    Value::from(counter).wrap(),
                ]).wrap()));
        }
        frames.send(C2S::Turn(actions)).await?;

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
