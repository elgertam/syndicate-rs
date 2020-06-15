use futures::{SinkExt, StreamExt, poll};
use std::task::Poll;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use syndicate::packets::{ClientCodec, C2S, S2C, Action, Event};
use syndicate::value::Value;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut frames = Framed::new(TcpStream::connect("127.0.0.1:8001").await?, ClientCodec::new());
    frames.send(C2S::Connect(Value::from("chat").wrap())).await?;

    let present_action = Action::Assert(
        Value::from(0).wrap(),
        Value::simple_record1("Present", Value::from(std::process::id()).wrap()).wrap());
    let absent_action = Action::Clear(
        Value::from(0).wrap());

    frames.send(C2S::Turn(vec![present_action.clone()])).await?;
    loop {
        frames.send(C2S::Turn(vec![absent_action.clone()])).await?;
        frames.send(C2S::Turn(vec![present_action.clone()])).await?;

        loop {
            match poll!(frames.next()) {
                Poll::Pending => break,
                Poll::Ready(None) => {
                    print!("Server closed connection");
                    return Ok(());
                }
                Poll::Ready(Some(res)) => {
                    match res? {
                        S2C::Turn(events) => {
                            for e in events {
                                match e {
                                    Event::End(_) => (),
                                    _ => println!("{:?}", e),
                                }
                            }
                        }
                        S2C::Ping() => frames.send(C2S::Pong()).await?,
                        p => println!("{:?}", p),
                    }
                }
            }
        }
    }
}
