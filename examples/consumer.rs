#![recursion_limit = "256"]

use syndicate::{V, value::Value};
use syndicate::packets::{ClientCodec, C2S, S2C, Action};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures::SinkExt;
use futures::StreamExt;
use futures::FutureExt;
use futures::select;
use core::time::Duration;
use tokio::time::interval;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let discard: V = Value::simple_record("Discard", vec![]).wrap();
    let capture: V = Value::simple_record("Capture", vec![discard]).wrap();

    let mut frames = Framed::new(TcpStream::connect("127.0.0.1:8001").await?, ClientCodec::new());
    frames.send(C2S::Connect(Value::from("chat").wrap())).await?;
    frames.send(
        C2S::Turn(vec![Action::Assert(
            Value::from(0).wrap(),
            Value::simple_record("Observe", vec![
                Value::simple_record("Says", vec![capture.clone(), capture]).wrap()]).wrap())]))
        .await?;

    let mut stats_timer = interval(Duration::from_secs(1));
    let mut turn_counter = 0;
    let mut event_counter = 0;

    loop {
        select! {
            _instant = stats_timer.next().boxed().fuse() => {
                print!("{:?} turns, {:?} events in the last second\n", turn_counter, event_counter);
                turn_counter = 0;
                event_counter = 0;
            },
            frame = frames.next().boxed().fuse() => match frame {
                None => return Ok(()),
                Some(res) => match res? {
                    S2C::Err(msg, _) => return Err(msg.into()),
                    S2C::Turn(es) => {
                        // print!("{:?}\n", es);
                        turn_counter = turn_counter + 1;
                        event_counter = event_counter + es.len();
                    },
                    S2C::Ping() => frames.send(C2S::Pong()).await?,
                    S2C::Pong() => (),
                }
            },
        }
    }
}
