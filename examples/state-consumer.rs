#![recursion_limit = "256"]

use syndicate::{V, value::Value};
use syndicate::packets::{ClientCodec, C2S, S2C, Action, Event};
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
    let discard: V = Value::simple_record("discard", vec![]).wrap();
    let capture: V = Value::simple_record("capture", vec![discard]).wrap();

    let mut frames = Framed::new(TcpStream::connect("127.0.0.1:8001").await?, ClientCodec::new());
    frames.send(C2S::Connect(Value::from("chat").wrap())).await?;
    frames.send(
        C2S::Turn(vec![Action::Assert(
            Value::from(0).wrap(),
            Value::simple_record("observe", vec![
                Value::simple_record("Present", vec![capture]).wrap()]).wrap())]))
        .await?;

    let mut stats_timer = interval(Duration::from_secs(1));
    let mut turn_counter = 0;
    let mut event_counter = 0;
    let mut arrival_counter = 0;
    let mut departure_counter = 0;
    let mut occupancy = 0;

    loop {
        select! {
            _instant = stats_timer.next().boxed().fuse() => {
                print!("{:?} turns, {:?} events, {:?} arrivals, {:?} departures, {:?} present in the last second\n",
                       turn_counter,
                       event_counter,
                       arrival_counter,
                       departure_counter,
                       occupancy);
                turn_counter = 0;
                event_counter = 0;
                arrival_counter = 0;
                departure_counter = 0;
            },
            frame = frames.next().boxed().fuse() => match frame {
                None => return Ok(()),
                Some(res) => match res? {
                    S2C::Err(msg, _) => return Err(msg.into()),
                    S2C::Turn(events) => {
                        turn_counter = turn_counter + 1;
                        event_counter = event_counter + events.len();
                        for e in events {
                            match e {
                                Event::Add(_, _) => {
                                    arrival_counter = arrival_counter + 1;
                                    occupancy = occupancy + 1;
                                },
                                Event::Del(_, _) => {
                                    departure_counter = departure_counter + 1;
                                    occupancy = occupancy - 1;
                                },
                                _ => ()
                            }
                        }
                    },
                    S2C::Ping() => frames.send(C2S::Pong()).await?,
                    S2C::Pong() => (),
                }
            },
        }
    }
}
