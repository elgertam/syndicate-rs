#![recursion_limit = "256"]

use core::time::Duration;
use futures::FutureExt;
use futures::SinkExt;
use futures::StreamExt;
use futures::select;
use structopt::StructOpt;
use tokio::net::TcpStream;
use tokio::time::interval;
use tokio_util::codec::Framed;

use syndicate::packets::{ClientCodec, C2S, S2C, Action, Event};
use syndicate::value::Value;

#[derive(Clone, Debug, StructOpt)]
pub enum PingPongMode {
    Ping {
        #[structopt(short = "t", default_value = "1")]
        turn_count: u32,

        #[structopt(short = "a", default_value = "1")]
        action_count: u32,
    },

    Pong,
}

#[derive(Clone, Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    mode: PingPongMode,

    #[structopt(default_value = "pingpong")]
    dataspace: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_args();

    let (send_label, recv_label) = match config.mode {
        PingPongMode::Ping { turn_count: _, action_count: _ } => ("Ping", "Pong"),
        PingPongMode::Pong => ("Pong", "Ping"),
    };

    let mut frames = Framed::new(TcpStream::connect("127.0.0.1:8001").await?, ClientCodec::new());
    frames.send(C2S::Connect(Value::from(config.dataspace).wrap())).await?;

    frames.send(
        C2S::Turn(vec![Action::Assert(
            Value::from(0).wrap(),
            Value::simple_record("Observe", vec![
                Value::simple_record(recv_label, vec![]).wrap()]).wrap())]))
        .await?;

    let mut stats_timer = interval(Duration::from_secs(1));
    let mut turn_counter = 0;
    let mut event_counter = 0;

    if let PingPongMode::Ping { turn_count, action_count } = config.mode {
        for _ in 0..turn_count {
            let mut actions = vec![];
            for _ in 0..action_count {
                actions.push(Action::Message(
                    Value::simple_record(send_label, vec![]).wrap()));
            }
            frames.send(C2S::Turn(actions)).await?;
        }
    }

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
                    S2C::Turn(events) => {
                        turn_counter = turn_counter + 1;
                        event_counter = event_counter + events.len();
                        let mut actions = vec![];
                        for e in events {
                            match e {
                                Event::Msg(_, _) =>
                                    actions.push(Action::Message(
                                        Value::simple_record(send_label, vec![]).wrap())),
                                _ =>
                                    ()
                            }
                        }
                        frames.send(C2S::Turn(actions)).await?;
                    },
                    S2C::Ping() => frames.send(C2S::Pong()).await?,
                    S2C::Pong() => (),
                }
            },
        }
    }
}
