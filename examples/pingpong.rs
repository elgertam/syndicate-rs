#![recursion_limit = "512"]

use core::time::Duration;
use futures::FutureExt;
use futures::SinkExt;
use futures::StreamExt;
use futures::select;
use std::time::{SystemTime, SystemTimeError};
use structopt::StructOpt;
use tokio::net::TcpStream;
use tokio::time::interval;
use tokio_util::codec::Framed;

use syndicate::packets::{ClientCodec, C2S, S2C, Action, Event};
use syndicate::value::{NestedValue, Value, IOValue};

#[derive(Clone, Debug, StructOpt)]
pub struct PingConfig {
    #[structopt(short = "t", default_value = "1")]
    turn_count: u32,

    #[structopt(short = "a", default_value = "1")]
    action_count: u32,

    #[structopt(short = "l", default_value = "0")]
    report_latency_every: usize,

    #[structopt(short = "b", default_value = "0")]
    bytes_padding: usize,
}

#[derive(Clone, Debug, StructOpt)]
pub enum PingPongMode {
    Ping(PingConfig),
    Pong,
}

#[derive(Clone, Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    mode: PingPongMode,

    #[structopt(default_value = "pingpong")]
    dataspace: String,
}

fn now() -> Result<u64, SystemTimeError> {
    Ok(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_nanos() as u64)
}

fn simple_record2(label: &str, v1: IOValue, v2: IOValue) -> IOValue {
    let mut r = Value::simple_record(label, 2);
    r.fields_vec_mut().push(v1);
    r.fields_vec_mut().push(v2);
    r.finish().wrap()
}

fn report_latencies(rtt_ns_samples: &Vec<u64>) {
    let n = rtt_ns_samples.len();
    let rtt_0 = rtt_ns_samples[0];
    let rtt_50 = rtt_ns_samples[n * 1 / 2];
    let rtt_90 = rtt_ns_samples[n * 90 / 100];
    let rtt_95 = rtt_ns_samples[n * 95 / 100];
    let rtt_99 = rtt_ns_samples[n * 99 / 100];
    let rtt_99_9 = rtt_ns_samples[n * 999 / 1000];
    let rtt_99_99 = rtt_ns_samples[n * 9999 / 10000];
    let rtt_max = rtt_ns_samples[n - 1];
    println!("rtt: 0% {:05.5}ms, 50% {:05.5}ms, 90% {:05.5}ms, 95% {:05.5}ms, 99% {:05.5}ms, 99.9% {:05.5}ms, 99.99% {:05.5}ms, max {:05.5}ms",
             rtt_0 as f64 / 1000000.0,
             rtt_50 as f64 / 1000000.0,
             rtt_90 as f64 / 1000000.0,
             rtt_95 as f64 / 1000000.0,
             rtt_99 as f64 / 1000000.0,
             rtt_99_9 as f64 / 1000000.0,
             rtt_99_99 as f64 / 1000000.0,
             rtt_max as f64 / 1000000.0);
    println!("msg: 0% {:05.5}ms, 50% {:05.5}ms, 90% {:05.5}ms, 95% {:05.5}ms, 99% {:05.5}ms, 99.9% {:05.5}ms, 99.99% {:05.5}ms, max {:05.5}ms",
             rtt_0 as f64 / 2000000.0,
             rtt_50 as f64 / 2000000.0,
             rtt_90 as f64 / 2000000.0,
             rtt_95 as f64 / 2000000.0,
             rtt_99 as f64 / 2000000.0,
             rtt_99_9 as f64 / 2000000.0,
             rtt_99_99 as f64 / 2000000.0,
             rtt_max as f64 / 2000000.0);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_args();

    let (send_label, recv_label, report_latency_every, should_echo, bytes_padding) =
        match config.mode {
            PingPongMode::Ping(ref c) =>
                ("Ping", "Pong", c.report_latency_every, false, c.bytes_padding),
            PingPongMode::Pong =>
                ("Pong", "Ping", 0, true, 0),
        };

    let mut frames = Framed::new(TcpStream::connect("127.0.0.1:8001").await?, ClientCodec::new());
    frames.send(C2S::Connect(Value::from(config.dataspace).wrap())).await?;

    let discard: IOValue = Value::simple_record0("discard").wrap();
    let capture: IOValue = Value::simple_record1("capture", discard).wrap();
    let pat: IOValue = simple_record2(recv_label, capture.clone(), capture);
    frames.send(
        C2S::Turn(vec![Action::Assert(
            Value::from(0).wrap(),
            Value::simple_record1("observe", pat).wrap())]))
        .await?;

    let padding: IOValue = Value::ByteString(vec![0; bytes_padding]).wrap();

    let mut stats_timer = interval(Duration::from_secs(1));
    let mut turn_counter = 0;
    let mut event_counter = 0;
    let mut current_rec: IOValue = simple_record2(send_label,
                                                  Value::from(0).wrap(),
                                                  padding.clone());

    if let PingPongMode::Ping(ref c) = config.mode {
        for _ in 0..c.turn_count {
            let mut actions = vec![];
            current_rec = simple_record2(send_label,
                                         Value::from(now()?).wrap(),
                                         padding.clone());
            for _ in 0..c.action_count {
                actions.push(Action::Message(current_rec.clone()));
            }
            frames.send(C2S::Turn(actions)).await?;
        }
    }

    let mut rtt_ns_samples: Vec<u64> = vec![0; report_latency_every];
    let mut rtt_batch_count = 0;

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
                        let mut have_sample = false;
                        for e in events {
                            match e {
                                Event::Msg(_, captures) => {
                                    if should_echo || (report_latency_every == 0) {
                                        actions.push(Action::Message(
                                            simple_record2(send_label,
                                                           captures[0].clone(),
                                                           captures[1].clone())));
                                    } else {
                                        if !have_sample {
                                            let rtt_ns = now()? - captures[0].value().to_u64()?;
                                            rtt_ns_samples[rtt_batch_count] = rtt_ns;
                                            rtt_batch_count = rtt_batch_count + 1;

                                            if rtt_batch_count == report_latency_every {
                                                rtt_ns_samples.sort();
                                                report_latencies(&rtt_ns_samples);
                                                rtt_batch_count = 0;
                                            }

                                            have_sample = true;
                                            current_rec = simple_record2(send_label,
                                                                         Value::from(now()?).wrap(),
                                                                         padding.clone());
                                        }
                                        actions.push(Action::Message(current_rec.clone()));
                                    }
                                }
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
