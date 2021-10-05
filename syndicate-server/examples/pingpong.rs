use std::sync::Arc;
use std::time::SystemTime;

use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::language;
use syndicate::relay;
use syndicate::schemas::dataspace::Observe;
use syndicate::sturdy;
use syndicate::value::NestedValue;
use syndicate::value::Value;

use tokio::net::TcpStream;

use core::time::Duration;
use tokio::time::interval;

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

    #[structopt(short = "d", default_value = "b4b303726566b10973796e646963617465b584b210a6480df5306611ddd0d3882b546e197784")]
    dataspace: String,
}

fn now() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("time after epoch").as_nanos() as u64
}

fn simple_record2(label: &str, v1: AnyValue, v2: AnyValue) -> AnyValue {
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
    syndicate::convenient_logging()?;
    let config = Config::from_args();
    let sturdyref = sturdy::SturdyRef::from_hex(&config.dataspace)?;
    let (i, o) = TcpStream::connect("127.0.0.1:8001").await?.into_split();
    Actor::new().boot(syndicate::name!("pingpong"), |t| {
        relay::connect_stream(t, i, o, false, sturdyref, (), move |_state, t, ds| {

            let (send_label, recv_label, report_latency_every, should_echo, bytes_padding) =
                match config.mode {
                    PingPongMode::Ping(ref c) =>
                        ("Ping", "Pong", c.report_latency_every, false, c.bytes_padding),
                    PingPongMode::Pong =>
                        ("Pong", "Ping", 0, true, 0),
                };

            let consumer = {
                let ds = Arc::clone(&ds);
                let mut turn_counter: u64 = 0;
                let mut event_counter: u64 = 0;
                let mut rtt_ns_samples: Vec<u64> = vec![0; report_latency_every];
                let mut rtt_batch_count: usize = 0;
                let mut current_reply = None;
                let self_ref = t.create_inert();
                self_ref.become_entity(
                    syndicate::entity(Arc::clone(&self_ref))
                        .on_message(move |self_ref, t, m: AnyValue| {
                            match m.value().as_boolean() {
                                Some(true) => {
                                    tracing::info!("{:?} turns, {:?} events in the last second",
                                                   turn_counter,
                                                   event_counter);
                                    turn_counter = 0;
                                    event_counter = 0;
                                }
                                Some(false) => {
                                    current_reply = None;
                                }
                                None => {
                                    event_counter += 1;
                                    let bindings = m.value().to_sequence()?;
                                    let timestamp = &bindings[0];
                                    let padding = &bindings[1];

                                    if should_echo || (report_latency_every == 0) {
                                        ds.message(t, &(), &simple_record2(&send_label,
                                                                           timestamp.clone(),
                                                                           padding.clone()));
                                    } else {
                                        if let None = current_reply {
                                            turn_counter += 1;
                                            t.message_for_myself(&self_ref, AnyValue::new(false));
                                            let rtt_ns = now() - timestamp.value().to_u64()?;
                                            rtt_ns_samples[rtt_batch_count] = rtt_ns;
                                            rtt_batch_count += 1;

                                            if rtt_batch_count == report_latency_every {
                                                rtt_ns_samples.sort();
                                                report_latencies(&rtt_ns_samples);
                                                rtt_batch_count = 0;
                                            }

                                            current_reply = Some(
                                                simple_record2(&send_label,
                                                               Value::from(now()).wrap(),
                                                               padding.clone()));
                                        }
                                        ds.message(t, &(), current_reply.as_ref().expect("some reply"));
                                    }
                                }
                            }
                            Ok(())
                        }));
                Cap::new(&self_ref)
            };

            ds.assert(t, language(), &Observe {
                pattern: {
                    let recv_label = AnyValue::symbol(recv_label);
                    syndicate_macros::pattern!{<#(recv_label) $ $>}
                },
                observer: Arc::clone(&consumer),
            });

            t.linked_task(syndicate::name!("tick"), async move {
                let mut stats_timer = interval(Duration::from_secs(1));
                loop {
                    stats_timer.tick().await;
                    let consumer = Arc::clone(&consumer);
                    external_event(&Arc::clone(&consumer.underlying.mailbox),
                                   &Account::new(syndicate::name!("account")),
                                   Box::new(move |t| t.with_entity(
                                       &consumer.underlying,
                                       |t, e| e.message(t, AnyValue::new(true)))))?;
                }
            });

            if let PingPongMode::Ping(c) = &config.mode {
                let turn_count = c.turn_count;
                let action_count = c.action_count;
                let account = Arc::clone(t.account());
                t.linked_task(syndicate::name!("boot-ping"), async move {
                    let padding = AnyValue::bytestring(vec![0; bytes_padding]);
                    for _ in 0..turn_count {
                        let mut events: PendingEventQueue = vec![];
                        let current_rec = simple_record2(send_label,
                                                         Value::from(now()).wrap(),
                                                         padding.clone());
                        for _ in 0..action_count {
                            let ds = Arc::clone(&ds);
                            let current_rec = current_rec.clone();
                            events.push(Box::new(move |t| t.with_entity(
                                &ds.underlying,
                                |t, e| e.message(t, current_rec))));
                        }
                        external_events(&ds.underlying.mailbox, &account, events)?
                    }
                    Ok(LinkedTaskTermination::KeepFacet)
                });
            }

            Ok(None)
        });
        Ok(())
    }).await??;
    Ok(())
}
