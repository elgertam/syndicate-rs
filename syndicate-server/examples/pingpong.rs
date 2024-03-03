use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;

use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::language;
use syndicate::relay;
use syndicate::schemas::dataspace::Observe;
use syndicate::sturdy;
use syndicate::value::NestedValue;
use syndicate::value::Value;

use tokio::net::TcpStream;

use core::time::Duration;

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

    #[structopt(short = "d", default_value = "b4b303726566b7b3036f6964b10973796e646963617465b303736967b21069ca300c1dbfa08fba692102dd82311a8484")]
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
async fn main() -> ActorResult {
    syndicate::convenient_logging()?;
    let config = Config::from_args();
    let sturdyref = sturdy::SturdyRef::from_hex(&config.dataspace)?;
    let (i, o) = TcpStream::connect("127.0.0.1:9001").await?.into_split();
    Actor::top(None, |t| {
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
                let current_reply = Arc::new(Mutex::new(None));
                Cap::new(&t.create(
                    syndicate::entity(())
                        .on_message(move |(), t, m: AnyValue| {
                            match m.value().as_boolean() {
                                Some(_) => {
                                    tracing::info!("{:?} turns, {:?} events in the last second",
                                                   turn_counter,
                                                   event_counter);
                                    turn_counter = 0;
                                    event_counter = 0;
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
                                        let mut g = current_reply.lock().expect("unpoisoned");
                                        if let None = *g {
                                            turn_counter += 1;
                                            t.pre_commit(enclose!((current_reply) move |_| {
                                                *current_reply.lock().expect("unpoisoned") = None;
                                                Ok(())
                                            }));
                                            let rtt_ns = now() - timestamp.value().to_u64()?;
                                            rtt_ns_samples[rtt_batch_count] = rtt_ns;
                                            rtt_batch_count += 1;

                                            if rtt_batch_count == report_latency_every {
                                                rtt_ns_samples.sort();
                                                report_latencies(&rtt_ns_samples);
                                                rtt_batch_count = 0;
                                            }

                                            *g = Some(simple_record2(&send_label,
                                                                     Value::from(now()).wrap(),
                                                                     padding.clone()));
                                        }
                                        ds.message(t, &(), g.as_ref().expect("some reply"));
                                    }
                                }
                            }
                            Ok(())
                        })))
            };

            ds.assert(t, language(), &Observe {
                pattern: {
                    let recv_label = AnyValue::symbol(recv_label);
                    syndicate_macros::pattern!{<#(recv_label) $ $>}
                },
                observer: Arc::clone(&consumer),
            });

            t.every(Duration::from_secs(1), move |t| {
                consumer.message(t, &(), &AnyValue::new(true));
                Ok(())
            })?;

            if let PingPongMode::Ping(c) = &config.mode {
                let facet = t.facet_ref();
                let turn_count = c.turn_count;
                let action_count = c.action_count;
                let account = Arc::clone(t.account());
                t.linked_task(Some(AnyValue::symbol("boot-ping")), async move {
                    let padding = AnyValue::bytestring(vec![0; bytes_padding]);
                    for _ in 0..turn_count {
                        let current_rec = simple_record2(send_label,
                                                         Value::from(now()).wrap(),
                                                         padding.clone());
                        facet.activate(&account, None, |t| {
                            for _ in 0..action_count {
                                ds.message(t, &(), &current_rec);
                            }
                            Ok(())
                        });
                    }
                    Ok(LinkedTaskTermination::KeepFacet)
                });
            }

            Ok(None)
        })
    }).await??;
    Ok(())
}
