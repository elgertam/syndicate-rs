use criterion::{criterion_group, criterion_main, Criterion};
use futures::Sink;
use std::mem::drop;
use std::pin::Pin;
use std::sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}};
use std::task::{Context, Poll};
use std::thread;
use std::time::Instant;
use structopt::StructOpt;
use syndicate::peer::Peer;
use syndicate::{config, spaces, packets, value::{Value, IOValue}};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tracing::Level;

struct SinkTx<T> {
    tx: Option<UnboundedSender<T>>,
}

impl<T> SinkTx<T> {
    fn new(tx: UnboundedSender<T>) -> Self {
        SinkTx { tx: Some(tx) }
    }
}

impl<T> Sink<T> for SinkTx<T> {
    type Error = packets::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), packets::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, v: T) -> Result<(), packets::Error> {
        self.tx.as_ref().unwrap().send(v).map_err(|e| packets::Error::Message(e.to_string()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), packets::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), packets::Error>> {
        (&mut self).tx = None;
        Poll::Ready(Ok(()))
    }
}

#[inline]
fn says(who: IOValue, what: IOValue) -> IOValue {
    let mut r = Value::simple_record("Says", 2);
    r.fields_vec_mut().push(who);
    r.fields_vec_mut().push(what);
    r.finish().wrap()
}

pub fn bench_pub(c: &mut Criterion) {
    let filter = tracing_subscriber::filter::EnvFilter::from_default_env()
        .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into());
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_ansi(true)
        .with_max_level(Level::TRACE)
        .with_env_filter(filter)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Could not set tracing global subscriber");

    c.bench_function("publication alone", |b| {
        b.iter_custom(|iters| {
            let no_args: Vec<String> = vec![];
            let config = Arc::new(config::ServerConfig::from_iter(no_args.iter()));
            let spaces = Arc::new(Mutex::new(spaces::Spaces::new()));

            let (c2s_tx, c2s_rx) = unbounded_channel();
            let (s2c_tx, _s2c_rx) = unbounded_channel();
            let runtime_handle = thread::spawn(move || {
                let mut rt = Runtime::new().unwrap();
                rt.block_on(async {
                    Peer::new(0, c2s_rx, SinkTx::new(s2c_tx)).run(spaces, &config).await.unwrap();
                })
            });

            c2s_tx.send(Ok(packets::C2S::Connect(Value::from("bench_pub").wrap()))).unwrap();

            let turn = packets::C2S::Turn(vec![
                packets::Action::Message(says(Value::from("bench_pub").wrap(),
                                              Value::ByteString(vec![]).wrap()))]);

            let start = Instant::now();
            for _ in 0..iters {
                c2s_tx.send(Ok(turn.clone())).unwrap();
            }
            drop(c2s_tx);
            runtime_handle.join().unwrap();
            start.elapsed()
        })
    });

    c.bench_function("publication and subscription", |b| {
        b.iter_custom(|iters| {
            let no_args: Vec<String> = vec![];
            let config = Arc::new(config::ServerConfig::from_iter(no_args.iter()));
            let spaces = Arc::new(Mutex::new(spaces::Spaces::new()));

            let turn_count = Arc::new(AtomicU64::new(0));

            let (c2s_tx, c2s_rx) = unbounded_channel();
            let c2s_tx = Arc::new(c2s_tx);

            {
                let c2s_tx = c2s_tx.clone();

                c2s_tx.send(Ok(packets::C2S::Connect(Value::from("bench_pub").wrap()))).unwrap();

                let discard: IOValue = Value::simple_record0("discard").wrap();
                let capture: IOValue = Value::simple_record1("capture", discard).wrap();
                c2s_tx.send(Ok(packets::C2S::Turn(vec![
                    packets::Action::Assert(Value::from(0).wrap(),
                                            Value::simple_record1(
                                                "observe",
                                                says(Value::from("bench_pub").wrap(),
                                                     capture)).wrap())]))).unwrap();

                // tracing::info!("Sending {} messages", iters);
                let turn = packets::C2S::Turn(vec![
                    packets::Action::Message(says(Value::from("bench_pub").wrap(),
                                                  Value::ByteString(vec![]).wrap()))]);
                for _ in 0..iters {
                    c2s_tx.send(Ok(turn.clone())).unwrap();
                }

                c2s_tx.send(Ok(packets::C2S::Turn(vec![
                    packets::Action::Clear(Value::from(0).wrap())]))).unwrap();
            }

            let start = Instant::now();
            let runtime_handle = {
                let turn_count = turn_count.clone();
                let mut c2s_tx = Some(c2s_tx.clone());
                thread::spawn(move || {
                    let mut rt = Runtime::new().unwrap();
                    rt.block_on(async move {
                        let (s2c_tx, mut s2c_rx) = unbounded_channel();
                        let consumer_handle = tokio::spawn(async move {
                            while let Some(p) = s2c_rx.recv().await {
                                // tracing::info!("Consumer got {:?}", &p);
                                match p {
                                    packets::S2C::Ping() => (),
                                    packets::S2C::Turn(actions) => {
                                        for a in actions {
                                            match a {
                                                packets::Event::Msg(_, _) => {
                                                    turn_count.fetch_add(1, Ordering::Relaxed);
                                                },
                                                packets::Event::End(_) => {
                                                    c2s_tx.take();
                                                }
                                                _ => panic!("Unexpected action: {:?}", a),
                                            }
                                        }
                                    },
                                    _ => panic!("Unexpected packet: {:?}", p),
                                }
                            }
                            // tracing::info!("Consumer terminating");
                        });
                        Peer::new(0, c2s_rx, SinkTx::new(s2c_tx)).run(spaces, &config).await.unwrap();
                        consumer_handle.await.unwrap();
                    })
                })
            };
            drop(c2s_tx);
            runtime_handle.join().unwrap();
            let elapsed = start.elapsed();

            let actual_turns = turn_count.load(Ordering::SeqCst);
            if actual_turns != iters {
                panic!("Expected {}, got {} messages", iters, actual_turns);
            }
            elapsed
        })
    });
}

criterion_group!(publish, bench_pub);
criterion_main!(publish);
