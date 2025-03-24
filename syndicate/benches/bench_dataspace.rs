use criterion::{criterion_group, criterion_main, Criterion};

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use syndicate::actor::*;
use syndicate::during::entity;
use syndicate::dataspace::Dataspace;
use syndicate::schemas::dataspace::Observe;
use syndicate::schemas::dataspace_patterns as p;
use preserves::Map;
use preserves::Record;
use preserves::Value;

use tokio::runtime::Runtime;

use tracing::Level;

#[inline]
fn says(who: AnyValue, what: AnyValue) -> AnyValue {
    Value::new(Record::_from_vec(vec![Value::symbol("Says"), who, what]))
}

struct ShutdownEntity;

impl Entity<AnyValue> for ShutdownEntity {
    fn message(&mut self, t: &mut Activation, _m: AnyValue) -> ActorResult {
        Ok(t.stop())
    }
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

    let rt = Runtime::new().unwrap();

    c.bench_function("publication alone", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            rt.block_on(async move {
                Actor::top(None, move |t| {
                    let _ = t.prevent_inert_check();
                    // The reason this works is that all the messages to `ds` will be delivered
                    // before the message to `shutdown`, because `ds` and `shutdown` are in the
                    // same Actor.
                    let ds = t.create(Dataspace::new(None));
                    let shutdown = t.create(ShutdownEntity);
                    for _ in 0..iters {
                        t.message(&ds, says(AnyValue::new("bench_pub"), Value::bytes(vec![])));
                    }
                    t.message(&shutdown, AnyValue::new(true));
                    Ok(())
                }).await.unwrap().unwrap();
            });
            start.elapsed()
        })
    });

    c.bench_function("publication and subscription", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            rt.block_on(async move {
                let turn_count = Arc::new(AtomicU64::new(0));

                Actor::top(None, {
                    let iters = iters.clone();
                    let turn_count = Arc::clone(&turn_count);

                    move |t| {
                        let ds = Cap::new(&t.create(Dataspace::new(None)));

                        let shutdown = entity(())
                            .on_asserted(|_, _, _| Ok(Some(Box::new(|_, t| Ok(t.stop())))))
                            .create_cap(t);

                        ds.assert(t, &Observe {
                            pattern: p::Pattern::Bind {
                                pattern: Box::new(p::Pattern::Lit {
                                    value: p::AnyAtom::Symbol("consumer".to_owned()),
                                }),
                            },
                            observer: shutdown,
                        });

                        t.spawn(Some(AnyValue::symbol("consumer")), move |t| {
                            struct Receiver(Arc<AtomicU64>);
                            impl Entity<AnyValue> for Receiver {
                                fn message(&mut self, _t: &mut Activation, _m: AnyValue) -> ActorResult {
                                    self.0.fetch_add(1, Ordering::Relaxed);
                                    Ok(())
                                }
                            }

                            let shutdown = Cap::new(&t.create(ShutdownEntity));
                            let receiver = Cap::new(&t.create(Receiver(Arc::clone(&turn_count))));

                            ds.assert(t, &AnyValue::symbol("consumer"));
                            ds.assert(t, &Observe {
                                pattern: p::Pattern::Group {
                                    type_: p::GroupType::Rec {
                                        label: AnyValue::symbol("Says"),
                                    },
                                    entries: Map::from([
                                        (p::_Any::new(0), p::Pattern::Lit {
                                            value: p::AnyAtom::String("bench_pub".to_owned()),
                                        }),
                                        (p::_Any::new(1), p::Pattern::Bind {
                                            pattern: Box::new(p::Pattern::Discard),
                                        }),
                                    ]),
                                },
                                observer: receiver,
                            });
                            ds.assert(t, &Observe {
                                pattern: p::Pattern::Bind {
                                    pattern: Box::new(p::Pattern::Lit {
                                        value: p::AnyAtom::Bool(true),
                                    }),
                                },
                                observer: shutdown,
                            });

                            t.after(core::time::Duration::from_secs(0), move |t| {
                                for _i in 0..iters {
                                    ds.message(t, &says(AnyValue::new("bench_pub"),
                                                        Value::bytes(vec![])));
                                }
                                ds.message(t, &AnyValue::new(true));
                                Ok(())
                            });

                            Ok(())
                        });
                        Ok(())
                    }
                }).await.unwrap().unwrap();

                let actual_turns = turn_count.load(Ordering::SeqCst);
                if actual_turns != iters {
                    panic!("Expected {}, got {} messages", iters, actual_turns);
                }
            });
            start.elapsed()
        })
    });
}

criterion_group!(publish, bench_pub);
criterion_main!(publish);
