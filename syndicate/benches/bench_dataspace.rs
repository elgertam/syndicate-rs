use criterion::{criterion_group, criterion_main, Criterion};

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use syndicate::language;
use syndicate::actor::*;
use syndicate::during::entity;
use syndicate::dataspace::Dataspace;
use syndicate::schemas::dataspace::Observe;
use syndicate::schemas::dataspace_patterns as p;
use syndicate::value::NestedValue;
use syndicate::value::Value;

use tokio::runtime::Runtime;

use tracing::Level;

#[inline]
fn says(who: AnyValue, what: AnyValue) -> AnyValue {
    let mut r = Value::simple_record("Says", 2);
    r.fields_vec_mut().push(who);
    r.fields_vec_mut().push(what);
    r.finish().wrap()
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
                        t.message(&ds, says(AnyValue::new("bench_pub"),
                                            Value::ByteString(vec![]).wrap()));
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

                        ds.assert(t, language(), &Observe {
                            pattern: p::Pattern::DBind(Box::new(p::DBind {
                                pattern: p::Pattern::DLit(Box::new(p::DLit {
                                    value: p::AnyAtom::Symbol("consumer".to_owned()),
                                })),
                            })),
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

                            ds.assert(t, &(), &AnyValue::symbol("consumer"));
                            ds.assert(t, language(), &Observe {
                                pattern: p::Pattern::DCompound(Box::new(p::DCompound::Rec {
                                    label: AnyValue::symbol("Says"),
                                    fields: vec![
                                        p::Pattern::DLit(Box::new(p::DLit {
                                            value: p::AnyAtom::String("bench_pub".to_owned()),
                                        })),
                                        p::Pattern::DBind(Box::new(p::DBind {
                                            pattern: p::Pattern::DDiscard(Box::new(p::DDiscard)),
                                        })),
                                    ]})),
                                observer: receiver,
                            });
                            ds.assert(t, language(), &Observe {
                                pattern: p::Pattern::DBind(Box::new(p::DBind {
                                    pattern: p::Pattern::DLit(Box::new(p::DLit {
                                        value: p::AnyAtom::Bool(true),
                                    })),
                                })),
                                observer: shutdown,
                            });

                            t.after(core::time::Duration::from_secs(0), move |t| {
                                for _i in 0..iters {
                                    ds.message(t, &(), &says(AnyValue::new("bench_pub"),
                                                             Value::ByteString(vec![]).wrap()));
                                }
                                ds.message(t, &(), &AnyValue::new(true));
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
