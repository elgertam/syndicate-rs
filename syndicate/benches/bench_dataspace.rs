use criterion::{criterion_group, criterion_main, Criterion};

use std::iter::FromIterator;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use syndicate::actor::*;
use syndicate::during::entity;
use syndicate::dataspace::Dataspace;
use syndicate::schemas::dataspace::Observe;
use syndicate::schemas::dataspace_patterns as p;
use syndicate::value::Map;
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
        t.stop();
        Ok(())
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
                Actor::new().boot(syndicate::name!("dataspace"), move |t| {
                    let ds = t.create(Dataspace::new());
                    let shutdown = t.create(ShutdownEntity);
                    let account = Account::new(syndicate::name!("sender-account"));
                    t.linked_task(syndicate::name!("sender"), async move {
                        for _ in 0..iters {
                            let ds = Arc::clone(&ds);
                            external_event(&Arc::clone(&ds.mailbox), &account, Box::new(
                                move |t| t.with_entity(
                                    &ds,
                                    |t, e| e.message(t, says(AnyValue::new("bench_pub"),
                                                             Value::ByteString(vec![]).wrap())))))?
                        }
                        external_event(&Arc::clone(&shutdown.mailbox), &account, Box::new(
                            move |t| t.with_entity(
                                &shutdown,
                                |t, e| e.message(t, AnyValue::new(true)))))?;
                        Ok(())
                    });
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

                Actor::new().boot(syndicate::name!("dataspace"), {
                    let iters = iters.clone();
                    let turn_count = Arc::clone(&turn_count);

                    move |t| {
                        let ds = Cap::new(&t.create(Dataspace::new()));

                        let shutdown = entity(())
                            .on_asserted(|_, _, _| {
                                Ok(Some(Box::new(|_, t| {
                                    t.stop();
                                    Ok(())
                                })))
                            })
                            .create_cap(t);

                        ds.assert(t, &Observe {
                            pattern: p::Pattern::DBind(Box::new(p::DBind {
                                pattern: p::Pattern::DLit(Box::new(p::DLit {
                                    value: AnyValue::symbol("consumer"),
                                })),
                            })),
                            observer: shutdown,
                        });

                        Actor::new().boot(syndicate::name!("consumer"), move |t| {
                            struct Receiver(Arc<AtomicU64>);
                            impl Entity<AnyValue> for Receiver {
                                fn message(&mut self, _t: &mut Activation, _m: AnyValue) -> ActorResult {
                                    self.0.fetch_add(1, Ordering::Relaxed);
                                    Ok(())
                                }
                            }

                            let shutdown = Cap::new(&t.create(ShutdownEntity));
                            let receiver = Cap::new(&t.create(Receiver(Arc::clone(&turn_count))));

                            ds.assert(t, Value::<AnyValue, _>::symbol("consumer").wrap());
                            ds.assert(t, &Observe {
                                pattern: p::Pattern::DCompound(Box::new(p::DCompound::Rec {
                                    ctor: Box::new(p::CRec {
                                        label: AnyValue::symbol("Says"),
                                        arity: 2.into(),
                                    }),
                                    members: Map::from_iter(vec![
                                        (0.into(), p::Pattern::DLit(Box::new(p::DLit {
                                            value: AnyValue::new("bench_pub"),
                                        }))),
                                        (1.into(), p::Pattern::DBind(Box::new(p::DBind {
                                            pattern: p::Pattern::DDiscard(Box::new(p::DDiscard)),
                                        }))),
                                    ].into_iter()),
                                })),
                                observer: receiver,
                            });
                            ds.assert(t, &Observe {
                                pattern: p::Pattern::DBind(Box::new(p::DBind {
                                    pattern: p::Pattern::DLit(Box::new(p::DLit {
                                        value: AnyValue::new(true),
                                    })),
                                })),
                                observer: shutdown,
                            });

                            let account = Arc::clone(t.account());
                            t.linked_task(syndicate::name!("sender"), async move {
                                for _i in 0..iters {
                                    let ds = Arc::clone(&ds);
                                    external_event(&Arc::clone(&ds.underlying.mailbox), &account, Box::new(
                                        move |t| t.with_entity(
                                            &ds.underlying,
                                            |t, e| e.message(t, says(AnyValue::new("bench_pub"),
                                                                     Value::ByteString(vec![]).wrap())))))?
                                }
                                {
                                    let ds = Arc::clone(&ds);
                                    external_event(&Arc::clone(&ds.underlying.mailbox), &account, Box::new(
                                        move |t| t.with_entity(
                                            &ds.underlying,
                                            |t, e| e.message(t, AnyValue::new(true)))))?;
                                }
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
