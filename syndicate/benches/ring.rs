use criterion::{criterion_group, criterion_main, Criterion};

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use syndicate::actor::*;

use tokio::runtime::Runtime;

static ACTORS_CREATED: AtomicU64 = AtomicU64::new(0);
static MESSAGES_SENT: AtomicU64 = AtomicU64::new(0);

pub fn bench_ring(c: &mut Criterion) {
    syndicate::convenient_logging().unwrap();

    let rt = Runtime::new().unwrap();

    c.bench_function("Armstrong's Ring", |b| {
        // "Write a ring benchmark. Create N processes in a ring. Send a message round the ring
        // M times so that a total of N * M messages get sent. Time how long this takes for
        // different values of N and M."
        //   -- Joe Armstrong, "Programming Erlang: Software for a Concurrent World"
        //
        // Here we fix N = 1000, and let `iters` take on the role of M.
        //
        b.iter_custom(|iters| {
            const ACTOR_COUNT: u32 = 1000;
            ACTORS_CREATED.store(0, Ordering::SeqCst);
            MESSAGES_SENT.store(0, Ordering::SeqCst);
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            rt.block_on(async move {
                struct Forwarder {
                    next: Arc<Ref<()>>,
                }
                struct Counter {
                    start: Instant,
                    tx: std::sync::mpsc::SyncSender<Duration>,
                    remaining_to_send: u64,
                    iters: u64,
                    next: Arc<Ref<()>>,
                }
                struct Spawner {
                    self_ref: Arc<Ref<Arc<Ref<()>>>>, // !
                    tx: std::sync::mpsc::SyncSender<Duration>,
                    iters: u64,
                    i: u32,
                    c: Arc<Ref<()>>,
                }

                impl Entity<()> for Forwarder {
                    fn message(&mut self, t: &mut Activation, _message: ()) -> ActorResult {
                        MESSAGES_SENT.fetch_add(1, Ordering::Relaxed);
                        t.message(&self.next, ());
                        Ok(())
                    }
                }

                impl Counter {
                    fn step(&mut self, t: &mut Activation) -> ActorResult {
                        if self.remaining_to_send > 0 {
                            self.remaining_to_send -= 1;
                            MESSAGES_SENT.fetch_add(1, Ordering::Relaxed);
                            t.message(&self.next, ());
                        } else {
                            tracing::info!(iters = self.iters,
                                           actors_created = ACTORS_CREATED.load(Ordering::SeqCst),
                                           messages_sent = MESSAGES_SENT.load(Ordering::SeqCst));
                            t.stop();
                            self.tx.send(self.start.elapsed() / ACTOR_COUNT).unwrap()
                        }
                        Ok(())
                    }
                }

                impl Entity<()> for Counter {
                    fn message(&mut self, t: &mut Activation, _message: ()) -> ActorResult {
                        self.step(t)
                    }
                }

                impl Spawner {
                    fn step(&mut self, t: &mut Activation, next: Arc<Ref<()>>) -> ActorResult {
                        if self.i < ACTOR_COUNT {
                            let i = self.i;
                            self.i += 1;
                            let spawner_ref = Arc::clone(&self.self_ref);
                            ACTORS_CREATED.fetch_add(1, Ordering::Relaxed);
                            Actor::new().boot(syndicate::name!("forwarder", ?i), move |t| {
                                let _ = t.prevent_inert_check();
                                let f = t.create(Forwarder {
                                    next,
                                });
                                t.message(&spawner_ref, f);
                                Ok(())
                            });
                        } else {
                            let mut c_state = Counter {
                                start: Instant::now(),
                                tx: self.tx.clone(),
                                remaining_to_send: self.iters,
                                iters: self.iters,
                                next,
                            };
                            c_state.step(t)?;
                            self.c.become_entity(c_state);
                        }
                        Ok(())
                    }
                }

                impl Entity<Arc<Ref<()>>> for Spawner {
                    fn message(&mut self, t: &mut Activation, f: Arc<Ref<()>>) -> ActorResult {
                        self.step(t, f)
                    }
                }

                ACTORS_CREATED.fetch_add(1, Ordering::Relaxed);
                Actor::new().boot(syndicate::name!("counter"), move |t| {
                    let _ = t.prevent_inert_check();
                    let mut s = Spawner {
                        self_ref: t.create_inert(),
                        tx,
                        iters,
                        i: 1,
                        c: t.create_inert(),
                    };
                    s.step(t, Arc::clone(&s.c))?;
                    Arc::clone(&s.self_ref).become_entity(s);
                    Ok(())
                }).await.unwrap().unwrap();
            });
            rx.recv().unwrap()
        })
    });
}

criterion_group!(ring, bench_ring);
criterion_main!(ring);
