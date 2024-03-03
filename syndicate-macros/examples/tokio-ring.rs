use std::env;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

type Ref<T> = UnboundedSender<T>;

#[derive(Debug)]
enum Instruction {
    SetPeer(Arc<Ref<Instruction>>),
    HandleMessage(u64),
}

struct Forwarder {
    hop_limit: u64,
    supervisor: Arc<Ref<Instruction>>,
    peer: Option<Arc<Ref<Instruction>>>,
}

impl Drop for Forwarder {
    fn drop(&mut self) {
        let r = self.peer.take();
        let _ = tokio::spawn(async move {
            drop(r);
        });
    }
}

enum Action { Continue, Stop }

trait Actor<T> {
    fn message(&mut self, message: T) -> Action;
}

fn send<T: std::marker::Send + 'static>(ch: &Arc<Ref<T>>, message: T) -> () {
    match ch.send(message) {
        Ok(()) => (),
        Err(v) => panic!("Aiee! Could not send {:?}", v),
    }
}

fn spawn<T: std::marker::Send + 'static, R: Actor<T> + std::marker::Send + 'static>(rt: Option<Arc<AtomicU64>>, mut ac: R) -> Arc<Ref<T>> {
    let (tx, mut rx) = unbounded_channel();
    if let Some(ref c) = rt {
        c.fetch_add(1, Ordering::SeqCst);
    }
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                None => break,
                Some(message) => {
                    match ac.message(message) {
                        Action::Continue => continue,
                        Action::Stop => break,
                    }
                }
            }
        }
        if let Some(c) = rt {
            c.fetch_sub(1, Ordering::SeqCst);
        }
    });
    Arc::new(tx)
}

impl Actor<Instruction> for Forwarder {
    fn message(&mut self, message: Instruction) -> Action {
        match message {
            Instruction::SetPeer(r) => {
                tracing::info!("Setting peer {:?}", r);
                self.peer = Some(r);
            }
            Instruction::HandleMessage(n) => {
                let target = if n >= self.hop_limit { &self.supervisor } else { self.peer.as_ref().expect("peer") };
                send(target, Instruction::HandleMessage(n + 1));
            }
        }
        Action::Continue
    }
}

struct Supervisor {
    latency_mode: bool,
    total_transfers: u64,
    remaining_to_receive: u32,
    start_time: Option<std::time::Instant>,
}

impl Actor<Instruction> for Supervisor {
    fn message(&mut self, message: Instruction) -> Action {
        match message {
            Instruction::SetPeer(_) => {
                tracing::info!("Start");
                self.start_time = Some(std::time::Instant::now());
            },
            Instruction::HandleMessage(_n) => {
                self.remaining_to_receive -= 1;
                if self.remaining_to_receive == 0 {
                    let stop_time = std::time::Instant::now();
                    let duration = stop_time - self.start_time.unwrap();
                    tracing::info!("Stop after {:?}; {:?} messages, so {:?} Hz ({} mode)",
                                   duration,
                                   self.total_transfers,
                                   (1000.0 * self.total_transfers as f64) / duration.as_millis() as f64,
                                   if self.latency_mode { "latency" } else { "throughput" });
                    return Action::Stop;
                }
            },
        }
        Action::Continue
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + std::marker::Send + std::marker::Sync>> {
    syndicate::convenient_logging()?;

    let args: Vec<String> = env::args().collect();
    let n_actors: u32 = args.get(1).unwrap_or(&"1000000".to_string()).parse()?;
    let n_rounds: u32 = args.get(2).unwrap_or(&"200".to_string()).parse()?;
    let latency_mode: bool = match args.get(3).unwrap_or(&"throughput".to_string()).as_str() {
        "latency" => true,
        "throughput" => false,
        _other => return Err("Invalid throughput/latency mode".into()),
    };
    tracing::info!("Will run {:?} actors for {:?} rounds", n_actors, n_rounds);

    let count = Arc::new(AtomicU64::new(0));

    let total_transfers: u64 = n_actors as u64 * n_rounds as u64;
    let (hop_limit, injection_count) = if latency_mode {
        (total_transfers, 1)
    } else {
        (n_rounds as u64, n_actors)
    };

    let me = spawn(Some(count.clone()), Supervisor {
        latency_mode,
        total_transfers,
        remaining_to_receive: injection_count,
        start_time: None,
    });

    let mut forwarders: Vec<Arc<Ref<Instruction>>> = Vec::new();
    for _i in 0 .. n_actors {
        if _i % 10000 == 0 { tracing::info!("Actor {:?}", _i); }
        forwarders.push(spawn(None, Forwarder {
            hop_limit,
            supervisor: me.clone(),
            peer: forwarders.last().cloned(),
        }));
    }
    send(&forwarders[0], Instruction::SetPeer(forwarders.last().expect("an entity").clone()));
    send(&me, Instruction::SetPeer(me.clone()));

    let mut injected: u32 = 0;
    for f in forwarders.into_iter() {
        if injected >= injection_count {
            break;
        }
        send(&f, Instruction::HandleMessage(0));
        injected += 1;
    }

    loop {
        if count.load(Ordering::SeqCst) == 0 {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    Ok(())
}
