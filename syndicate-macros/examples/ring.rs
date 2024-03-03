use syndicate::actor::*;
use std::env;
use std::sync::Arc;

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

impl Entity<Instruction> for Forwarder {
    fn message(&mut self, turn: &mut Activation, message: Instruction) -> ActorResult {
        match message {
            Instruction::SetPeer(r) => {
                tracing::info!("Setting peer {:?}", r);
                self.peer = Some(r);
            }
            Instruction::HandleMessage(n) => {
                let target = if n >= self.hop_limit { &self.supervisor } else { self.peer.as_ref().expect("peer") };
                turn.message(target, Instruction::HandleMessage(n + 1));
            }
        }
        Ok(())
    }
}

struct Supervisor {
    latency_mode: bool,
    total_transfers: u64,
    remaining_to_receive: u32,
    start_time: Option<std::time::Instant>,
}

impl Entity<Instruction> for Supervisor {
    fn message(&mut self, turn: &mut Activation, message: Instruction) -> ActorResult {
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
                    turn.stop_root();
                }
            },
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> ActorResult {
    syndicate::convenient_logging()?;
    Actor::top(None, |t| {
        let args: Vec<String> = env::args().collect();
        let n_actors: u32 = args.get(1).unwrap_or(&"1000000".to_string()).parse()?;
        let n_rounds: u32 = args.get(2).unwrap_or(&"200".to_string()).parse()?;
        let latency_mode: bool = match args.get(3).unwrap_or(&"throughput".to_string()).as_str() {
            "latency" => true,
            "throughput" => false,
            _other => return Err("Invalid throughput/latency mode".into()),
        };
        tracing::info!("Will run {:?} actors for {:?} rounds", n_actors, n_rounds);

        let total_transfers: u64 = n_actors as u64 * n_rounds as u64;
        let (hop_limit, injection_count) = if latency_mode {
            (total_transfers, 1)
        } else {
            (n_rounds as u64, n_actors)
        };

        let me = t.create(Supervisor {
            latency_mode,
            total_transfers,
            remaining_to_receive: injection_count,
            start_time: None,
        });

        let mut forwarders: Vec<Arc<Ref<Instruction>>> = Vec::new();
        for _i in 0 .. n_actors {
            if _i % 10000 == 0 { tracing::info!("Actor {:?}", _i); }
            forwarders.push(
                t.spawn_for_entity(None, true, Box::new(
                    Forwarder {
                        hop_limit,
                        supervisor: me.clone(),
                        peer: forwarders.last().cloned(),
                    }))
                    .0.expect("an entity"));
        }
        t.message(&forwarders[0], Instruction::SetPeer(forwarders.last().expect("an entity").clone()));
        t.later(move |t| {
            t.message(&me, Instruction::SetPeer(me.clone()));
            t.later(move |t| {
                let mut injected: u32 = 0;
                for f in forwarders.into_iter() {
                    if injected >= injection_count {
                        break;
                    }
                    t.message(&f, Instruction::HandleMessage(0));
                    injected += 1;
                }
                Ok(())
            });
            Ok(())
        });
        Ok(())
    }).await??;
    Ok(())
}
