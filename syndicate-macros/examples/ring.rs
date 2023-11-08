use syndicate::actor::*;
use std::env;
use std::sync::Arc;

#[derive(Debug)]
enum Instruction {
    SetPeer(Arc<Ref<Instruction>>),
    HandleMessage(u32),
}

struct Forwarder {
    n_rounds: u32,
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
                let target = if n >= self.n_rounds { &self.supervisor } else { self.peer.as_ref().expect("peer") };
                turn.message(target, Instruction::HandleMessage(n + 1));
            }
        }
        Ok(())
    }
}

struct Supervisor {
    n_actors: u32,
    n_rounds: u32,
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
                    let n_messages: u64 = self.n_actors as u64 * self.n_rounds as u64;
                    tracing::info!("Stop after {:?}; {:?} messages, so {:?} Hz",
                                   duration,
                                   n_messages,
                                   (1000.0 * n_messages as f64) / duration.as_millis() as f64);
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
        tracing::info!("Will run {:?} actors for {:?} rounds", n_actors, n_rounds);

        let me = t.create(Supervisor {
            n_actors,
            n_rounds,
            remaining_to_receive: n_actors,
            start_time: None,
        });

        let mut forwarders: Vec<Arc<Ref<Instruction>>> = Vec::new();
        for _i in 0 .. n_actors {
            if _i % 10000 == 0 { tracing::info!("Actor {:?}", _i); }
            forwarders.push(
                t.spawn_for_entity(None, true, Box::new(
                    Forwarder {
                        n_rounds,
                        supervisor: me.clone(),
                        peer: forwarders.last().cloned(),
                    }))
                    .0.expect("an entity"));
        }
        t.message(&forwarders[0], Instruction::SetPeer(forwarders.last().expect("an entity").clone()));
        t.later(move |t| {
            t.message(&me, Instruction::SetPeer(me.clone()));
            t.later(move |t| {
                for f in forwarders.into_iter() {
                    t.message(&f, Instruction::HandleMessage(0));
                }
                Ok(())
            });
            Ok(())
        });
        Ok(())
    }).await??;
    Ok(())
}
