//! Extremely simple single-actor supervision. Vastly simplified compared to the available
//! options in [Erlang/OTP](https://erlang.org/doc/man/supervisor.html).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::actor::*;

enum Protocol<Boot: 'static + Send + FnMut(&mut Activation) -> ActorResult> {
    SuperviseeStarted, // assertion
    BootFunction(Boot), // message
    Retry, // message
}

#[derive(Debug)]
pub struct SupervisorConfiguration {
    pub intensity: usize,
    pub period: Duration,
    pub pause_time: Duration,
    pub sleep_time: Duration,
}

pub struct Supervisor<Boot: 'static + Send + FnMut(&mut Activation) -> ActorResult> {
    self_ref: Arc<Ref<Protocol<Boot>>>,
    name: tracing::Span,
    config: SupervisorConfiguration,
    boot_fn: Option<Boot>,
    restarts: VecDeque<Instant>,
    supervisee: Supervisee,
}

#[derive(Debug, PartialEq, Eq)]
enum Supervisee {
    NotRunning,
    Booting,
    Running,
    Recovering,
}

impl<Boot: 'static + Send + FnMut(&mut Activation) -> ActorResult> std::fmt::Debug for Protocol<Boot> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Protocol::SuperviseeStarted => write!(f, "Protocol::SuperviseeStarted"),
            Protocol::BootFunction(_) => write!(f, "Protocol::BootFunction(_)"),
            Protocol::Retry => write!(f, "Protocol::Retry"),
        }
    }
}

impl Default for SupervisorConfiguration {
    fn default() -> Self {
        Self {
            intensity: 1,
            period: Duration::from_secs(5),
            pause_time: Duration::from_millis(200),
            sleep_time: Duration::from_secs(10),
        }
    }
}

impl<Boot: 'static + Send + FnMut(&mut Activation) -> ActorResult>
    Entity<Protocol<Boot>> for Supervisor<Boot>
{
    fn assert(&mut self, _t: &mut Activation, m: Protocol<Boot>, _h: Handle) -> ActorResult {
        match m {
            Protocol::SuperviseeStarted => self.enter_state(Supervisee::Booting),
            _ => (),
        }
        Ok(())
    }

    fn retract(&mut self, t: &mut Activation, _h: Handle) -> ActorResult {
        self.enter_state(Supervisee::Recovering);
        let now = Instant::now();
        let oldest_to_keep = now - self.config.period;
        self.restarts.push_back(now);
        while let Some(stamp) = self.restarts.front() {
            if stamp < &oldest_to_keep {
                self.restarts.pop_front();
            } else {
                break;
            }
        }
        let self_ref = Arc::clone(&self.self_ref);
        let wait_time = if self.restarts.len() > self.config.intensity {
            self.config.sleep_time
        } else {
            self.config.pause_time
        };
        t.after(wait_time, Box::new(move |t| {
            t.message(&self_ref, Protocol::Retry);
            Ok(())
        }));
        Ok(())
    }

    fn message(&mut self, t: &mut Activation, m: Protocol<Boot>) -> ActorResult {
        match m {
            Protocol::BootFunction(b) => {
                self.enter_state(Supervisee::Running);
                self.boot_fn = Some(b);
                Ok(())
            }
            Protocol::Retry => {
                self.enter_state(Supervisee::NotRunning);
                self.ensure_started(t)
            }
            _ => Ok(())
        }
    }
}

impl<Boot: 'static + Send + FnMut(&mut Activation) -> ActorResult> Supervisor<Boot> {
    pub fn start(
        t: &mut Activation,
        name: tracing::Span,
        config: SupervisorConfiguration,
        boot_fn: Boot,
    ) {
        tracing::trace!(?config);
        let self_ref = t.create_inert();
        let mut supervisor = Supervisor {
            self_ref: Arc::clone(&self_ref),
            name,
            config,
            boot_fn: Some(boot_fn),
            restarts: VecDeque::new(),
            supervisee: Supervisee::NotRunning,
        };

        // In cases where we are the only Entity in our Facet, and our
        // supervisee terminates, we will often be "inert" until we
        // can restart it. So we prevent_inert_check to signal to the
        // system that there's something going on for that moment of
        // time between the supervisee terminating and our responding
        // to it.
        let _ = t.prevent_inert_check();

        supervisor.ensure_started(t).unwrap();
        self_ref.become_entity(supervisor);
    }

    fn enter_state(&mut self, supervisee: Supervisee) {
        let _entry = self.name.enter();
        tracing::info!("{:?} --> {:?}", self.supervisee, supervisee);
        self.supervisee = supervisee;
    }

    fn ensure_started(&mut self, t: &mut Activation) -> ActorResult {
        match self.boot_fn.take() {
            None =>
                tracing::error!("Cannot restart supervisee, because it panicked at startup"),
            Some(mut boot_fn) => {
                let self_ref = Arc::clone(&self.self_ref);
                t.facet(|t: &mut Activation| {
                    t.assert(&self.self_ref, Protocol::SuperviseeStarted);
                    t.spawn_link(crate::name!(parent: &self.name, "supervisee"), move |t| {
                        match boot_fn(t) {
                            Ok(()) => {
                                t.message(&self_ref, Protocol::BootFunction(boot_fn));
                                Ok(())
                            }
                            Err(e) => {
                                t.clear();
                                t.message(&self_ref, Protocol::BootFunction(boot_fn));
                                t.deliver();
                                Err(e)
                            }
                        }
                    });
                    Ok(())
                })?;
            }
        }
        Ok(())
    }
}
