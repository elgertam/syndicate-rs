//! Extremely simple single-actor supervision. Vastly simplified compared to the available
//! options in [Erlang/OTP](https://erlang.org/doc/man/supervisor.html).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::actor::*;
use crate::enclose;
use crate::schemas::service::State;

pub type Boot = Box<dyn Send + FnMut(&mut Activation) -> ActorResult>;

enum Protocol {
    SuperviseeStarted, // assertion
    BootFunction(Boot), // message
    Retry, // message
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestartPolicy {
    Always,
    OnErrorOnly,
}

#[derive(Debug, Clone)]
pub struct SupervisorConfiguration {
    pub intensity: usize,
    pub period: Duration,
    pub pause_time: Duration,
    pub sleep_time: Duration,
    pub restart_policy: RestartPolicy,
}

pub struct Supervisor {
    self_ref: Arc<Ref<Protocol>>,
    name: tracing::Span,
    config: SupervisorConfiguration,
    boot_fn: Option<Boot>,
    restarts: VecDeque<Instant>,
    state: Arc<Field<State>>,
    ac_ref: Option<ActorRef>,
}

impl std::fmt::Debug for Protocol {
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
            restart_policy: RestartPolicy::Always,
        }
    }
}

impl SupervisorConfiguration {
    pub fn on_error_only() -> Self {
        Self {
            restart_policy: RestartPolicy::OnErrorOnly,
            .. Self::default()
        }
    }
}

impl Entity<Protocol> for Supervisor
{
    fn assert(&mut self, t: &mut Activation, m: Protocol, _h: Handle) -> ActorResult {
        match m {
            Protocol::SuperviseeStarted => t.set(&self.state, State::Started),
            _ => Err(format!("Unexpected assertion: {:?}", m).as_str())?,
        }
        Ok(())
    }

    fn retract(&mut self, t: &mut Activation, _h: Handle) -> ActorResult {
        let _name = self.name.clone();
        let _entry = _name.enter();
        let exit_status =
            self.ac_ref.take().expect("valid supervisee ActorRef")
            .exit_status()
            .expect("supervisee to have terminated");
        tracing::debug!(?exit_status);
        match exit_status {
            Ok(()) if self.config.restart_policy == RestartPolicy::OnErrorOnly => {
                tracing::trace!("Not restarting: normal exit, restart_policy is OnErrorOnly");
                t.set(&self.state, State::Complete);
            },
            _ => {
                tracing::trace!("Restarting: restart_policy is Always or exit was abnormal");
                t.set(&self.state,
                      if exit_status.is_ok() { State::Complete } else { State::Failed });
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
                    tracing::warn!(?self.config.sleep_time, "Restart intensity exceeded; sleeping");
                    self.config.sleep_time
                } else {
                    tracing::trace!(?self.config.pause_time, "pausing");
                    self.config.pause_time
                };
                t.after(wait_time, move |t| {
                    tracing::trace!("Sending retry trigger");
                    t.message(&self_ref, Protocol::Retry);
                    Ok(())
                });
            },
        }
        Ok(())
    }

    fn message(&mut self, t: &mut Activation, m: Protocol) -> ActorResult {
        match m {
            Protocol::BootFunction(b) => {
                self.boot_fn = Some(b);
                Ok(())
            }
            Protocol::Retry => {
                self.ensure_started(t)
            }
            _ => Ok(())
        }
    }

    fn stop(&mut self, _t: &mut Activation) -> ActorResult {
        let _entry = self.name.enter();
        tracing::info!("Supervisor terminating");
        Ok(())
    }
}

impl Supervisor {
    pub fn start<C: 'static + Send + FnMut(&mut Activation, State) -> ActorResult,
                 B: 'static + Send + FnMut(&mut Activation) -> ActorResult>(
        t: &mut Activation,
        name: tracing::Span,
        config: SupervisorConfiguration,
        mut state_cb: C,
        boot_fn: B,
    ) -> ActorResult {
        let _entry = name.enter();
        tracing::trace!(?config);
        let self_ref = t.create_inert();
        let state_field = t.named_field("supervisee_state", State::Started);
        let mut supervisor = Supervisor {
            self_ref: Arc::clone(&self_ref),
            name: name.clone(),
            config,
            boot_fn: Some(Box::new(boot_fn)),
            restarts: VecDeque::new(),
            state: Arc::clone(&state_field),
            ac_ref: None,
        };
        supervisor.ensure_started(t)?;
        t.dataflow(enclose!((name) move |t| {
            let state = t.get(&state_field).clone();
            {
                let _entry = name.enter();
                tracing::debug!(?state);
            }
            state_cb(t, state)
        }))?;
        self_ref.become_entity(supervisor);
        t.on_stop_notify(&self_ref);
        Ok(())
    }

    fn ensure_started(&mut self, t: &mut Activation) -> ActorResult {
        match self.boot_fn.take() {
            None => {
                let _entry = self.name.enter();
                t.set(&self.state, State::Failed);
                tracing::error!("Cannot restart supervisee, because it panicked at startup")
            }
            Some(mut boot_fn) => {
                let self_ref = Arc::clone(&self.self_ref);
                t.facet(|t: &mut Activation| {
                    t.assert(&self.self_ref, Protocol::SuperviseeStarted);
                    self.ac_ref = Some(t.spawn_link(
                        crate::name!(parent: &self.name, "supervisee"),
                        move |t| {
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
                        }));
                    Ok(())
                })?;
            }
        }
        Ok(())
    }
}
