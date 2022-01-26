//! Extremely simple single-actor supervision. Vastly simplified compared to the available
//! options in [Erlang/OTP](https://erlang.org/doc/man/supervisor.html).

use preserves::value::NestedValue;

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

use crate::actor::*;
use crate::schemas::service::State;

pub type Boot = Arc<Mutex<Box<dyn Send + FnMut(&mut Activation) -> ActorResult>>>;

#[derive(Debug)]
enum Protocol {
    SuperviseeStarted, // assertion
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
    my_name: Name,
    child_name: Name,
    config: SupervisorConfiguration,
    boot_fn: Boot,
    restarts: VecDeque<Instant>,
    state: Arc<Field<State>>,
    ac_ref: Option<ActorRef>,
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
        let _entry = tracing::info_span!("supervisor", name = ?self.child_name).entered();
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
            Protocol::Retry => {
                self.start_now(t)
            }
            _ => Ok(())
        }
    }

    fn stop(&mut self, _t: &mut Activation) -> ActorResult {
        tracing::info!(name = ?self.my_name,
                       self_ref = ?self.self_ref,
                       "Supervisor terminating");
        Ok(())
    }
}

impl Supervisor {
    pub fn start<C: 'static + Send + FnMut(&mut Activation, State) -> ActorResult,
                 B: 'static + Send + FnMut(&mut Activation) -> ActorResult>(
        t: &mut Activation,
        name: Name,
        config: SupervisorConfiguration,
        mut state_cb: C,
        boot_fn: B,
    ) -> ActorResult {
        let _entry = tracing::info_span!("supervisor", ?name).entered();
        tracing::trace!(?config);
        let self_ref = t.create_inert();
        let state_field = t.named_field("supervisee_state", State::Started);
        let my_name = name.as_ref().map(
            |n| preserves::rec![AnyValue::symbol("supervisor"), n.clone()]);
        let mut supervisor = Supervisor {
            self_ref: Arc::clone(&self_ref),
            my_name: my_name.clone(),
            child_name: name,
            config,
            boot_fn: Arc::new(Mutex::new(Box::new(boot_fn))),
            restarts: VecDeque::new(),
            state: Arc::clone(&state_field),
            ac_ref: None,
        };
        tracing::info!(self_ref = ?supervisor.self_ref, "Supervisor starting");
        supervisor.start_now(t)?;
        t.dataflow(move |t| {
            let state = t.get(&state_field).clone();
            tracing::debug!(name = ?my_name, ?state);
            state_cb(t, state)
        })?;
        self_ref.become_entity(supervisor);
        t.on_stop_notify(&self_ref);
        Ok(())
    }

    fn start_now(&mut self, t: &mut Activation) -> ActorResult {
        let boot_cell = Arc::clone(&self.boot_fn);
        t.facet(|t: &mut Activation| {
            t.assert(&self.self_ref, Protocol::SuperviseeStarted);
            self.ac_ref = Some(t.spawn_link(
                self.child_name.clone(),
                move |t| boot_cell.lock().expect("Unpoisoned boot_fn mutex")(t)));
            tracing::debug!(self_ref = ?self.self_ref,
                            supervisee = ?self.ac_ref,
                            "Supervisee started");
            Ok(())
        })?;
        Ok(())
    }
}
