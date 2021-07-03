use super::Assertion;
use super::Handle;
use super::skeleton;
use super::actor::*;
use super::schemas::dataspace::*;

use preserves::value::Map;

use std::convert::TryFrom;

#[derive(Debug)]
pub struct Churn {
    pub assertions_added: usize,
    pub assertions_removed: usize,
    pub endpoints_added: usize,
    pub endpoints_removed: usize,
    pub observers_added: usize,
    pub observers_removed: usize,
    pub messages_injected: usize,
    pub messages_delivered: usize,
}

impl Churn {
    pub fn new() -> Self {
        Self {
            assertions_added: 0,
            assertions_removed: 0,
            endpoints_added: 0,
            endpoints_removed: 0,
            observers_added: 0,
            observers_removed: 0,
            messages_injected: 0,
            messages_delivered: 0,
        }
    }

    pub fn reset(&mut self) {
        *self = Churn::new()
    }
}

#[derive(Debug)]
pub struct Dataspace {
    pub index: skeleton::Index,
    pub handle_map: Map<Handle, (Assertion, Option<Observe>)>,
    pub churn: Churn,
}

impl Dataspace {
    pub fn new() -> Self {
        Self {
            index: skeleton::Index::new(),
            handle_map: Map::new(),
            churn: Churn::new(),
        }
    }

    pub fn assertion_count(&self) -> usize {
        self.index.assertion_count()
    }

    pub fn endpoint_count(&self) -> isize {
        self.index.endpoint_count()
    }

    pub fn observer_count(&self) -> usize {
        self.index.observer_count()
    }
}

impl Entity for Dataspace {
    fn assert(&mut self, t: &mut Activation, a: Assertion, h: Handle) -> ActorResult {
        tracing::trace!(action = debug(&a), "assert");

        let old_assertions = self.index.assertion_count();
        self.index.insert(t, &a);
        self.churn.assertions_added += self.index.assertion_count() - old_assertions;
        self.churn.endpoints_added += 1;

        if let Ok(o) = Observe::try_from(&a) {
            self.index.add_observer(t, &o.pattern, &o.observer);
            self.churn.observers_added += 1;
            self.handle_map.insert(h, (a, Some(o)));
        } else {
            self.handle_map.insert(h, (a, None));
        }
        Ok(())
    }

    fn retract(&mut self, t: &mut Activation, h: Handle) -> ActorResult {
        if let Some((a, maybe_o)) = self.handle_map.remove(&h) {
            if let Some(o) = maybe_o {
                self.index.remove_observer(o.pattern, &o.observer);
                self.churn.observers_removed += 1;
            }

            let old_assertions = self.index.assertion_count();
            self.index.remove(t, &a);
            self.churn.assertions_removed += old_assertions - self.index.assertion_count();
            self.churn.endpoints_removed += 1;
        }
        Ok(())
    }

    fn message(&mut self, t: &mut Activation, m: Assertion) -> ActorResult {
        self.index.send(t, &m, &mut self.churn.messages_delivered);
        self.churn.messages_injected += 1;
        Ok(())
    }
}
