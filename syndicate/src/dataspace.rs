//! Implements a [*dataspace*][crate::dataspace#GarnockJones2017]
//! entity.
//!
//! **References.**
//!
//! - Garnock-Jones, Tony. <a name="GarnockJones2017">“Conversational
//!   Concurrency.”</a> PhD, Northeastern University, 2017. [Available
//!   on the web](https://syndicate-lang.org/tonyg-dissertation/).
//!   [PDF](https://syndicate-lang.org/papers/conversational-concurrency-201712310922.pdf).

use super::skeleton;
use super::actor::*;
use super::schemas::dataspace::*;
use super::schemas::dataspace::_Any;

use preserves::value::Map;

use std::convert::TryFrom;

// #[derive(Debug)]
// pub struct Churn {
//     pub assertions_added: usize,
//     pub assertions_removed: usize,
//     pub endpoints_added: usize,
//     pub endpoints_removed: usize,
//     pub observers_added: usize,
//     pub observers_removed: usize,
//     pub messages_injected: usize,
//     pub messages_delivered: usize,
// }

// impl Churn {
//     pub fn new() -> Self {
//         Self {
//             assertions_added: 0,
//             assertions_removed: 0,
//             endpoints_added: 0,
//             endpoints_removed: 0,
//             observers_added: 0,
//             observers_removed: 0,
//             messages_injected: 0,
//             messages_delivered: 0,
//         }
//     }
//
//     pub fn reset(&mut self) {
//         *self = Churn::new()
//     }
// }

/// A Dataspace object (entity).
#[derive(Debug)]
pub struct Dataspace {
    /// Index over assertions placed in the dataspace; used to
    /// efficiently route assertion changes and messages to observers.
    pub index: skeleton::Index,
    /// Local memory of assertions indexed by `Handle`, used to remove
    /// assertions from the `index` when they are retracted.
    pub handle_map: Map<Handle, (_Any, Option<Observe>)>,
    // pub churn: Churn,
}

impl Dataspace {
    /// Construct a new, empty dataspace.
    pub fn new() -> Self {
        Self {
            index: skeleton::Index::new(),
            handle_map: Map::new(),
            // churn: Churn::new(),
        }
    }

    /// Retrieve the current count of *distinct* assertions placed in
    /// the dataspace.
    pub fn assertion_count(&self) -> usize {
        self.index.assertion_count()
    }

    /// Retrieve the current count of assertions, including
    /// duplicates, placed in the dataspace.
    pub fn endpoint_count(&self) -> isize {
        self.index.endpoint_count()
    }

    /// Retrieve the current count of
    /// [`Observe`][crate::schemas::dataspace::Observe] assertions in
    /// the dataspace.
    pub fn observer_count(&self) -> usize {
        self.index.observer_count()
    }
}

impl Entity<_Any> for Dataspace {
    fn assert(&mut self, t: &mut Activation, a: _Any, h: Handle) -> ActorResult {
        tracing::trace!(assertion = ?a, handle = ?h, "assert");

        // let old_assertions = self.index.assertion_count();
        self.index.insert(t, &a);
        // self.churn.assertions_added += self.index.assertion_count() - old_assertions;
        // self.churn.endpoints_added += 1;

        if let Ok(o) = Observe::try_from(&a) {
            self.index.add_observer(t, &o.pattern, &o.observer);
            // self.churn.observers_added += 1;
            self.handle_map.insert(h, (a, Some(o)));
        } else {
            self.handle_map.insert(h, (a, None));
        }
        Ok(())
    }

    fn retract(&mut self, t: &mut Activation, h: Handle) -> ActorResult {
        tracing::trace!(handle = ?h, "retract");

        if let Some((a, maybe_o)) = self.handle_map.remove(&h) {
            if let Some(o) = maybe_o {
                self.index.remove_observer(t, o.pattern, &o.observer);
                // self.churn.observers_removed += 1;
            }

            // let old_assertions = self.index.assertion_count();
            self.index.remove(t, &a);
            // self.churn.assertions_removed += old_assertions - self.index.assertion_count();
            // self.churn.endpoints_removed += 1;
        }
        Ok(())
    }

    fn message(&mut self, t: &mut Activation, m: _Any) -> ActorResult {
        tracing::trace!(body = ?m, "message");

        // self.index.send(t, &m, &mut self.churn.messages_delivered);
        self.index.send(t, &m);
        // self.churn.messages_injected += 1;
        Ok(())
    }
}
