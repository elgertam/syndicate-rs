//! Implements a [*dataspace*][crate::dataspace#GarnockJones2017]
//! entity.
//!
//! **References.**
//!
//! - Garnock-Jones, Tony. <a name="GarnockJones2017">“Conversational
//!   Concurrency.”</a> PhD, Northeastern University, 2017. [Available
//!   on the web](https://syndicate-lang.org/tonyg-dissertation/).
//!   [PDF](https://syndicate-lang.org/papers/conversational-concurrency-201712310922.pdf).

use super::language;
use super::skeleton;
use super::actor::*;
use super::schemas::dataspace::*;

use preserves::Map;
use preserves_schema::Codec;

/// A Dataspace object (entity).
#[derive(Debug)]
pub struct Dataspace {
    pub name: Name,
    /// Index over assertions placed in the dataspace; used to
    /// efficiently route assertion changes and messages to observers.
    pub index: skeleton::Index,
    /// Local memory of assertions indexed by `Handle`, used to remove
    /// assertions from the `index` when they are retracted.
    pub handle_map: Map<Handle, _Any>,
}

impl Dataspace {
    /// Construct a new, empty dataspace.
    pub fn new(name: Name) -> Self {
        Self {
            name,
            index: skeleton::Index::new(),
            handle_map: Map::new(),
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

    /// Retrieve the current count of [`Observe`] assertions in the dataspace.
    pub fn observer_count(&self) -> usize {
        self.index.observer_count()
    }
}

impl Entity<_Any> for Dataspace {
    fn assert(&mut self, t: &mut Activation, a: _Any, h: Handle) -> ActorResult {
        let is_new = self.index.insert(t, &a);
        tracing::trace!(dataspace = ?self.name, assertion = ?a, handle = ?h, ?is_new, "assert");

        if is_new {
            if let Ok(o) = language().parse::<Observe>(&a) {
                self.index.add_observer(t, &o.pattern, &o.observer);
            }
        }

        self.handle_map.insert(h, a);
        Ok(())
    }

    fn retract(&mut self, t: &mut Activation, h: Handle) -> ActorResult {
        match self.handle_map.remove(&h) {
            None => tracing::warn!(dataspace = ?self.name, handle = ?h, "retract of unknown handle"),
            Some(a) => {
                let is_last = self.index.remove(t, &a);
                tracing::trace!(dataspace = ?self.name, assertion = ?a, handle = ?h, ?is_last, "retract");

                if is_last {
                    if let Ok(o) = language().parse::<Observe>(&a) {
                        self.index.remove_observer(t, o.pattern, &o.observer);
                    }
                }
            }
        }
        Ok(())
    }

    fn message(&mut self, t: &mut Activation, m: _Any) -> ActorResult {
        tracing::trace!(dataspace = ?self.name, body = ?m, "message");
        self.index.send(t, &m);
        Ok(())
    }
}
