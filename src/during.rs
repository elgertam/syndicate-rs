use crate::actor::*;
use crate::error::Error;

use preserves::value::Map;

use std::any::Any;
use std::sync::Arc;

pub type DuringRetractionHandler<T> = Box<dyn Send + Sync + FnOnce(&mut T, &mut Activation) -> ActorResult>;
pub struct During<T>(Map<Handle, DuringRetractionHandler<T>>);

pub type DuringResult<E> =
    Result<Option<Box<dyn 'static + Send + Sync + FnOnce(&mut E, &mut Activation) -> ActorResult>>,
           Error>;

pub struct DuringEntity<E, Fa, Fm>
where
    E: 'static + Send + Sync,
    Fa: 'static + Send + Sync + FnMut(&mut E, &mut Activation, _Any) -> DuringResult<E>,
    Fm: 'static + Send + Sync + FnMut(&mut E, &mut Activation, _Any) -> ActorResult,
{
    state: E,
    assertion_handler: Option<Fa>,
    message_handler: Option<Fm>,
    during: During<E>,
}

impl<T> During<T> {
    pub fn new() -> Self {
        During(Map::new())
    }

    pub fn await_retraction<F: 'static + Send + Sync + FnOnce(&mut T, &mut Activation) -> ActorResult>(
        &mut self,
        h: Handle,
        f: F,
    ) -> ActorResult {
        self.0.insert(h, Box::new(f));
        Ok(())
    }

    pub fn retract(&mut self, h: Handle) -> DuringRetractionHandler<T> {
        self.0.remove(&h).unwrap_or_else(|| Box::new(|_, _| Ok(())))
    }
}

pub fn entity<E>(
    state: E
) -> DuringEntity<E,
                  fn (&mut E, &mut Activation, _Any) -> DuringResult<E>,
                  fn (&mut E, &mut Activation, _Any) -> ActorResult>
where
    E: 'static + Send + Sync,
{
    DuringEntity::new(state, None, None)
}

impl<E, Fa, Fm> DuringEntity<E, Fa, Fm>
where
    E: 'static + Send + Sync,
    Fa: 'static + Send + Sync + FnMut(&mut E, &mut Activation, _Any) -> DuringResult<E>,
    Fm: 'static + Send + Sync + FnMut(&mut E, &mut Activation, _Any) -> ActorResult,
{
    pub fn new(state: E, assertion_handler: Option<Fa>, message_handler: Option<Fm>) -> Self {
        DuringEntity {
            state,
            assertion_handler,
            message_handler,
            during: During::new(),
        }
    }

    pub fn on_asserted<Fa1>(self, assertion_handler: Fa1) -> DuringEntity<E, Fa1, Fm>
    where
        Fa1: 'static + Send + Sync + FnMut(&mut E, &mut Activation, _Any) -> DuringResult<E>,
    {
        DuringEntity {
            state: self.state,
            assertion_handler: Some(assertion_handler),
            message_handler: self.message_handler,
            during: self.during,
        }
    }

    pub fn on_message<Fm1>(self, message_handler: Fm1) -> DuringEntity<E, Fa, Fm1>
    where
        Fm1: 'static + Send + Sync + FnMut(&mut E, &mut Activation, _Any) -> ActorResult,
    {
        DuringEntity {
            state: self.state,
            assertion_handler: self.assertion_handler,
            message_handler: Some(message_handler),
            during: self.during,
        }
    }

    pub fn create(self, ac: &mut Actor) -> Arc<Ref> {
        ac.create(self)
    }

    pub fn create_rec<F>(self, ac: &mut Actor, f: F) -> Arc<Ref>
    where
        F: FnOnce(&mut Actor, &mut E, &Arc<Ref>) -> ()
    {
        ac.create_rec(self, |ac, e, e_ref| f(ac, &mut e.state, e_ref))
    }
}

impl<E, Fa, Fm> Entity for DuringEntity<E, Fa, Fm>
where
    E: 'static + Send + Sync,
    Fa: 'static + Send + Sync + FnMut(&mut E, &mut Activation, _Any) -> DuringResult<E>,
    Fm: 'static + Send + Sync + FnMut(&mut E, &mut Activation, _Any) -> ActorResult,
{
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn assert(&mut self, t: &mut Activation, a: _Any, h: Handle) -> ActorResult {
        match &mut self.assertion_handler {
            Some(handler) => match handler(&mut self.state, t, a)? {
                Some(f) => self.during.await_retraction(h, f),
                None => Ok(())
            }
            None => Ok(()),
        }
    }

    fn retract(&mut self, t: &mut Activation, h: Handle) -> ActorResult {
        self.during.retract(h)(&mut self.state, t)
    }

    fn message(&mut self, t: &mut Activation, m: _Any) -> ActorResult {
        match &mut self.message_handler {
            Some(handler) => handler(&mut self.state, t, m),
            None => Ok(()),
        }
    }
}
