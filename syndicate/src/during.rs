use crate::actor::*;
use crate::error::Error;

use preserves::value::Map;

use std::sync::Arc;
use std::marker::PhantomData;

pub struct During<T>(Map<Handle, DuringRetractionHandler<T>>);
pub type DuringRetractionHandler<T> = Box<dyn Send + FnOnce(&mut T, &mut Activation) -> ActorResult>;
pub type DuringResult<E> = Result<Option<DuringRetractionHandler<E>>, Error>;

pub struct DuringEntity<M, E, Fa, Fm>
where
    M: 'static + Send,
    E: 'static + Send,
    Fa: 'static + Send + FnMut(&mut E, &mut Activation, M) -> DuringResult<E>,
    Fm: 'static + Send + FnMut(&mut E, &mut Activation, M) -> ActorResult,
{
    state: E,
    assertion_handler: Option<Fa>,
    message_handler: Option<Fm>,
    during: During<E>,
    phantom: PhantomData<M>,
}

impl<T> During<T> {
    pub fn new() -> Self {
        During(Map::new())
    }

    pub fn await_retraction<F: 'static + Send + FnOnce(&mut T, &mut Activation) -> ActorResult>(
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

pub fn entity<M: 'static + Send, E>(
    state: E
) -> DuringEntity<M,
                  E,
                  fn (&mut E, &mut Activation, M) -> DuringResult<E>,
                  fn (&mut E, &mut Activation, M) -> ActorResult>
where
    E: 'static + Send,
{
    DuringEntity::new(state, None, None)
}

impl<M, E, Fa, Fm> DuringEntity<M, E, Fa, Fm>
where
    M: 'static + Send,
    E: 'static + Send,
    Fa: 'static + Send + FnMut(&mut E, &mut Activation, M) -> DuringResult<E>,
    Fm: 'static + Send + FnMut(&mut E, &mut Activation, M) -> ActorResult,
{
    pub fn new(state: E, assertion_handler: Option<Fa>, message_handler: Option<Fm>) -> Self {
        DuringEntity {
            state,
            assertion_handler,
            message_handler,
            during: During::new(),
            phantom: PhantomData,
        }
    }

    pub fn on_asserted<Fa1>(self, assertion_handler: Fa1) -> DuringEntity<M, E, Fa1, Fm>
    where
        Fa1: 'static + Send + FnMut(&mut E, &mut Activation, M) -> DuringResult<E>,
    {
        DuringEntity {
            state: self.state,
            assertion_handler: Some(assertion_handler),
            message_handler: self.message_handler,
            during: self.during,
            phantom: PhantomData,
        }
    }

    pub fn on_message<Fm1>(self, message_handler: Fm1) -> DuringEntity<M, E, Fa, Fm1>
    where
        Fm1: 'static + Send + FnMut(&mut E, &mut Activation, M) -> ActorResult,
    {
        DuringEntity {
            state: self.state,
            assertion_handler: self.assertion_handler,
            message_handler: Some(message_handler),
            during: self.during,
            phantom: PhantomData,
        }
    }

    pub fn create(self, t: &mut Activation) -> Arc<Ref<M>> {
        t.create(self)
    }
}

impl<E, Fa, Fm> DuringEntity<AnyValue, E, Fa, Fm>
where
    E: 'static + Send,
    Fa: 'static + Send + FnMut(&mut E, &mut Activation, AnyValue) -> DuringResult<E>,
    Fm: 'static + Send + FnMut(&mut E, &mut Activation, AnyValue) -> ActorResult,
{
    pub fn create_cap(self, t: &mut Activation) -> Arc<Cap>
    {
        Cap::new(&self.create(t))
    }
}

impl<M, E, Fa, Fm> Entity<M> for DuringEntity<M, E, Fa, Fm>
where
    M: Send,
    E: 'static + Send,
    Fa: 'static + Send + FnMut(&mut E, &mut Activation, M) -> DuringResult<E>,
    Fm: 'static + Send + FnMut(&mut E, &mut Activation, M) -> ActorResult,
{
    fn assert(&mut self, t: &mut Activation, a: M, h: Handle) -> ActorResult {
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

    fn message(&mut self, t: &mut Activation, m: M) -> ActorResult {
        match &mut self.message_handler {
            Some(handler) => handler(&mut self.state, t, m),
            None => Ok(()),
        }
    }
}
