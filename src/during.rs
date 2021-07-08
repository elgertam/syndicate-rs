use crate::actor::*;
use crate::error::Error;

use preserves::value::Map;

pub type DuringRetractionHandler<T> = Box<dyn Send + FnOnce(&mut T, &mut Activation) -> ActorResult>;
pub struct During<T>(Map<Handle, DuringRetractionHandler<T>>);

pub type DuringResult<E> =
    Result<Option<Box<dyn 'static + Send + FnOnce(&mut E, &mut Activation) -> ActorResult>>,
           Error>;

pub struct DuringEntity<E, Fa>
where
    E: 'static + Send,
    Fa: Send + FnMut(&mut E, &mut Activation, Assertion) -> DuringResult<E>,
{
    state: E,
    handler: Fa,
    during: During<E>,
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

impl<E, Fa> DuringEntity<E, Fa>
where
    E: 'static + Send,
    Fa: Send + FnMut(&mut E, &mut Activation, Assertion) -> DuringResult<E>,
{
    pub fn new(state: E, handler: Fa) -> Self {
        DuringEntity {
            state,
            handler,
            during: During::new(),
        }
    }
}

impl<E, Fa> Entity for DuringEntity<E, Fa>
where
    E: 'static + Send,
    Fa: Send + FnMut(&mut E, &mut Activation, Assertion) -> DuringResult<E>,
{
    fn assert(&mut self, t: &mut Activation, a: Assertion, h: Handle) -> ActorResult {
        match (self.handler)(&mut self.state, t, a)? {
            Some(f) => self.during.await_retraction(h, f),
            None => Ok(())
        }
    }

    fn retract(&mut self, t: &mut Activation, h: Handle) -> ActorResult {
        self.during.retract(h)(&mut self.state, t)
    }
}
