use crate::actor::*;
use crate::error::Error;

use preserves::value::Map;

use std::sync::Arc;
use std::marker::PhantomData;

pub struct During<T>(Map<Handle, DuringRetractionHandler<T>>);
pub type DuringRetractionHandler<T> = Box<dyn Send + FnOnce(&mut T, &mut Activation) -> ActorResult>;
pub type DuringResult<E> = Result<Option<DuringRetractionHandler<E>>, Error>;

pub struct DuringEntity<M, E, Fa, Fm, Fs, Fx>
where
    M: 'static + Send,
    E: 'static + Send,
    Fa: 'static + Send + FnMut(&mut E, &mut Activation, M) -> DuringResult<E>,
    Fm: 'static + Send + FnMut(&mut E, &mut Activation, M) -> ActorResult,
    Fs: 'static + Send + FnMut(&mut E, &mut Activation) -> ActorResult,
    Fx: 'static + Send + FnMut(&mut E, &mut Activation, &Arc<ExitStatus>),
{
    state: E,
    assertion_handler: Option<Fa>,
    message_handler: Option<Fm>,
    stop_handler: Option<Fs>,
    exit_handler: Option<Fx>,
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
                  fn (&mut E, &mut Activation, M) -> ActorResult,
                  fn (&mut E, &mut Activation) -> ActorResult,
                  fn (&mut E, &mut Activation, &Arc<ExitStatus>)>
where
    E: 'static + Send,
{
    DuringEntity::new(state, None, None, None, None)
}

impl<M, E, Fa, Fm, Fs, Fx> DuringEntity<M, E, Fa, Fm, Fs, Fx>
where
    M: 'static + Send,
    E: 'static + Send,
    Fa: 'static + Send + FnMut(&mut E, &mut Activation, M) -> DuringResult<E>,
    Fm: 'static + Send + FnMut(&mut E, &mut Activation, M) -> ActorResult,
    Fs: 'static + Send + FnMut(&mut E, &mut Activation) -> ActorResult,
    Fx: 'static + Send + FnMut(&mut E, &mut Activation, &Arc<ExitStatus>),
{
    pub fn new(
        state: E,
        assertion_handler: Option<Fa>,
        message_handler: Option<Fm>,
        stop_handler: Option<Fs>,
        exit_handler: Option<Fx>,
    ) -> Self {
        DuringEntity {
            state,
            assertion_handler,
            message_handler,
            stop_handler,
            exit_handler,
            during: During::new(),
            phantom: PhantomData,
        }
    }

    pub fn on_asserted<Fa1>(self, assertion_handler: Fa1) -> DuringEntity<M, E, Fa1, Fm, Fs, Fx>
    where
        Fa1: 'static + Send + FnMut(&mut E, &mut Activation, M) -> DuringResult<E>,
    {
        DuringEntity {
            state: self.state,
            assertion_handler: Some(assertion_handler),
            message_handler: self.message_handler,
            stop_handler: self.stop_handler,
            exit_handler: self.exit_handler,
            during: self.during,
            phantom: self.phantom,
        }
    }

    pub fn on_asserted_facet<Fa1>(
        self,
        mut assertion_handler: Fa1,
    ) -> DuringEntity<M, E, Box<dyn 'static + Send + FnMut(&mut E, &mut Activation, M) -> DuringResult<E>>, Fm, Fs, Fx>
    where
        Fa1: 'static + Send + FnMut(&mut E, &mut Activation, M) -> ActorResult
    {
        self.on_asserted(Box::new(move |state, t, a| {
            let facet_id = t.facet(|t| {
                // Prevent inertness check because we have a bounded lifetime anyway. This
                // allows e.g. facets containing Supervisors to Just Work (they go momentarily
                // inert when their supervisee exits).
                let _ = t.prevent_inert_check();
                assertion_handler(state, t, a)
            })?;
            Ok(Some(Box::new(move |_state, t| Ok(t.stop_facet(facet_id)))))
        }))
    }

    pub fn on_message<Fm1>(self, message_handler: Fm1) -> DuringEntity<M, E, Fa, Fm1, Fs, Fx>
    where
        Fm1: 'static + Send + FnMut(&mut E, &mut Activation, M) -> ActorResult,
    {
        DuringEntity {
            state: self.state,
            assertion_handler: self.assertion_handler,
            message_handler: Some(message_handler),
            stop_handler: self.stop_handler,
            exit_handler: self.exit_handler,
            during: self.during,
            phantom: self.phantom,
        }
    }

    pub fn on_stop<Fs1>(self, stop_handler: Fs1) -> DuringEntity<M, E, Fa, Fm, Fs1, Fx>
    where
        Fs1: 'static + Send + FnMut(&mut E, &mut Activation) -> ActorResult,
    {
        DuringEntity {
            state: self.state,
            assertion_handler: self.assertion_handler,
            message_handler: self.message_handler,
            stop_handler: Some(stop_handler),
            exit_handler: self.exit_handler,
            during: self.during,
            phantom: self.phantom,
        }
    }

    pub fn on_exit<Fx1>(self, exit_handler: Fx1) -> DuringEntity<M, E, Fa, Fm, Fs, Fx1>
    where
        Fx1: 'static + Send + FnMut(&mut E, &mut Activation, &Arc<ExitStatus>),
    {
        DuringEntity {
            state: self.state,
            assertion_handler: self.assertion_handler,
            message_handler: self.message_handler,
            stop_handler: self.stop_handler,
            exit_handler: Some(exit_handler),
            during: self.during,
            phantom: self.phantom,
        }
    }

    pub fn create(self, t: &mut Activation) -> Arc<Ref<M>> {
        let should_register_stop_action = self.stop_handler.is_some();
        let should_register_exit_hook = self.exit_handler.is_some();
        let r = t.create(self);
        if should_register_stop_action {
            t.on_stop_notify(&r);
        }
        if should_register_exit_hook {
            t.add_exit_hook(&r);
        }
        r
    }
}

impl<E, Fa, Fm, Fs, Fx> DuringEntity<AnyValue, E, Fa, Fm, Fs, Fx>
where
    E: 'static + Send,
    Fa: 'static + Send + FnMut(&mut E, &mut Activation, AnyValue) -> DuringResult<E>,
    Fm: 'static + Send + FnMut(&mut E, &mut Activation, AnyValue) -> ActorResult,
    Fs: 'static + Send + FnMut(&mut E, &mut Activation) -> ActorResult,
    Fx: 'static + Send + FnMut(&mut E, &mut Activation, &Arc<ExitStatus>),
{
    pub fn create_cap(self, t: &mut Activation) -> Arc<Cap>
    {
        Cap::new(&self.create(t))
    }
}

impl<M, E, Fa, Fm, Fs, Fx> Entity<M> for DuringEntity<M, E, Fa, Fm, Fs, Fx>
where
    M: Send,
    E: 'static + Send,
    Fa: 'static + Send + FnMut(&mut E, &mut Activation, M) -> DuringResult<E>,
    Fm: 'static + Send + FnMut(&mut E, &mut Activation, M) -> ActorResult,
    Fs: 'static + Send + FnMut(&mut E, &mut Activation) -> ActorResult,
    Fx: 'static + Send + FnMut(&mut E, &mut Activation, &Arc<ExitStatus>),
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

    fn stop(&mut self, t: &mut Activation) -> ActorResult {
        match &mut self.stop_handler {
            Some(handler) => handler(&mut self.state, t),
            None => Ok(()),
        }
    }

    fn exit_hook(&mut self, t: &mut Activation, exit_status: &Arc<ExitStatus>) {
        if let Some(handler) = &mut self.exit_handler {
            handler(&mut self.state, t, exit_status);
        }
    }
}
